import crypto from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'

import { restoreCache, saveCache } from '@actions/cache'
import { SignJWT } from 'jose'
import { afterAll, beforeAll, describe, expect, test } from 'vitest'
import { Storage } from '~/lib/storage'
import { TEST_TEMP_DIR } from './setup'

const testFilePath = path.join(TEST_TEMP_DIR, 'test-stale.bin')
const cacheServiceUrl = 'http://localhost:3000/twirp/github.actions.results.api.v1.CacheService'

async function cacheServiceRequest(method: string, body: unknown) {
  const response = await fetch(`${cacheServiceUrl}/${method}`, {
    method: 'POST',
    headers: {
      'authorization': `Bearer ${process.env.ACTIONS_RUNTIME_TOKEN}`,
      'content-type': 'application/json',
    },
    body: JSON.stringify(body),
  })
  expect(response.status).toBe(200)
  return response.json()
}

describe('stale cache entry handling (missing storage objects)', () => {
  let adapter: Awaited<ReturnType<typeof Storage.getAdapterFromEnv>>

  beforeAll(async () => {
    process.env.ACTIONS_CACHE_SERVICE_V2 = 'true'
    process.env.ACTIONS_RUNTIME_TOKEN = await new SignJWT({
      ac: JSON.stringify([{ Scope: 'refs/heads/main', Permission: 3 }]),
      repository_id: '123',
    })
      .setProtectedHeader({ alg: 'HS256' })
      .sign(crypto.createSecretKey('mock-secret-key', 'ascii'))

    adapter = await Storage.getAdapterFromEnv()
  })
  afterAll(() => {
    delete process.env.ACTIONS_CACHE_SERVICE_V2
    delete process.env.ACTIONS_RUNTIME_TOKEN
  })

  test(
    'returns cache miss when parts are wiped before first download (unmerged entry)',
    { timeout: 30_000 },
    async () => {
      const contents = crypto.randomBytes(1024)
      await fs.writeFile(testFilePath, contents)
      await saveCache([testFilePath], 'stale-fresh-key')
      await fs.rm(testFilePath)

      await adapter.clear()

      const missKey = await restoreCache([testFilePath], 'stale-fresh-key')
      expect(missKey).toBeUndefined()

      const missKey2 = await restoreCache([testFilePath], 'stale-fresh-key')
      expect(missKey2).toBeUndefined()
    },
  )

  test(
    'returns cache miss when the merged blob is wiped after merge completes',
    { timeout: 30_000 },
    async () => {
      const contents = crypto.randomBytes(1024)
      await fs.writeFile(testFilePath, contents)
      await saveCache([testFilePath], 'stale-merged-key')
      await fs.rm(testFilePath)

      const hitKey = await restoreCache([testFilePath], 'stale-merged-key')
      expect(hitKey).toBe('stale-merged-key')
      await fs.rm(testFilePath)

      // Wait for the background merge to flush before wiping storage.
      await new Promise((resolve) => setTimeout(resolve, 2000))

      await adapter.clear()

      const missKey = await restoreCache([testFilePath], 'stale-merged-key')
      expect(missKey).toBeUndefined()

      const missKey2 = await restoreCache([testFilePath], 'stale-merged-key')
      expect(missKey2).toBeUndefined()
    },
  )

  test('does not return a download URL for pending cache uploads', async () => {
    const key = `pending-${crypto.randomUUID()}`
    const version = crypto.randomUUID()

    await expect(cacheServiceRequest('CreateCacheEntry', { key, version })).resolves.toMatchObject({
      ok: true,
    })

    await expect(
      cacheServiceRequest('GetCacheEntryDownloadURL', { key, version }),
    ).resolves.toEqual({
      ok: false,
    })
  })

  test('returns cache miss instead of a download URL when stored objects are missing', async () => {
    const key = `stale-${crypto.randomUUID()}`
    const version = crypto.randomUUID()
    const createResponse = (await cacheServiceRequest('CreateCacheEntry', {
      key,
      version,
    })) as { ok: boolean; signed_upload_url: string }
    expect(createResponse.ok).toBe(true)

    const uploadResponse = await fetch(createResponse.signed_upload_url, {
      method: 'PUT',
      body: crypto.randomBytes(1024),
    })
    expect(uploadResponse.status).toBe(201)

    await expect(
      cacheServiceRequest('FinalizeCacheEntryUpload', { key, version }),
    ).resolves.toMatchObject({
      ok: true,
    })

    await adapter.clear()

    await expect(
      cacheServiceRequest('GetCacheEntryDownloadURL', { key, version }),
    ).resolves.toEqual({
      ok: false,
    })
  })
})
