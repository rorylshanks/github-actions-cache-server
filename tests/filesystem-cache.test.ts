import type { StorageAdapter } from '~/lib/storage'
import fs from 'node:fs/promises'
import path from 'node:path'
import { Readable } from 'node:stream'
import { beforeEach, describe, expect, test } from 'vitest'
import { FilesystemCachingAdapter, ObjectNotFoundError } from '~/lib/storage'
import { TEST_TEMP_DIR } from './setup'

const testRoot = path.join(TEST_TEMP_DIR, 'filesystem-cache')

async function readStream(stream: Readable) {
  const chunks: Buffer[] = []
  for await (const chunk of stream) chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk))
  return Buffer.concat(chunks)
}

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((res) => {
    resolve = res
  })
  return { promise, resolve }
}

class MemoryStorageAdapter implements StorageAdapter {
  downloads = new Map<string, number>()
  store = new Map<string, Buffer>()
  uploadGate: Promise<void> | undefined
  uploads = new Map<string, number>()

  async createDownloadStream(objectName: string) {
    const body = this.store.get(objectName)
    if (!body) throw new ObjectNotFoundError(objectName)
    this.downloads.set(objectName, (this.downloads.get(objectName) ?? 0) + 1)
    return Readable.from(body)
  }

  async uploadStream(objectName: string, stream: Readable) {
    this.uploads.set(objectName, (this.uploads.get(objectName) ?? 0) + 1)
    await this.uploadGate
    this.store.set(objectName, await readStream(stream))
  }

  async deleteFolder(folderName: string) {
    for (const key of this.store.keys()) {
      if (key === folderName || key.startsWith(`${folderName}/`)) this.store.delete(key)
    }
  }

  async countFilesInFolder(folderName: string) {
    return [...this.store.keys()].filter((key) => key.startsWith(`${folderName}/`)).length
  }

  async clear() {
    this.store.clear()
  }
}

describe('filesystem tiered cache adapter', () => {
  beforeEach(async () => {
    await fs.rm(testRoot, { force: true, recursive: true })
    await fs.mkdir(testRoot, { recursive: true })
  })

  test('makes uploads immediately readable locally while writeback is still pending', async () => {
    const backend = new MemoryStorageAdapter()
    const gate = deferred()
    backend.uploadGate = gate.promise

    const adapter = await FilesystemCachingAdapter.fromEnv({
      backend,
      cachePath: path.join(testRoot, 'cache'),
      maxObjectSize: 1024,
      maxSize: 1024,
    })

    await adapter.uploadStream('objects/value', Readable.from('writeback-data'))

    expect(backend.store.has('objects/value')).toBe(false)
    await expect(readStream(await adapter.createDownloadStream('objects/value'))).resolves.toEqual(
      Buffer.from('writeback-data'),
    )

    gate.resolve()
    await adapter.waitForIdle()

    expect(backend.store.get('objects/value')).toEqual(Buffer.from('writeback-data'))
    expect(backend.uploads.get('objects/value')).toBe(1)
  })

  test('evicts least-recently-used local objects when the cache exceeds max size', async () => {
    const backend = new MemoryStorageAdapter()
    backend.store.set('a', Buffer.from('aaaa'))
    backend.store.set('b', Buffer.from('bbbb'))

    const cachePath = path.join(testRoot, 'lru-cache')
    const adapter = await FilesystemCachingAdapter.fromEnv({
      backend,
      cachePath,
      maxObjectSize: 1024,
      maxSize: 5,
    })

    await readStream(await adapter.createDownloadStream('a'))
    await readStream(await adapter.createDownloadStream('b'))
    await adapter.waitForIdle()

    await expect(fs.access(path.join(cachePath, 'a'))).rejects.toMatchObject({ code: 'ENOENT' })
    await expect(fs.access(path.join(cachePath, 'b'))).resolves.toBeUndefined()

    await readStream(await adapter.createDownloadStream('a'))
    expect(backend.downloads.get('a')).toBe(2)
    expect(backend.downloads.get('b')).toBe(1)
  })

  test('does not retain downloaded objects larger than the per-object cache limit', async () => {
    const backend = new MemoryStorageAdapter()
    backend.store.set('large', Buffer.from('large-object'))

    const cachePath = path.join(testRoot, 'object-limit-cache')
    const adapter = await FilesystemCachingAdapter.fromEnv({
      backend,
      cachePath,
      maxObjectSize: 4,
      maxSize: 1024,
    })

    await expect(readStream(await adapter.createDownloadStream('large'))).resolves.toEqual(
      Buffer.from('large-object'),
    )
    await adapter.waitForIdle()
    await expect(fs.access(path.join(cachePath, 'large'))).rejects.toMatchObject({
      code: 'ENOENT',
    })
  })
})
