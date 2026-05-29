import fs from 'node:fs/promises'
import { beforeEach, describe, expect, test } from 'vitest'
import { getDatabase } from '~/lib/db'
import {
  DOCKER_REGISTRY_AUTH_CHECK_REFERENCE,
  DOCKER_REGISTRY_AUTH_COUNT_KEY,
  DOCKER_REGISTRY_AUTH_REPOSITORY,
  DOCKER_REGISTRY_LAYER_BODY,
  DOCKER_REGISTRY_LAYER_DIGEST,
  DOCKER_REGISTRY_MANIFEST_BODY,
  DOCKER_REGISTRY_MANIFEST_DIGEST,
  DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE,
  DOCKER_REGISTRY_OFFLINE_PATH,
  DOCKER_REGISTRY_REPOSITORY,
  DOCKER_REGISTRY_SLOW_LAYER_BODY,
  DOCKER_REGISTRY_SLOW_LAYER_DIGEST,
  DOCKER_REGISTRY_TAG_CHANGED_PATH,
  DOCKER_REGISTRY_UPDATED_MANIFEST_BODY,
  DOCKER_REGISTRY_UPDATED_MANIFEST_DIGEST,
  dockerRegistryBlobPath,
  dockerRegistryManifestPath,
  readDockerRegistryFixtureCounts,
  resetDockerRegistryFixtureState,
} from './docker-registry-fixture'

const apiBaseUrl = process.env.API_BASE_URL ?? 'http://localhost:3000'

async function fetchBuffer(path: string, init?: RequestInit) {
  const response = await fetch(`${apiBaseUrl}${path}`, init)
  return {
    body: Buffer.from(await response.arrayBuffer()),
    response,
  }
}

async function expireDockerRegistryManifest(reference: string) {
  const db = await getDatabase()
  await db
    .updateTable('docker_registry_objects')
    .set({
      updatedAt: Date.now() - 60 * 60 * 1000,
    })
    .where('kind', '=', 'manifest')
    .where('repository', '=', DOCKER_REGISTRY_REPOSITORY)
    .where('reference', '=', reference)
    .execute()
}

describe('docker registry mirror', () => {
  beforeEach(async () => {
    await resetDockerRegistryFixtureState()
  })

  test('serves the v2 registry ping endpoint', async () => {
    const response = await fetch(`${apiBaseUrl}/v2/`)

    expect(response.status).toBe(200)
    expect(response.headers.get('docker-distribution-api-version')).toBe('registry/2.0')
  })

  test('uses configured Docker Hub credentials for upstream token requests', async () => {
    const manifestPath = dockerRegistryManifestPath(
      DOCKER_REGISTRY_AUTH_CHECK_REFERENCE,
      DOCKER_REGISTRY_AUTH_REPOSITORY,
    )
    const response = await fetch(`${apiBaseUrl}${manifestPath}`, {
      headers: {
        accept: DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE,
      },
    })

    expect(response.status).toBe(200)
    const counts = await readDockerRegistryFixtureCounts()
    expect(counts[DOCKER_REGISTRY_AUTH_COUNT_KEY]).toBe(1)
    expect(counts['GET /token unauthenticated']).toBeUndefined()
  })

  test('stores manifests and blobs, then serves cached objects while upstream is offline', async () => {
    const manifestPath = dockerRegistryManifestPath('latest')
    const blobPath = dockerRegistryBlobPath(DOCKER_REGISTRY_LAYER_DIGEST)
    const manifestHeaders = {
      accept: DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE,
    }

    const firstManifest = await fetchBuffer(manifestPath, {
      headers: manifestHeaders,
    })
    expect(firstManifest.response.status).toBe(200)
    expect(firstManifest.response.headers.get('docker-content-digest')).toBe(
      DOCKER_REGISTRY_MANIFEST_DIGEST,
    )
    expect(firstManifest.body.equals(DOCKER_REGISTRY_MANIFEST_BODY)).toBe(true)

    const firstBlob = await fetchBuffer(blobPath)
    expect(firstBlob.response.status).toBe(200)
    expect(firstBlob.response.headers.get('docker-content-digest')).toBe(
      DOCKER_REGISTRY_LAYER_DIGEST,
    )
    expect(firstBlob.body.equals(DOCKER_REGISTRY_LAYER_BODY)).toBe(true)

    const countsBeforeOffline = await readDockerRegistryFixtureCounts()
    expect(countsBeforeOffline[`GET ${manifestPath}`]).toBe(1)
    expect(countsBeforeOffline[`GET ${blobPath}`]).toBe(1)

    await fs.writeFile(DOCKER_REGISTRY_OFFLINE_PATH, '1')

    const cachedManifest = await fetchBuffer(manifestPath, {
      headers: manifestHeaders,
    })
    const cachedBlob = await fetchBuffer(blobPath)

    expect(cachedManifest.response.status).toBe(200)
    expect(cachedManifest.body.equals(DOCKER_REGISTRY_MANIFEST_BODY)).toBe(true)
    expect(cachedBlob.response.status).toBe(200)
    expect(cachedBlob.body.equals(DOCKER_REGISTRY_LAYER_BODY)).toBe(true)
    expect(await readDockerRegistryFixtureCounts()).toEqual(countsBeforeOffline)
  })

  test('coalesces concurrent fills for the same uncached blob', async () => {
    const blobPath = dockerRegistryBlobPath(DOCKER_REGISTRY_SLOW_LAYER_DIGEST)

    const responses = await Promise.all(Array.from({ length: 3 }, () => fetchBuffer(blobPath)))

    for (const { body, response } of responses) {
      expect(response.status).toBe(200)
      expect(body.equals(DOCKER_REGISTRY_SLOW_LAYER_BODY)).toBe(true)
    }

    const counts = await readDockerRegistryFixtureCounts()
    expect(counts[`GET ${blobPath}`]).toBe(1)
  })

  test('revalidates stale tag manifests without refetching unchanged content', async () => {
    const reference = 'ttl-unchanged'
    const manifestPath = dockerRegistryManifestPath(reference)
    const manifestHeaders = {
      accept: DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE,
    }

    const firstManifest = await fetchBuffer(manifestPath, {
      headers: manifestHeaders,
    })
    expect(firstManifest.response.status).toBe(200)
    expect(firstManifest.body.equals(DOCKER_REGISTRY_MANIFEST_BODY)).toBe(true)

    await expireDockerRegistryManifest(reference)

    const cachedManifest = await fetchBuffer(manifestPath, {
      headers: manifestHeaders,
    })
    expect(cachedManifest.response.status).toBe(200)
    expect(cachedManifest.body.equals(DOCKER_REGISTRY_MANIFEST_BODY)).toBe(true)

    const counts = await readDockerRegistryFixtureCounts()
    expect(counts[`GET ${manifestPath}`]).toBe(1)
    expect(counts[`HEAD ${manifestPath}`]).toBe(1)
  })

  test('invalidates stale tag manifests when the upstream digest changes', async () => {
    const reference = 'ttl-changed'
    const manifestPath = dockerRegistryManifestPath(reference)
    const manifestHeaders = {
      accept: DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE,
    }

    const firstManifest = await fetchBuffer(manifestPath, {
      headers: manifestHeaders,
    })
    expect(firstManifest.response.status).toBe(200)
    expect(firstManifest.body.equals(DOCKER_REGISTRY_MANIFEST_BODY)).toBe(true)

    await expireDockerRegistryManifest(reference)
    await fs.writeFile(DOCKER_REGISTRY_TAG_CHANGED_PATH, '1')

    const updatedManifest = await fetchBuffer(manifestPath, {
      headers: manifestHeaders,
    })
    expect(updatedManifest.response.status).toBe(200)
    expect(updatedManifest.response.headers.get('docker-content-digest')).toBe(
      DOCKER_REGISTRY_UPDATED_MANIFEST_DIGEST,
    )
    expect(updatedManifest.body.equals(DOCKER_REGISTRY_UPDATED_MANIFEST_BODY)).toBe(true)

    const counts = await readDockerRegistryFixtureCounts()
    expect(counts[`HEAD ${manifestPath}`]).toBe(1)
    expect(counts[`GET ${manifestPath}`]).toBe(2)
  })

  test('proxies tag list requests to Docker Hub-compatible upstream', async () => {
    const tagsPath = `/v2/${DOCKER_REGISTRY_REPOSITORY}/tags/list`
    const response = await fetch(`${apiBaseUrl}${tagsPath}`)

    expect(response.status).toBe(200)
    await expect(response.json()).resolves.toEqual({
      name: DOCKER_REGISTRY_REPOSITORY,
      tags: ['latest'],
    })
    const counts = await readDockerRegistryFixtureCounts()
    expect(counts[`GET ${tagsPath}`]).toBe(1)
  })
})
