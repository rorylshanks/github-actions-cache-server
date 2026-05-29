import { createHash } from 'node:crypto'
import fs from 'node:fs/promises'
import path from 'node:path'

const TEST_TEMP_DIR = 'tests/temp'

function digest(buffer: Buffer) {
  return `sha256:${createHash('sha256').update(buffer).digest('hex')}`
}

function jsonBuffer(value: unknown) {
  return Buffer.from(JSON.stringify(value))
}

export const DOCKER_REGISTRY_REPOSITORY = 'library/cache-server-test'
export const DOCKER_REGISTRY_AUTH_REPOSITORY = 'library/cache-server-auth-test'
export const DOCKER_REGISTRY_AUTH_CHECK_REFERENCE = 'auth-check'
export const DOCKER_REGISTRY_AUTH_USERNAME = 'fixture-dockerhub-user'
export const DOCKER_REGISTRY_AUTH_PASSWORD = 'fixture-dockerhub-token'
export const DOCKER_REGISTRY_AUTH_HEADER = `Basic ${Buffer.from(
  `${DOCKER_REGISTRY_AUTH_USERNAME}:${DOCKER_REGISTRY_AUTH_PASSWORD}`,
).toString('base64')}`
export const DOCKER_REGISTRY_AUTH_COUNT_KEY = 'GET /token authenticated'
export const DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE = 'application/vnd.oci.image.manifest.v1+json'
export const DOCKER_REGISTRY_CONFIG_MEDIA_TYPE = 'application/vnd.oci.image.config.v1+json'
export const DOCKER_REGISTRY_LAYER_MEDIA_TYPE = 'application/vnd.oci.image.layer.v1.tar'

export const DOCKER_REGISTRY_CONFIG_BODY = jsonBuffer({
  architecture: 'amd64',
  config: {},
  os: 'linux',
  rootfs: {
    diff_ids: [],
    type: 'layers',
  },
})
export const DOCKER_REGISTRY_CONFIG_DIGEST = digest(DOCKER_REGISTRY_CONFIG_BODY)

export const DOCKER_REGISTRY_LAYER_BODY = Buffer.from('cached docker layer\n')
export const DOCKER_REGISTRY_LAYER_DIGEST = digest(DOCKER_REGISTRY_LAYER_BODY)

export const DOCKER_REGISTRY_SLOW_LAYER_BODY = Buffer.from('coalesced docker layer\n')
export const DOCKER_REGISTRY_SLOW_LAYER_DIGEST = digest(DOCKER_REGISTRY_SLOW_LAYER_BODY)

export const DOCKER_REGISTRY_MANIFEST_BODY = jsonBuffer({
  config: {
    digest: DOCKER_REGISTRY_CONFIG_DIGEST,
    mediaType: DOCKER_REGISTRY_CONFIG_MEDIA_TYPE,
    size: DOCKER_REGISTRY_CONFIG_BODY.byteLength,
  },
  layers: [
    {
      digest: DOCKER_REGISTRY_LAYER_DIGEST,
      mediaType: DOCKER_REGISTRY_LAYER_MEDIA_TYPE,
      size: DOCKER_REGISTRY_LAYER_BODY.byteLength,
    },
  ],
  mediaType: DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE,
  schemaVersion: 2,
})
export const DOCKER_REGISTRY_MANIFEST_DIGEST = digest(DOCKER_REGISTRY_MANIFEST_BODY)
export const DOCKER_REGISTRY_UPDATED_MANIFEST_BODY = jsonBuffer({
  annotations: {
    'org.opencontainers.image.revision': 'updated',
  },
  config: {
    digest: DOCKER_REGISTRY_CONFIG_DIGEST,
    mediaType: DOCKER_REGISTRY_CONFIG_MEDIA_TYPE,
    size: DOCKER_REGISTRY_CONFIG_BODY.byteLength,
  },
  layers: [
    {
      digest: DOCKER_REGISTRY_LAYER_DIGEST,
      mediaType: DOCKER_REGISTRY_LAYER_MEDIA_TYPE,
      size: DOCKER_REGISTRY_LAYER_BODY.byteLength,
    },
  ],
  mediaType: DOCKER_REGISTRY_MANIFEST_MEDIA_TYPE,
  schemaVersion: 2,
})
export const DOCKER_REGISTRY_UPDATED_MANIFEST_DIGEST = digest(DOCKER_REGISTRY_UPDATED_MANIFEST_BODY)

export const DOCKER_REGISTRY_COUNTS_PATH = path.join(
  TEST_TEMP_DIR,
  'docker-registry-upstream-counts.json',
)
export const DOCKER_REGISTRY_OFFLINE_PATH = path.join(
  TEST_TEMP_DIR,
  'docker-registry-upstream-offline',
)
export const DOCKER_REGISTRY_TAG_CHANGED_PATH = path.join(
  TEST_TEMP_DIR,
  'docker-registry-tag-changed',
)

export function dockerRegistryManifestPath(
  reference: string,
  repository = DOCKER_REGISTRY_REPOSITORY,
) {
  return `/v2/${repository}/manifests/${reference}`
}

export function dockerRegistryBlobPath(
  objectDigest: string,
  repository = DOCKER_REGISTRY_REPOSITORY,
) {
  return `/v2/${repository}/blobs/${objectDigest}`
}

export async function resetDockerRegistryFixtureState() {
  await Promise.all([
    fs.rm(DOCKER_REGISTRY_COUNTS_PATH, { force: true }),
    fs.rm(DOCKER_REGISTRY_OFFLINE_PATH, { force: true }),
    fs.rm(DOCKER_REGISTRY_TAG_CHANGED_PATH, { force: true }),
  ])
}

export async function readDockerRegistryFixtureCounts() {
  try {
    return JSON.parse(await fs.readFile(DOCKER_REGISTRY_COUNTS_PATH, 'utf8')) as Record<
      string,
      number
    >
  } catch (err: any) {
    if (err.code === 'ENOENT') return {}
    throw err
  }
}
