import { getDockerRegistryMirror } from '~/lib/docker-registry'

function routeParts(pathParam: string | string[] | undefined) {
  const raw = Array.isArray(pathParam) ? pathParam.join('/') : (pathParam ?? '')
  return raw.split('/').filter(Boolean)
}

function parseRegistryPath(pathParam: string | string[] | undefined) {
  const parts = routeParts(pathParam)
  const markerIndex = parts.findIndex((part) => ['blobs', 'manifests', 'tags'].includes(part))
  if (markerIndex <= 0) return

  const repository = parts.slice(0, markerIndex).join('/')
  const marker = parts[markerIndex]
  const rest = parts.slice(markerIndex + 1)

  if (marker === 'manifests' && rest.length > 0)
    return {
      kind: 'manifest' as const,
      reference: rest.join('/'),
      repository,
    }

  if (marker === 'blobs' && rest.length > 0)
    return {
      digest: rest.join('/'),
      kind: 'blob' as const,
      repository,
    }

  if (marker === 'tags' && rest[0] === 'list')
    return {
      kind: 'proxy' as const,
      repository,
      upstreamPath: 'tags/list',
    }
}

export default defineEventHandler(async (event) => {
  const parsed = parseRegistryPath(event.context.params?.path)
  if (!parsed)
    throw createError({
      statusCode: 404,
      statusMessage: 'Unsupported Docker Registry API path',
    })

  const mirror = await getDockerRegistryMirror()
  if (parsed.kind === 'manifest')
    return mirror.handleManifest(event, parsed.repository, parsed.reference)
  if (parsed.kind === 'blob') return mirror.handleBlob(event, parsed.repository, parsed.digest)
  return mirror.proxy(event, parsed.repository, parsed.upstreamPath)
})
