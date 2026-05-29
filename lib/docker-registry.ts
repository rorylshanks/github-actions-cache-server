import type { Kysely } from 'kysely'
import type { Database, DockerRegistryObject } from './db'
import type { Storage } from './storage'
import { createHash } from 'node:crypto'
import { once } from 'node:events'
import { PassThrough, Readable } from 'node:stream'
import { createSingletonPromise } from '@antfu/utils'
import {
  createError,
  getHeader,
  getRequestWebStream,
  sendStream,
  setHeader,
  setResponseStatus,
} from 'h3'
import { getDatabase } from './db'
import { env } from './env'
import { logger } from './logger'
import { getStorage, ObjectNotFoundError } from './storage'

type RegistryObjectKind = DockerRegistryObject['kind']

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function objectId(objectKey: string) {
  return createHash('sha256').update(objectKey).digest('hex')
}

function normalizeRepository(repository: string) {
  const trimmed = repository.replaceAll(/^\/+|\/+$/g, '')
  if (!trimmed.includes('/')) return `library/${trimmed}`
  return trimmed
}

function isDigestReference(reference: string) {
  return /^[\w+.-]+:[\dA-F]+$/i.test(reference)
}

function safeDigestPath(digest: string) {
  return digest.replace(':', '/')
}

function headerNumber(headers: Headers, name: string) {
  const value = headers.get(name)
  if (!value) return null
  const parsed = Number.parseInt(value)
  return Number.isFinite(parsed) ? parsed : null
}

function methodAllowsBody(method: string) {
  return !['GET', 'HEAD'].includes(method.toUpperCase())
}

export class DockerRegistryMirror {
  private db
  private storage
  private fillPromises = new Map<string, Promise<void>>()
  private tokenCache = new Map<string, { expiresAt: number; token: string }>()

  private constructor({ db, storage }: { db: Kysely<Database>; storage: Storage }) {
    this.db = db
    this.storage = storage
  }

  static async fromEnv() {
    return new DockerRegistryMirror({
      db: await getDatabase(),
      storage: await getStorage(),
    })
  }

  setBaseHeaders(event: any) {
    setHeader(event, 'Docker-Distribution-API-Version', 'registry/2.0')
  }

  async handlePing(event: any) {
    this.setBaseHeaders(event)
    return ''
  }

  async handleManifest(event: any, repository: string, reference: string) {
    this.assertMirrorEnabled()
    this.assertReadMethod(event)

    const normalizedRepository = normalizeRepository(repository)
    const accept = getHeader(event, 'accept') ?? ''
    const objectKey = `manifest:${normalizedRepository}:${reference}:${accept}`
    const objectName = `docker-registry/manifests/${objectId(objectKey)}`
    const now = Date.now()
    const existing = await this.getObject(objectKey)
    const isFresh =
      existing?.status === 'ready' &&
      (isDigestReference(reference) ||
        now - existing.updatedAt < env.DOCKERHUB_MANIFEST_TTL_SECONDS * 1000)

    if (isFresh) return this.serveStoredObject(event, existing)
    if (existing?.status === 'ready' && !isDigestReference(reference)) {
      const revalidated = await this.revalidateManifestTag({
        accept,
        object: existing,
        reference,
        repository: normalizedRepository,
      })
      if (revalidated) return this.serveStoredObject(event, revalidated)
    }

    if (existing?.status === 'filling') {
      const object = await this.waitForObject(objectKey)
      if (object) return this.serveStoredObject(event, object)
    }

    if (event.method === 'HEAD')
      return this.proxyUpstream(event, normalizedRepository, `manifests/${reference}`)

    const reserved = await this.reserveObject({
      force: existing?.status === 'ready',
      kind: 'manifest',
      objectKey,
      objectName,
      reference,
      repository: normalizedRepository,
    })
    if (!reserved.created) {
      const object = await this.waitForObject(objectKey)
      if (object) return this.serveStoredObject(event, object)
    }

    const upstream = await this.fetchUpstream(normalizedRepository, `manifests/${reference}`, {
      headers: {
        accept,
      },
      method: 'GET',
    })
    if (!upstream.ok) return this.proxyUpstreamResponse(event, upstream, objectKey)

    const body = await upstream.arrayBuffer()
    const content = Buffer.from(body)
    const contentType =
      upstream.headers.get('content-type') ?? 'application/vnd.oci.image.manifest.v1+json'
    const digest = upstream.headers.get('docker-content-digest')
    await this.storage.adapter.uploadStream(objectName, Readable.from(content))
    const object = await this.markObjectReady({
      contentLength: content.length,
      contentType,
      digest,
      etag: upstream.headers.get('etag'),
      objectKey,
    })

    return this.serveStoredObject(event, object)
  }

  async handleBlob(event: any, repository: string, digest: string) {
    this.assertMirrorEnabled()
    this.assertReadMethod(event)

    const normalizedRepository = normalizeRepository(repository)
    if (getHeader(event, 'range'))
      return this.proxyUpstream(event, normalizedRepository, `blobs/${digest}`)

    const objectKey = `blob:${digest}`
    const objectName = `docker-registry/blobs/${safeDigestPath(digest)}`
    const existing = await this.getObject(objectKey)

    if (existing?.status === 'ready') return this.serveStoredObject(event, existing)
    if (existing?.status === 'filling') {
      const object = await this.waitForObject(objectKey)
      if (object) return this.serveStoredObject(event, object)
    }

    if (event.method === 'HEAD')
      return this.proxyUpstream(event, normalizedRepository, `blobs/${digest}`)

    const reserved = await this.reserveObject({
      kind: 'blob',
      objectKey,
      objectName,
      reference: digest,
      repository: normalizedRepository,
    })
    if (!reserved.created) {
      const object = await this.waitForObject(objectKey)
      if (object) return this.serveStoredObject(event, object)
    }

    const upstream = await this.fetchUpstream(normalizedRepository, `blobs/${digest}`, {
      headers: this.forwardedRequestHeaders(event, ['range']),
      method: 'GET',
    })
    if (!upstream.ok) return this.proxyUpstreamResponse(event, upstream, objectKey)
    if (!upstream.body) throw new Error('Docker Hub blob response did not include a body')

    const responseStream = new PassThrough()
    const storageStream = new PassThrough()
    const contentType = upstream.headers.get('content-type') ?? 'application/octet-stream'
    const digestHeader = upstream.headers.get('docker-content-digest') ?? digest
    const declaredLength = headerNumber(upstream.headers, 'content-length')

    this.copyObjectHeaders(event, {
      contentLength: declaredLength,
      contentType,
      digest: digestHeader,
      etag: upstream.headers.get('etag'),
    })

    const fillPromise = this.pumpUpstreamToResponseAndStorage({
      contentType,
      digest: digestHeader,
      etag: upstream.headers.get('etag'),
      objectKey,
      objectName,
      responseStream,
      source: Readable.fromWeb(upstream.body as any),
      storageStream,
    }).finally(() => {
      this.fillPromises.delete(objectKey)
    })
    this.fillPromises.set(objectKey, fillPromise)
    fillPromise.catch((err) => logger.error(`Failed to fill Docker blob ${digest}`, err))

    return sendStream(event, Readable.toWeb(responseStream) as unknown as ReadableStream)
  }

  async proxy(event: any, repository: string, upstreamPath: string) {
    this.assertMirrorEnabled()
    this.assertReadMethod(event)
    return this.proxyUpstream(event, normalizeRepository(repository), upstreamPath)
  }

  private assertMirrorEnabled() {
    if (!env.DOCKERHUB_MIRROR_ENABLED)
      throw createError({ statusCode: 404, statusMessage: 'Docker Hub mirror is disabled' })
  }

  private assertReadMethod(event: any) {
    if (!['GET', 'HEAD'].includes(event.method))
      throw createError({ statusCode: 405, statusMessage: 'Registry mirror is read-only' })
  }

  private async getObject(objectKey: string) {
    return this.db
      .selectFrom('docker_registry_objects')
      .where('id', '=', objectId(objectKey))
      .selectAll()
      .executeTakeFirst()
  }

  private async reserveObject({
    force,
    kind,
    objectKey,
    objectName,
    reference,
    repository,
  }: {
    force?: boolean
    kind: RegistryObjectKind
    objectKey: string
    objectName: string
    reference: string
    repository: string
  }) {
    const id = objectId(objectKey)
    const now = Date.now()
    const existing = await this.getObject(objectKey)

    if (existing && !force && existing.status !== 'error')
      return { created: false, object: existing }

    if (existing) {
      await this.db
        .updateTable('docker_registry_objects')
        .set({
          contentLength: null,
          contentType: null,
          digest: null,
          etag: null,
          filledAt: null,
          status: 'filling',
          updatedAt: now,
        })
        .where('id', '=', id)
        .execute()
      return { created: true, object: await this.getObject(objectKey) }
    }

    try {
      await this.db
        .insertInto('docker_registry_objects')
        .values({
          id,
          contentLength: null,
          contentType: null,
          createdAt: now,
          digest: null,
          etag: null,
          filledAt: null,
          kind,
          lastAccessedAt: null,
          objectKey,
          objectName,
          reference,
          repository,
          status: 'filling',
          updatedAt: now,
        })
        .execute()
      return { created: true, object: await this.getObject(objectKey) }
    } catch {
      const object = await this.getObject(objectKey)
      return { created: false, object }
    }
  }

  private async waitForObject(objectKey: string) {
    const inProcessFill = this.fillPromises.get(objectKey)
    if (inProcessFill) await inProcessFill.catch(() => {})

    const deadline = Date.now() + env.DOCKERHUB_PENDING_WAIT_TIMEOUT_MS
    while (Date.now() < deadline) {
      const object = await this.getObject(objectKey)
      if (!object) return
      if (object.status === 'ready') return object
      if (object.status === 'error') return
      await sleep(250)
    }

    logger.warn(`Timed out waiting for Docker registry object ${objectKey}`)
    await this.markObjectError(objectKey)
  }

  private async markObjectReady({
    contentLength,
    contentType,
    digest,
    etag,
    objectKey,
  }: {
    contentLength: number | null
    contentType: string
    digest: string | null
    etag: string | null
    objectKey: string
  }) {
    const now = Date.now()
    await this.db
      .updateTable('docker_registry_objects')
      .set({
        contentLength,
        contentType,
        digest,
        etag,
        filledAt: now,
        lastAccessedAt: now,
        status: 'ready',
        updatedAt: now,
      })
      .where('id', '=', objectId(objectKey))
      .execute()

    const object = await this.getObject(objectKey)
    if (!object) throw new Error(`Docker registry object disappeared after fill: ${objectKey}`)
    return object
  }

  private async markObjectChecked(object: DockerRegistryObject, upstream?: Response) {
    const contentLength = upstream ? headerNumber(upstream.headers, 'content-length') : null
    const contentType = upstream?.headers.get('content-type')
    const digest = upstream?.headers.get('docker-content-digest')
    const etag = upstream?.headers.get('etag')

    await this.db
      .updateTable('docker_registry_objects')
      .set({
        ...(contentLength === null ? {} : { contentLength }),
        ...(contentType ? { contentType } : {}),
        ...(digest ? { digest } : {}),
        ...(etag ? { etag } : {}),
        updatedAt: Date.now(),
      })
      .where('id', '=', object.id)
      .execute()

    const updated = await this.getObject(object.objectKey)
    if (!updated)
      throw new Error(`Docker registry object disappeared after revalidation: ${object.objectKey}`)
    return updated
  }

  private async markObjectError(objectKey: string) {
    await this.db
      .updateTable('docker_registry_objects')
      .set({
        status: 'error',
        updatedAt: Date.now(),
      })
      .where('id', '=', objectId(objectKey))
      .execute()
  }

  private async revalidateManifestTag({
    accept,
    object,
    reference,
    repository,
  }: {
    accept: string
    object: DockerRegistryObject
    reference: string
    repository: string
  }) {
    let upstream: Response
    try {
      upstream = await this.fetchUpstream(repository, `manifests/${reference}`, {
        headers: {
          accept,
        },
        method: 'HEAD',
      })
    } catch (err) {
      logger.warn(`Docker manifest tag revalidation failed for ${repository}:${reference}`, err)
      return this.markObjectChecked(object)
    }

    if (!upstream.ok) {
      logger.warn(
        `Docker manifest tag revalidation failed for ${repository}:${reference}: ${upstream.status} ${upstream.statusText}`,
      )
      return this.markObjectChecked(object)
    }

    const upstreamDigest = upstream.headers.get('docker-content-digest')
    const upstreamEtag = upstream.headers.get('etag')
    const unchanged =
      (upstreamDigest && object.digest && upstreamDigest === object.digest) ||
      (upstreamEtag && object.etag && upstreamEtag === object.etag)

    if (unchanged) return this.markObjectChecked(object, upstream)

    logger.info(`Docker manifest tag changed for ${repository}:${reference}; invalidating cache`)
    await this.markObjectError(object.objectKey)
  }

  private async serveStoredObject(event: any, object: DockerRegistryObject) {
    this.copyObjectHeaders(event, object)
    await this.db
      .updateTable('docker_registry_objects')
      .set({
        lastAccessedAt: Date.now(),
      })
      .where('id', '=', object.id)
      .execute()

    if (event.method === 'HEAD') return ''

    try {
      const stream = await this.storage.adapter.createDownloadStream(object.objectName)
      return sendStream(event, Readable.toWeb(stream) as unknown as ReadableStream)
    } catch (err) {
      if (!(err instanceof ObjectNotFoundError)) throw err
      await this.markObjectError(object.objectKey)
      throw createError({ statusCode: 404, statusMessage: 'Cached registry object is missing' })
    }
  }

  private copyObjectHeaders(
    event: any,
    object: Pick<DockerRegistryObject, 'contentLength' | 'contentType' | 'digest' | 'etag'>,
  ) {
    this.setBaseHeaders(event)
    if (object.contentType) setHeader(event, 'content-type', object.contentType)
    if (object.contentLength !== null) setHeader(event, 'content-length', object.contentLength)
    if (object.digest) setHeader(event, 'docker-content-digest', object.digest)
    if (object.etag) setHeader(event, 'etag', object.etag)
  }

  private async pumpUpstreamToResponseAndStorage({
    contentType,
    digest,
    etag,
    objectKey,
    objectName,
    responseStream,
    source,
    storageStream,
  }: {
    contentType: string
    digest: string
    etag: string | null
    objectKey: string
    objectName: string
    responseStream: PassThrough
    source: Readable
    storageStream: PassThrough
  }) {
    const uploadPromise = this.storage.adapter.uploadStream(objectName, storageStream)
    let contentLength = 0

    try {
      for await (const chunk of source) {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
        contentLength += buffer.length

        if (!responseStream.write(buffer)) await once(responseStream, 'drain')
        if (!storageStream.write(buffer)) await once(storageStream, 'drain')
      }

      responseStream.end()
      storageStream.end()
      await uploadPromise
      await this.markObjectReady({
        contentLength,
        contentType,
        digest,
        etag,
        objectKey,
      })
    } catch (err) {
      responseStream.destroy(err as Error)
      storageStream.destroy(err as Error)
      await this.markObjectError(objectKey)
      throw err
    }
  }

  private async proxyUpstream(event: any, repository: string, upstreamPath: string) {
    const body = methodAllowsBody(event.method) ? getRequestWebStream(event) : undefined
    const upstream = await this.fetchUpstream(repository, upstreamPath, {
      body: body as BodyInit | undefined,
      headers: this.forwardedRequestHeaders(event, ['accept', 'range']),
      method: event.method,
    })
    return this.proxyUpstreamResponse(event, upstream)
  }

  private async proxyUpstreamResponse(event: any, upstream: Response, objectKey?: string) {
    this.setBaseHeaders(event)
    setResponseStatus(event, upstream.status, upstream.statusText)

    for (const header of [
      'content-length',
      'content-type',
      'docker-content-digest',
      'etag',
      'location',
      'range',
      'www-authenticate',
    ]) {
      const value = upstream.headers.get(header)
      if (value) setHeader(event, header, value)
    }

    if (!upstream.ok && objectKey) await this.markObjectError(objectKey)
    if (event.method === 'HEAD' || !upstream.body) return ''

    return sendStream(event, upstream.body as unknown as ReadableStream)
  }

  private async fetchUpstream(
    repository: string,
    upstreamPath: string,
    init: RequestInit & { headers?: HeadersInit },
  ) {
    const token = await this.getToken(repository)
    const headers = new Headers(init.headers)
    headers.set('authorization', `Bearer ${token}`)
    headers.set('user-agent', '@falcondev-oss/github-actions-cache-server')

    return fetch(`${env.DOCKERHUB_REGISTRY_URL}/v2/${repository}/${upstreamPath}`, {
      ...init,
      headers,
    })
  }

  private forwardedRequestHeaders(event: any, names: string[]) {
    const headers = new Headers()
    for (const name of names) {
      const value = getHeader(event, name)
      if (value) headers.set(name, value)
    }
    return headers
  }

  private async getToken(repository: string) {
    const scope = `repository:${repository}:pull`
    const cached = this.tokenCache.get(scope)
    if (cached && cached.expiresAt > Date.now()) return cached.token

    const url = new URL(env.DOCKERHUB_AUTH_URL)
    url.searchParams.set('service', 'registry.docker.io')
    url.searchParams.set('scope', scope)

    const headers = new Headers()
    const credentials = this.dockerHubCredentials()
    if (credentials) headers.set('authorization', credentials)

    const response = await fetch(url, {
      headers,
    })
    if (!response.ok)
      throw createError({
        statusCode: response.status,
        statusMessage: `Docker Hub auth failed: ${response.statusText}`,
      })

    const body = (await response.json()) as {
      access_token?: string
      expires_in?: number
      token?: string
    }
    const token = body.token ?? body.access_token
    if (!token) throw new Error('Docker Hub auth response did not include a token')

    this.tokenCache.set(scope, {
      expiresAt: Date.now() + Math.max((body.expires_in ?? 300) - 60, 60) * 1000,
      token,
    })
    return token
  }

  private dockerHubCredentials() {
    if (!env.DOCKERHUB_USERNAME && !env.DOCKERHUB_PASSWORD) return
    if (!env.DOCKERHUB_USERNAME || !env.DOCKERHUB_PASSWORD)
      throw createError({
        statusCode: 500,
        statusMessage: 'DOCKERHUB_USERNAME and DOCKERHUB_PASSWORD must be set together',
      })

    return `Basic ${Buffer.from(`${env.DOCKERHUB_USERNAME}:${env.DOCKERHUB_PASSWORD}`).toString(
      'base64',
    )}`
  }
}

export const getDockerRegistryMirror = createSingletonPromise(async () =>
  DockerRegistryMirror.fromEnv(),
)
