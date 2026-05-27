/* eslint-disable no-shadow */
/* eslint-disable ts/method-signature-style */
import type { Kysely } from 'kysely'
import type { ReadableStream } from 'node:stream/web'
import type { Database, StorageLocation } from './db'
import type { Env } from './schemas'
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { createReadStream, createWriteStream } from 'node:fs'
import fs from 'node:fs/promises'
import { Agent } from 'node:https'
import path from 'node:path'
import { PassThrough, Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { createSingletonPromise } from '@antfu/utils'
import {
  DeleteObjectsCommand,
  GetObjectCommand,
  HeadBucketCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3'
import { Upload as S3Upload } from '@aws-sdk/lib-storage'
import { getSignedUrl } from '@aws-sdk/s3-request-presigner'
import { Storage as GcsClient } from '@google-cloud/storage'
import { NodeHttpHandler } from '@smithy/node-http-handler'
import { sql } from 'kysely'
import { chunk } from 'remeda'
import { match } from 'ts-pattern'
import { getDatabase } from './db'
import { env } from './env'
import { generateNumberId } from './helpers'
import { logger } from './logger'

function escapeLikePattern(value: string) {
  return value
    .replaceAll('\\', '\\\\')
    .replaceAll('%', String.raw`\%`)
    .replaceAll('_', String.raw`\_`)
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

export class ObjectNotFoundError extends Error {
  constructor(objectName: string) {
    super(`Object not found in storage: ${objectName}`)
    this.name = 'ObjectNotFoundError'
  }
}

export class Storage {
  adapter
  private db
  private mergeStreamPromises = new Set<Promise<void>>()

  private constructor({ db, adapter }: { adapter: StorageAdapter; db: Kysely<Database> }) {
    this.adapter = adapter
    this.db = db
  }

  static async getAdapterFromEnv() {
    const adapter = await match(env)
      .with({ STORAGE_DRIVER: 's3' }, S3Adapter.fromEnv)
      .with({ STORAGE_DRIVER: 'filesystem' }, FileSystemAdapter.fromEnv)
      .with({ STORAGE_DRIVER: 'gcs' }, GcsAdapter.fromEnv)
      .exhaustive()

    if (!env.STORAGE_FILESYSTEM_CACHE_PATH || env.STORAGE_DRIVER === 'filesystem') return adapter

    return FilesystemCachingAdapter.fromEnv({
      backend: adapter,
      cachePath: env.STORAGE_FILESYSTEM_CACHE_PATH,
      maxObjectSize: env.STORAGE_FILESYSTEM_CACHE_MAX_OBJECT_SIZE_BYTES,
      maxSize: env.STORAGE_FILESYSTEM_CACHE_MAX_SIZE_BYTES,
    })
  }

  static async fromEnv() {
    return new Storage({
      adapter: await Storage.getAdapterFromEnv(),
      db: await getDatabase(),
    })
  }

  waitForOngoingMerges() {
    return Promise.all([
      ...this.mergeStreamPromises,
      ...(this.adapter.waitForIdle ? [this.adapter.waitForIdle()] : []),
    ])
  }

  async uploadPart(uploadId: number, partIndex: number, stream: ReadableStream) {
    const upload = await this.db
      .selectFrom('uploads')
      .where('id', '=', uploadId)
      .select(['folderName'])
      .executeTakeFirst()
    if (!upload) return

    await this.db
      .updateTable('uploads')
      .set({
        startedPartUploadCount: sql`${sql.ref('startedPartUploadCount')} + 1`,
      })
      .where('id', '=', uploadId)
      .execute()

    await this.adapter.uploadStream(
      `${upload.folderName}/parts/${partIndex}`,
      Readable.fromWeb(stream),
    )

    await this.db
      .updateTable('uploads')
      .set({
        lastPartUploadedAt: Date.now(),
        finishedPartUploadCount: sql`${sql.ref('finishedPartUploadCount')} + 1`,
      })
      .where('id', '=', uploadId)
      .execute()
  }

  async completeUpload({
    key,
    version,
    scope,
    repoId,
  }: {
    key: string
    version: string
    scope: string
    repoId: string
  }) {
    const upload = await this.db
      .selectFrom('uploads')
      .where('key', '=', key)
      .where('version', '=', version)
      .where('scope', '=', scope)
      .where('repoId', '=', repoId)
      .selectAll()
      .executeTakeFirst()
    if (!upload) return

    if (upload.finishedPartUploadCount === 0) {
      await this.db.deleteFrom('uploads').where('id', '=', upload.id).execute()
      throw new Error('No parts have been uploaded')
    }

    if (upload.startedPartUploadCount !== upload.finishedPartUploadCount) {
      await this.db.deleteFrom('uploads').where('id', '=', upload.id).execute()
      throw new Error(
        `Not all parts have been uploaded (only ${upload.finishedPartUploadCount} of ${upload.startedPartUploadCount} parts uploaded)`,
      )
    }

    const partCount = await this.adapter.countFilesInFolder(`${upload.folderName}/parts`)
    if (partCount !== upload.finishedPartUploadCount) {
      await this.db.deleteFrom('uploads').where('id', '=', upload.id).execute()
      throw new Error(
        `Uploaded part count does not match actual part count in storage (expected ${upload.finishedPartUploadCount} but found ${partCount})`,
      )
    }

    await this.db.transaction().execute(async (tx) => {
      const existingLocation = await tx
        .selectFrom('storage_locations')
        .where('folderName', '=', upload.folderName)
        .select('id')
        .executeTakeFirst()
      const locationId = existingLocation?.id ?? randomUUID()

      if (existingLocation) {
        await tx
          .updateTable('storage_locations')
          .set({
            partCount,
            availableAt: Date.now(),
            mergedAt: null,
            mergeStartedAt: null,
            partsDeletedAt: null,
          })
          .where('id', '=', locationId)
          .execute()
      } else {
        await tx
          .insertInto('storage_locations')
          .values({
            id: locationId,
            folderName: upload.folderName,
            partCount,
            availableAt: Date.now(),
            mergedAt: null,
            mergeStartedAt: null,
            partsDeletedAt: null,
            lastDownloadedAt: null,
          })
          .execute()
      }

      const existingCacheEntry = await tx
        .selectFrom('cache_entries')
        .where('key', '=', key)
        .where('version', '=', version)
        .where('scope', '=', scope)
        .where('repoId', '=', repoId)
        .innerJoin('storage_locations', 'storage_locations.id', 'cache_entries.locationId')
        .select(['cache_entries.id', 'cache_entries.locationId', 'storage_locations.folderName'])
        .executeTakeFirst()

      if (existingCacheEntry?.locationId === locationId) {
        await tx
          .updateTable('cache_entries')
          .set({
            updatedAt: Date.now(),
          })
          .where('id', '=', existingCacheEntry.id)
          .execute()
      } else if (existingCacheEntry) {
        await tx
          .updateTable('cache_entries')
          .set({
            updatedAt: Date.now(),
            locationId,
          })
          .where('id', '=', existingCacheEntry.id)
          .execute()
        await tx
          .deleteFrom('storage_locations')
          .where('id', '=', existingCacheEntry.locationId)
          .execute()
        await this.adapter.deleteFolder(existingCacheEntry.folderName)
      } else
        await tx
          .insertInto('cache_entries')
          .values({
            key: upload.key,
            version: upload.version,
            id: randomUUID(),
            updatedAt: Date.now(),
            locationId,
            scope,
            repoId,
          })
          .execute()

      await tx.deleteFrom('uploads').where('id', '=', upload.id).execute()
    })

    return upload
  }

  async download(cacheEntryId: string): Promise<Readable | undefined> {
    const storageLocation = await this.db
      .selectFrom('storage_locations')
      .innerJoin('cache_entries', 'cache_entries.locationId', 'storage_locations.id')
      .where('cache_entries.id', '=', cacheEntryId)
      .selectAll('storage_locations')
      .executeTakeFirst()
    if (!storageLocation) return

    const availableLocation = storageLocation.availableAt
      ? storageLocation
      : await this.waitForStorageLocationAvailability(storageLocation.id)
    if (!availableLocation) return

    void this.db
      .updateTable('storage_locations')
      .set({
        lastDownloadedAt: Date.now(),
      })
      .where('id', '=', availableLocation.id)
      .execute()

    try {
      if (availableLocation.mergedAt || availableLocation.mergeStartedAt)
        return await this.downloadFromCacheEntryLocation(availableLocation)

      await this.ensurePartsExist(availableLocation)

      await this.db
        .updateTable('storage_locations')
        .set({
          mergeStartedAt: Date.now(),
        })
        .where('id', '=', availableLocation.id)
        .execute()

      const responseStream = new PassThrough()
      const mergerStream = new PassThrough()

      const mergePromise = this.adapter
        .uploadStream(`${availableLocation.folderName}/merged`, mergerStream)
        .then(async () => {
          await this.db
            .updateTable('storage_locations')
            .set({
              mergedAt: Date.now(),
            })
            .where('id', '=', availableLocation.id)
            .execute()
          await this.db.transaction().execute(async (tx) => {
            await tx
              .updateTable('storage_locations')
              .set({
                partsDeletedAt: Date.now(),
              })
              .where('id', '=', availableLocation.id)
              .execute()
            await this.adapter.deleteFolder(`${availableLocation.folderName}/parts`)
          })
        })
        .catch(async () => {
          await this.db
            .updateTable('storage_locations')
            .set({
              mergedAt: null,
              mergeStartedAt: null,
            })
            .where('id', '=', availableLocation.id)
            .execute()
          mergerStream.destroy()
        })
      this.mergeStreamPromises.add(mergePromise)
      mergePromise.finally(() => this.mergeStreamPromises.delete(mergePromise))

      this.pumpPartsToStreams(availableLocation, responseStream, mergerStream).catch((err) => {
        responseStream.destroy(err)
        mergerStream.destroy(err)
        if (err instanceof ObjectNotFoundError)
          logger.warn(`Stale cache entry ${cacheEntryId}: ${err.message}`)
      })

      return responseStream
    } catch (err) {
      if (err instanceof ObjectNotFoundError) {
        logger.warn(`Stale cache entry ${cacheEntryId}: ${err.message}`)
        return
      }
      throw err
    }
  }

  private async waitForStorageLocationAvailability(locationId: string) {
    const deadline = Date.now() + env.CACHE_PENDING_WAIT_TIMEOUT_MS

    while (Date.now() < deadline) {
      const location = await this.db
        .selectFrom('storage_locations')
        .where('id', '=', locationId)
        .selectAll()
        .executeTakeFirst()
      if (!location) return
      if (location.availableAt) return location
      await sleep(250)
    }

    logger.warn(`Timed out waiting for pending cache storage location ${locationId}`)
  }

  private async ensurePartsExist(location: StorageLocation) {
    const partsFolder = `${location.folderName}/parts`
    const actualPartCount = await this.adapter.countFilesInFolder(partsFolder)
    if (actualPartCount < location.partCount) throw new ObjectNotFoundError(partsFolder)
  }

  private async downloadFromCacheEntryLocation(location: StorageLocation) {
    if (location.mergedAt) return this.adapter.createDownloadStream(`${location.folderName}/merged`)

    await this.ensurePartsExist(location)
    return Readable.from(this.streamParts(location))
  }

  private async pumpPartsToStreams(
    location: StorageLocation,
    responseStream: PassThrough,
    mergerStream: PassThrough,
  ) {
    if (location.partsDeletedAt) throw new Error('No parts to feed')

    for await (const chunk of this.streamParts(location)) {
      const responseWantsMore = responseStream.write(chunk)
      const mergerWantsMore = mergerStream.write(chunk)

      if (!responseWantsMore) await once(responseStream, 'drain')
      if (!mergerWantsMore) await once(mergerStream, 'drain')
    }

    responseStream.end()
    mergerStream.end()

    await globalThis.gc?.()
  }

  private async *streamParts(location: StorageLocation) {
    if (location.partsDeletedAt) throw new Error('No parts to feed for location with deleted parts')

    for (let i = 0; i < location.partCount; i++) {
      const partStream = await this.adapter.createDownloadStream(
        `${location.folderName}/parts/${i}`,
      )

      for await (const chunk of partStream) yield chunk

      await globalThis.gc?.()
    }
  }

  async createUpload({
    key,
    version,
    scope,
    repoId,
  }: {
    key: string
    version: string
    scope: string
    repoId: string
  }) {
    const existingUpload = await this.db
      .selectFrom('uploads')
      .where('key', '=', key)
      .where('version', '=', version)
      .where('scope', '=', scope)
      .where('repoId', '=', repoId)
      .select('id')
      .executeTakeFirst()
    if (existingUpload) return

    const uploadId = generateNumberId()
    const folderName = uploadId.toString()
    await this.db.transaction().execute(async (tx) => {
      await tx
        .insertInto('uploads')
        .values({
          id: uploadId,
          folderName,
          createdAt: Date.now(),
          key,
          version,
          scope,
          repoId,
          lastPartUploadedAt: null,
          finishedPartUploadCount: 0,
          startedPartUploadCount: 0,
        })
        .execute()

      const existingCacheEntry = await tx
        .selectFrom('cache_entries')
        .where('key', '=', key)
        .where('version', '=', version)
        .where('scope', '=', scope)
        .where('repoId', '=', repoId)
        .select('id')
        .executeTakeFirst()
      if (existingCacheEntry) return

      const locationId = randomUUID()
      await tx
        .insertInto('storage_locations')
        .values({
          id: locationId,
          folderName,
          partCount: 0,
          availableAt: null,
          mergedAt: null,
          mergeStartedAt: null,
          partsDeletedAt: null,
          lastDownloadedAt: null,
        })
        .execute()
      await tx
        .insertInto('cache_entries')
        .values({
          key,
          version,
          id: randomUUID(),
          updatedAt: Date.now(),
          locationId,
          scope,
          repoId,
        })
        .execute()
    })

    return { id: uploadId }
  }

  async matchCacheEntry({
    keys: [primaryKey, ...restoreKeys],
    version,
    scopes,
    repoId,
  }: {
    keys: [string, ...string[]]
    version: string
    scopes: string[]
    repoId: string
  }) {
    for (const scope of scopes) {
      const exactPrimaryMatch = await this.db
        .selectFrom('cache_entries')
        .where('key', '=', primaryKey)
        .where('version', '=', version)
        .where('scope', '=', scope)
        .where('repoId', '=', repoId)
        .selectAll()
        .executeTakeFirst()
      if (exactPrimaryMatch)
        return {
          match: exactPrimaryMatch,
          type: 'exact-primary' as const,
        }

      const prefixedPrimaryMatch = await this.db
        .selectFrom('cache_entries')
        .where(
          sql<boolean>`${sql.ref('key')} like ${`${escapeLikePattern(primaryKey)}%`} escape ${'\\'}`,
        )
        .where('version', '=', version)
        .where('scope', '=', scope)
        .where('repoId', '=', repoId)
        .orderBy('cache_entries.updatedAt', 'desc')
        .selectAll()
        .executeTakeFirst()

      if (prefixedPrimaryMatch)
        return {
          match: prefixedPrimaryMatch,
          type: 'prefixed-primary' as const,
        }

      if (restoreKeys.length === 0) continue

      for (const key of restoreKeys) {
        const exactMatch = await this.db
          .selectFrom('cache_entries')
          .where('key', '=', key)
          .where('version', '=', version)
          .where('scope', '=', scope)
          .where('repoId', '=', repoId)
          .orderBy('updatedAt', 'desc')
          .selectAll()
          .executeTakeFirst()
        if (exactMatch)
          return {
            match: exactMatch,
            type: 'exact-restore' as const,
          }

        const prefixedMatch = await this.db
          .selectFrom('cache_entries')
          .where(
            sql<boolean>`${sql.ref('key')} like ${`${escapeLikePattern(key)}%`} escape ${'\\'}`,
          )
          .where('version', '=', version)
          .where('scope', '=', scope)
          .where('repoId', '=', repoId)
          .orderBy('updatedAt', 'desc')
          .selectAll()
          .executeTakeFirst()

        if (prefixedMatch)
          return {
            match: prefixedMatch,
            type: 'prefixed-restore' as const,
          }
      }
    }
  }

  async getCacheEntryWithDownloadUrl(args: Parameters<typeof this.matchCacheEntry>[0]) {
    const cacheEntry = await this.matchCacheEntry(args)
    if (!cacheEntry) return

    const defaultUrl = `${env.API_BASE_URL}/download/${cacheEntry.match.id}`

    if (!env.ENABLE_DIRECT_DOWNLOADS || !this.adapter.createDownloadUrl)
      return {
        downloadUrl: defaultUrl,
        cacheEntry: cacheEntry.match,
      }

    const location = await this.db
      .selectFrom('storage_locations')
      .where('id', '=', cacheEntry.match.locationId)
      .select(['availableAt', 'folderName', 'mergedAt'])
      .executeTakeFirst()
    if (!location) throw new Error('Storage location not found')

    const downloadUrl =
      location.availableAt && location.mergedAt
        ? await this.adapter.createDownloadUrl(`${location.folderName}/merged`)
        : defaultUrl

    return {
      downloadUrl,
      cacheEntry: cacheEntry.match,
    }
  }
}

export const getStorage = createSingletonPromise(async () => Storage.fromEnv())

export interface StorageAdapter {
  createDownloadStream(objectName: string): Promise<Readable>
  uploadStream(objectName: string, stream: Readable): Promise<void>
  deleteFolder(folderName: string): Promise<void>
  countFilesInFolder(folderName: string): Promise<number>
  createDownloadUrl?(objectName: string): Promise<string>
  clear(): Promise<void>
  waitForIdle?(): Promise<void>
}

class S3Adapter implements StorageAdapter {
  private s3
  private bucket
  private keyPrefix = 'gh-actions-cache'

  constructor({ bucket, s3 }: { s3: S3Client; bucket: string }) {
    this.s3 = s3
    this.bucket = bucket
  }

  static async fromEnv(env: Extract<Env, { STORAGE_DRIVER: 's3' }>) {
    const bucket = env.STORAGE_S3_BUCKET
    const agent = new Agent({
      keepAlive: true,
      maxSockets: 50,
      keepAliveMsecs: 1000,
    })
    const s3 = new S3Client({
      forcePathStyle: true,
      region: env.AWS_REGION,
      requestHandler: new NodeHttpHandler({
        httpsAgent: agent,
        socketTimeout: 3000,
      }),
    })

    try {
      await s3.send(
        new HeadBucketCommand({
          Bucket: bucket,
        }),
      )
    } catch (err: any) {
      if (err.name === 'NotFound') {
        throw new Error(`Bucket ${bucket} does not exist`)
      }
      throw err
    }

    return new S3Adapter({ s3, bucket })
  }

  async createDownloadStream(objectName: string) {
    try {
      const response = await this.s3.send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: `${this.keyPrefix}/${objectName}`,
        }),
      )
      if (!response.Body) throw new Error('No body in S3 get object response')

      return response.Body as Readable
    } catch (err: any) {
      if (err.name === 'NoSuchKey') throw new ObjectNotFoundError(objectName)
      throw err
    }
  }

  async deleteFolder(folderName: string) {
    return this.deleteByPrefix(`${this.keyPrefix}/${folderName}/`)
  }

  async clear() {
    return this.deleteByPrefix(this.keyPrefix)
  }

  private async deleteByPrefix(prefix: string) {
    const listResponse = await this.s3.send(
      new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: prefix,
      }),
    )

    if (!listResponse.Contents || listResponse.Contents.length === 0) return

    await Promise.all(
      chunk(
        listResponse.Contents.filter((obj): obj is { Key: string } => !!obj.Key),
        1000,
      ).map((chunkedObjects) =>
        this.s3.send(
          new DeleteObjectsCommand({
            Bucket: this.bucket,
            Delete: {
              Objects: chunkedObjects.map((obj) => ({
                Key: obj.Key,
              })),
              Quiet: true,
            },
          }),
        ),
      ),
    )
  }

  async uploadStream(objectName: string, iterator: AsyncIterable<Uint8Array>) {
    await new S3Upload({
      client: this.s3,
      params: {
        Bucket: this.bucket,
        Key: `${this.keyPrefix}/${objectName}`,
        Body: iterator as Readable,
      },
      queueSize: 1,
      partSize: 5 * 1024 * 1024, // 5MB
      leavePartsOnError: false,
    }).done()
  }

  async countFilesInFolder(folderName: string) {
    const listResponse = await this.s3.send(
      new ListObjectsV2Command({
        Bucket: this.bucket,
        Prefix: `${this.keyPrefix}/${folderName}/`,
      }),
    )

    return listResponse.KeyCount ?? 0
  }

  async createDownloadUrl(objectName: string) {
    return getSignedUrl(
      this.s3,
      new GetObjectCommand({
        Bucket: this.bucket,
        Key: `${this.keyPrefix}/${objectName}`,
      }),
      {
        expiresIn: 10 * 60 * 1000, // 10min
      },
    )
  }
}

class FileSystemAdapter implements StorageAdapter {
  private rootFolder

  constructor({ rootFolder }: { rootFolder: string }) {
    this.rootFolder = path.resolve(rootFolder)
  }

  private safePath(name: string) {
    const resolved = path.resolve(this.rootFolder, name)
    if (!resolved.startsWith(this.rootFolder + path.sep) && resolved !== this.rootFolder)
      throw new Error(`Invalid object name`)
    return resolved
  }

  static async fromEnv(env: Extract<Env, { STORAGE_DRIVER: 'filesystem' }>) {
    const rootFolder = env.STORAGE_FILESYSTEM_PATH
    await fs.mkdir(rootFolder, {
      recursive: true,
    })

    return new FileSystemAdapter({
      rootFolder,
    })
  }

  async createDownloadStream(objectName: string) {
    const filePath = this.safePath(objectName)
    try {
      await fs.access(filePath)
    } catch {
      throw new ObjectNotFoundError(objectName)
    }
    return createReadStream(filePath)
  }

  async deleteFolder(folderName: string) {
    await fs.rm(this.safePath(folderName), {
      recursive: true,
      force: true,
    })
  }

  async clear() {
    await fs.rm(this.rootFolder, {
      recursive: true,
      force: true,
    })
    await fs.mkdir(this.rootFolder, {
      recursive: true,
    })
  }

  async uploadStream(objectName: string, stream: Readable) {
    const filePath = this.safePath(objectName)
    await fs.mkdir(path.dirname(filePath), { recursive: true })
    await pipeline(stream, createWriteStream(filePath))
  }

  async countFilesInFolder(folderName: string) {
    try {
      const dir = await fs.readdir(this.safePath(folderName), {
        withFileTypes: true,
      })
      return dir.filter((item) => item.isFile()).length
    } catch (err: any) {
      if (err.code === 'ENOENT') return 0
      throw err
    }
  }
}

class FilesystemObjectCache {
  private rootFolder
  private maxObjectSize
  private maxSize

  constructor({
    maxObjectSize,
    maxSize,
    rootFolder,
  }: {
    maxObjectSize: number
    maxSize: number
    rootFolder: string
  }) {
    this.rootFolder = path.resolve(rootFolder)
    this.maxObjectSize = maxObjectSize
    this.maxSize = maxSize
  }

  static async create(args: ConstructorParameters<typeof FilesystemObjectCache>[0]) {
    await fs.mkdir(args.rootFolder, { recursive: true })
    return new FilesystemObjectCache(args)
  }

  private safePath(name: string) {
    const resolved = path.resolve(this.rootFolder, name)
    if (!resolved.startsWith(this.rootFolder + path.sep) && resolved !== this.rootFolder)
      throw new Error(`Invalid object name`)
    return resolved
  }

  private markerPath(objectName: string) {
    return `${this.safePath(objectName)}.writeback`
  }

  private isCacheMetadataFile(filePath: string) {
    return filePath.includes(`${path.sep}.tmp-`) || filePath.endsWith('.writeback')
  }

  private async touch(filePath: string) {
    const now = new Date()
    await fs.utimes(filePath, now, now).catch(() => {})
  }

  async createDownloadStream(objectName: string) {
    const filePath = this.safePath(objectName)
    try {
      await fs.access(filePath)
      await this.touch(filePath)
      return createReadStream(filePath)
    } catch {
      throw new ObjectNotFoundError(objectName)
    }
  }

  async countFilesInFolder(folderName: string) {
    try {
      const dir = await fs.readdir(this.safePath(folderName), {
        withFileTypes: true,
      })
      return dir.filter((item) => {
        const itemPath = path.join(this.safePath(folderName), item.name)
        return item.isFile() && !this.isCacheMetadataFile(itemPath)
      }).length
    } catch (err: any) {
      if (err.code === 'ENOENT') return 0
      throw err
    }
  }

  async deleteFolder(folderName: string) {
    await fs.rm(this.safePath(folderName), {
      recursive: true,
      force: true,
    })
  }

  async clear() {
    await fs.rm(this.rootFolder, {
      recursive: true,
      force: true,
    })
    await fs.mkdir(this.rootFolder, {
      recursive: true,
    })
  }

  async putForWriteback(objectName: string, stream: Readable) {
    const markerPath = this.markerPath(objectName)
    await fs.mkdir(path.dirname(markerPath), { recursive: true })
    await fs.writeFile(markerPath, '')

    try {
      return await this.putObject(objectName, stream)
    } catch (err) {
      await fs.rm(markerPath, { force: true }).catch(() => {})
      throw err
    }
  }

  async finishWriteback(objectName: string, size: number) {
    await fs.rm(this.markerPath(objectName), { force: true }).catch(() => {})

    if (size > this.maxObjectSize)
      await fs.rm(this.safePath(objectName), { force: true }).catch(() => {})

    await this.evictIfNeeded()
  }

  private async putObject(objectName: string, stream: Readable) {
    const filePath = this.safePath(objectName)
    const tmpPath = path.join(
      path.dirname(filePath),
      `.tmp-${path.basename(filePath)}-${randomUUID()}`,
    )
    await fs.mkdir(path.dirname(filePath), { recursive: true })

    let size = 0
    const counter = new PassThrough()
    counter.on('data', (chunk: Buffer) => {
      size += chunk.length
    })

    try {
      await pipeline(stream, counter, createWriteStream(tmpPath))
      await fs.rename(tmpPath, filePath)
      await this.touch(filePath)
      return { filePath, size }
    } catch (err) {
      await fs.rm(tmpPath, { force: true }).catch(() => {})
      throw err
    }
  }

  async cacheAndStream(objectName: string, source: Readable) {
    const filePath = this.safePath(objectName)
    const tmpPath = path.join(
      path.dirname(filePath),
      `.tmp-${path.basename(filePath)}-${randomUUID()}`,
    )
    await fs.mkdir(path.dirname(filePath), { recursive: true })

    const responseStream = new PassThrough()
    let cacheWriter: ReturnType<typeof createWriteStream> | undefined = createWriteStream(tmpPath)

    const done = (async () => {
      let size = 0
      try {
        for await (const chunk of source) {
          const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
          size += buffer.length

          if (cacheWriter) {
            if (size <= this.maxObjectSize) {
              if (!cacheWriter.write(buffer)) await once(cacheWriter, 'drain')
            } else {
              cacheWriter.destroy()
              cacheWriter = undefined
              await fs.rm(tmpPath, { force: true }).catch(() => {})
            }
          }

          if (!responseStream.write(buffer)) await once(responseStream, 'drain')
        }

        responseStream.end()

        if (cacheWriter) {
          cacheWriter.end()
          await once(cacheWriter, 'finish')
          await fs.rename(tmpPath, filePath)
          await this.touch(filePath)
          await this.evictIfNeeded()
        }
      } catch (err) {
        responseStream.destroy(err as Error)
        cacheWriter?.destroy()
        await fs.rm(tmpPath, { force: true }).catch(() => {})
        throw err
      }
    })()

    return { done, stream: responseStream }
  }

  private async listObjects() {
    const objects: { filePath: string; lastAccessedAt: number; size: number }[] = []

    const walk = async (folder: string) => {
      let entries
      try {
        entries = await fs.readdir(folder, { withFileTypes: true })
      } catch (err: any) {
        if (err.code === 'ENOENT') return
        throw err
      }

      for (const entry of entries) {
        const entryPath = path.join(folder, entry.name)
        if (entry.isDirectory()) {
          await walk(entryPath)
          continue
        }
        if (!entry.isFile() || this.isCacheMetadataFile(entryPath)) continue

        const markerPath = `${entryPath}.writeback`
        const [stat, isWritebackPending] = await Promise.all([
          fs.stat(entryPath),
          fs
            .access(markerPath)
            .then(() => true)
            .catch(() => false),
        ])
        objects.push({
          filePath: entryPath,
          lastAccessedAt: stat.mtimeMs,
          size: stat.size,
          ...(isWritebackPending ? { lastAccessedAt: Number.POSITIVE_INFINITY } : {}),
        })
      }
    }

    await walk(this.rootFolder)
    return objects
  }

  private async evictIfNeeded() {
    const objects = await this.listObjects()
    let totalSize = objects.reduce((sum, object) => sum + object.size, 0)
    if (totalSize <= this.maxSize) return

    for (const object of objects.sort((a, b) => a.lastAccessedAt - b.lastAccessedAt)) {
      if (object.lastAccessedAt === Number.POSITIVE_INFINITY) continue
      await fs.rm(object.filePath, { force: true }).catch(() => {})
      totalSize -= object.size
      if (totalSize <= this.maxSize) return
    }
  }
}

export class FilesystemCachingAdapter implements StorageAdapter {
  private backend
  private cache
  private downloadFills = new Map<string, Promise<void>>()
  private writebackPromises = new Set<Promise<void>>()

  constructor({ backend, cache }: { backend: StorageAdapter; cache: FilesystemObjectCache }) {
    this.backend = backend
    this.cache = cache
  }

  static async fromEnv({
    backend,
    cachePath,
    maxObjectSize,
    maxSize,
  }: {
    backend: StorageAdapter
    cachePath: string
    maxObjectSize: number
    maxSize: number
  }) {
    logger.info(`Using filesystem cache at ${cachePath}`)
    return new FilesystemCachingAdapter({
      backend,
      cache: await FilesystemObjectCache.create({
        maxObjectSize,
        maxSize,
        rootFolder: cachePath,
      }),
    })
  }

  async createDownloadStream(objectName: string) {
    try {
      return await this.cache.createDownloadStream(objectName)
    } catch (err) {
      if (!(err instanceof ObjectNotFoundError)) throw err
    }

    const existingFill = this.downloadFills.get(objectName)
    if (existingFill) {
      await existingFill
      try {
        return await this.cache.createDownloadStream(objectName)
      } catch (err) {
        if (!(err instanceof ObjectNotFoundError)) throw err
      }
    }

    const source = await this.backend.createDownloadStream(objectName)
    const { done, stream } = await this.cache.cacheAndStream(objectName, source)
    const fill = done.finally(() => this.downloadFills.delete(objectName))
    this.downloadFills.set(objectName, fill)
    fill.catch((err) => logger.warn(`Failed to populate filesystem cache for ${objectName}`, err))

    return stream
  }

  async uploadStream(objectName: string, stream: Readable) {
    const { filePath, size } = await this.cache.putForWriteback(objectName, stream)
    const writeback = this.backend
      .uploadStream(objectName, createReadStream(filePath))
      .then(() => this.cache.finishWriteback(objectName, size))
      .catch((err) => {
        logger.error(`Failed to write back ${objectName} to backing storage`, err)
      })
      .finally(() => {
        this.writebackPromises.delete(writeback)
      })

    this.writebackPromises.add(writeback)
  }

  async deleteFolder(folderName: string) {
    await Promise.all([this.cache.deleteFolder(folderName), this.backend.deleteFolder(folderName)])
  }

  async countFilesInFolder(folderName: string) {
    const [localCount, backendCount] = await Promise.all([
      this.cache.countFilesInFolder(folderName),
      this.backend.countFilesInFolder(folderName),
    ])
    return Math.max(localCount, backendCount)
  }

  async clear() {
    await Promise.all([this.cache.clear(), this.backend.clear()])
  }

  async waitForIdle() {
    await Promise.all([
      ...this.downloadFills.values(),
      ...this.writebackPromises,
      ...(this.backend.waitForIdle ? [this.backend.waitForIdle()] : []),
    ])
  }
}

class GcsAdapter implements StorageAdapter {
  private bucket
  private keyPrefix = 'gh-actions-cache'

  constructor({ bucket, gcs }: { bucket: string; gcs: GcsClient }) {
    this.bucket = gcs.bucket(bucket)
  }

  static async fromEnv(env: Extract<Env, { STORAGE_DRIVER: 'gcs' }>) {
    const bucketName = env.STORAGE_GCS_BUCKET

    const gcs = new GcsClient({
      keyFilename: env.STORAGE_GCS_SERVICE_ACCOUNT_KEY,
      apiEndpoint: env.STORAGE_GCS_ENDPOINT,
    })
    const bucket = gcs.bucket(bucketName)

    await bucket.getMetadata()

    return new GcsAdapter({
      bucket: bucketName,
      gcs,
    })
  }

  async createDownloadStream(objectName: string) {
    const file = this.bucket.file(`${this.keyPrefix}/${objectName}`)
    const [exists] = await file.exists()
    if (!exists) throw new ObjectNotFoundError(objectName)
    return file.createReadStream()
  }

  async deleteFolder(folderName: string) {
    await this.bucket.deleteFiles({
      prefix: `${this.keyPrefix}/${folderName}/`,
    })
  }

  async clear() {
    await this.bucket.deleteFiles({
      prefix: this.keyPrefix,
    })
  }

  async uploadStream(objectName: string, iterator: AsyncIterable<Uint8Array>) {
    const file = this.bucket.file(`${this.keyPrefix}/${objectName}`)

    await pipeline(
      iterator,
      file.createWriteStream({
        resumable: false,
        validation: false,
      }),
    )
  }

  async countFilesInFolder(folderName: string) {
    return this.bucket
      .getFiles({
        prefix: `${this.keyPrefix}/${folderName}/`,
        autoPaginate: true,
      })
      .then((res) => res[0].length)
  }

  async createDownloadUrl(objectName: string) {
    return this.bucket
      .file(`${this.keyPrefix}/${objectName}`)
      .getSignedUrl({
        action: 'read',
        expires: Date.now() + 10 * 60 * 1000, // 10min
      })
      .then((res) => res[0])
  }
}
