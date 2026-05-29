import { Readable } from 'node:stream'
import { z } from 'zod'
import { logger } from '~/lib/logger'
import { getStorage } from '~/lib/storage'

const pathParamsSchema = z.object({
  cacheEntryId: z.string(),
})

export default defineEventHandler(async (event) => {
  const parsedPathParams = pathParamsSchema.safeParse(event.context.params)
  if (!parsedPathParams.success)
    throw createError({
      statusCode: 400,
      statusMessage: `Invalid path parameters: ${parsedPathParams.error.message}`,
    })

  const { cacheEntryId } = parsedPathParams.data
  logger.debug('Download route requested cache entry', { cacheEntryId })

  const storage = await getStorage()
  const stream = await storage.download(cacheEntryId)
  if (!stream) {
    logger.debug('Download route cache file not found', { cacheEntryId })
    throw createError({
      statusCode: 404,
      message: 'Cache file not found',
    })
  }

  return sendStream(event, Readable.toWeb(stream) as ReadableStream)
})
