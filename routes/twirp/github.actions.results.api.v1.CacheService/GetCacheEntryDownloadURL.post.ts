import { map, pipe, prop, sortBy } from 'remeda'
import { z } from 'zod'
import { logger } from '~/lib/logger'
import { getCacheScope } from '~/lib/scope'
import { getStorage } from '~/lib/storage'

const bodySchema = z.object({
  key: z.string(),
  restore_keys: z.array(z.string()).nullish().optional(),
  version: z.string(),
})

export default defineEventHandler(async (event) => {
  const { scopes, repoId } = await getCacheScope(event)

  const parsedBody = bodySchema.safeParse(await readBody(event))
  if (!parsedBody.success)
    throw createError({
      statusCode: 400,
      statusMessage: `Invalid body: ${parsedBody.error.message}`,
    })

  const { key, restore_keys, version } = parsedBody.data
  logger.debug('Cache download URL lookup requested', {
    key,
    repoId,
    restoreKeys: restore_keys ?? [],
    version,
  })

  const storage = await getStorage()
  const match = await storage.getCacheEntryWithDownloadUrl({
    keys: [key, ...(restore_keys ?? [])],
    version,
    scopes: pipe(scopes, sortBy([prop('Permission'), 'desc']), map(prop('Scope'))),
    repoId,
  })
  if (!match) {
    logger.debug('Cache download URL lookup missed', {
      key,
      repoId,
      restoreKeys: restore_keys ?? [],
      version,
    })
    return {
      ok: false,
    }
  }

  logger.debug('Cache download URL lookup hit', {
    cacheEntryId: match.cacheEntry.id,
    key,
    matchedKey: match.cacheEntry.key,
    repoId,
    version,
  })

  return {
    ok: true,
    signed_download_url: match.downloadUrl,
    matched_key: match.cacheEntry.key,
  }
})
