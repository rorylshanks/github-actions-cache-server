import { getDockerRegistryMirror } from '~/lib/docker-registry'

export default defineEventHandler(async (event) => {
  const mirror = await getDockerRegistryMirror()
  return mirror.handlePing(event)
})
