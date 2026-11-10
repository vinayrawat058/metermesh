import type { NodeEndpoint } from './types'

const DEFAULT_NODES = [
  'http://127.0.0.1:8081',
  'http://127.0.0.1:8082',
  'http://127.0.0.1:8083',
]

export function configuredNodes(): NodeEndpoint[] {
  const raw = import.meta.env.VITE_NODES as string | undefined
  const urls = (raw && raw.trim().length > 0 ? raw.split(',') : DEFAULT_NODES)
    .map((value) => value.trim())
    .filter(Boolean)

  return urls.map((baseUrl, index) => ({
    id: `node${index + 1}`,
    baseUrl: baseUrl.replace(/\/$/, ''),
  }))
}
