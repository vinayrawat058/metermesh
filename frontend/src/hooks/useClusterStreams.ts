import { useEffect, useMemo, useState } from 'react'
import { configuredNodes } from '../nodes'
import type { MetricsEvent, NodeLiveState } from '../types'

const HISTORY = 24
// EventSource fires onerror while the browser is still reconnecting.
// Wait out a short grace window so brief reconnects do not blink the UI.
const DISCONNECT_GRACE_MS = 2500

function emptyState(endpoint: ReturnType<typeof configuredNodes>[number]): NodeLiveState {
  return {
    endpoint,
    connected: false,
    metrics: null,
    history: [],
    error: null,
  }
}

export function useClusterStreams() {
  const endpoints = useMemo(() => configuredNodes(), [])
  const [nodes, setNodes] = useState<NodeLiveState[]>(() => endpoints.map(emptyState))

  useEffect(() => {
    const sources: EventSource[] = []
    const disconnectTimers: Array<ReturnType<typeof setTimeout> | undefined> = []

    const clearDisconnectTimer = (index: number) => {
      const timer = disconnectTimers[index]
      if (timer !== undefined) {
        clearTimeout(timer)
        disconnectTimers[index] = undefined
      }
    }

    const markDisconnected = (index: number, endpoint: (typeof endpoints)[number], error: string) => {
      setNodes((current) => {
        const next = [...current]
        next[index] = {
          ...(next[index] ?? emptyState(endpoint)),
          connected: false,
          error,
        }
        return next
      })
    }

    endpoints.forEach((endpoint, index) => {
      const source = new EventSource(`${endpoint.baseUrl}/v1/metrics/stream`)
      sources.push(source)

      source.addEventListener('metrics', (message) => {
        clearDisconnectTimer(index)
        try {
          const metrics = JSON.parse((message as MessageEvent).data) as MetricsEvent
          setNodes((current) => {
            const next = [...current]
            const prev = next[index] ?? emptyState(endpoint)
            const history = [...prev.history, metrics.events_per_sec].slice(-HISTORY)
            next[index] = {
              endpoint,
              connected: true,
              metrics,
              history,
              error: null,
            }
            return next
          })
        } catch (error) {
          markDisconnected(
            index,
            endpoint,
            error instanceof Error ? error.message : 'bad metrics payload',
          )
        }
      })

      source.onerror = () => {
        // Still reconnecting — keep the last live metrics until grace expires.
        if (source.readyState === EventSource.CONNECTING) {
          if (disconnectTimers[index] !== undefined) {
            return
          }
          disconnectTimers[index] = setTimeout(() => {
            disconnectTimers[index] = undefined
            markDisconnected(index, endpoint, 'stream disconnected')
          }, DISCONNECT_GRACE_MS)
          return
        }

        clearDisconnectTimer(index)
        markDisconnected(index, endpoint, 'stream disconnected')
      }
    })

    return () => {
      disconnectTimers.forEach((timer) => {
        if (timer !== undefined) {
          clearTimeout(timer)
        }
      })
      sources.forEach((source) => source.close())
    }
  }, [endpoints])

  return nodes
}

export async function requestExit(baseUrl: string) {
  const response = await fetch(`${baseUrl}/v1/admin/exit`, { method: 'POST' })
  if (!response.ok) {
    throw new Error(`exit failed (${response.status})`)
  }
}

const OPS_URL = (import.meta.env.VITE_OPS_URL as string | undefined)?.replace(/\/$/, '') || 'http://127.0.0.1:8090'

export async function requestStart(nodeId: string) {
  const response = await fetch(`${OPS_URL}/v1/admin/nodes/${encodeURIComponent(nodeId)}/start`, {
    method: 'POST',
  })
  if (!response.ok) {
    const body = await response.text()
    throw new Error(body || `restart failed (${response.status})`)
  }
}
