import { useEffect, useMemo, useState } from 'react'
import { configuredNodes } from '../nodes'
import type { MetricsEvent, NodeLiveState } from '../types'

const HISTORY = 24

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

    endpoints.forEach((endpoint, index) => {
      const source = new EventSource(`${endpoint.baseUrl}/v1/metrics/stream`)
      sources.push(source)

      source.addEventListener('metrics', (message) => {
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
          setNodes((current) => {
            const next = [...current]
            next[index] = {
              ...(next[index] ?? emptyState(endpoint)),
              connected: false,
              error: error instanceof Error ? error.message : 'bad metrics payload',
            }
            return next
          })
        }
      })

      source.onerror = () => {
        setNodes((current) => {
          const next = [...current]
          next[index] = {
            ...(next[index] ?? emptyState(endpoint)),
            connected: false,
            error: 'stream disconnected',
          }
          return next
        })
      }
    })

    return () => {
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
