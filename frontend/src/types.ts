export type Member = {
  id: string
  url: string
}

export type MetricsEvent = {
  node_id: string
  time: string
  events_accepted: number
  events_forwarded: number
  rows_flushed: number
  quantity_flushed: number
  events_per_sec: number
  pending_keys: number
  totals_keys: number
  events_per_row?: number
  members: Member[]
  last_flush_at?: string
  last_flush_rows?: number
  last_flush_quantity?: number
}

export type NodeEndpoint = {
  id: string
  baseUrl: string
}

export type NodeLiveState = {
  endpoint: NodeEndpoint
  connected: boolean
  metrics: MetricsEvent | null
  history: number[]
  error: string | null
}
