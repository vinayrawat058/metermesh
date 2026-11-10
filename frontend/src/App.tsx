import { useMemo, useState } from 'react'
import { requestExit, requestStart, useClusterStreams } from './hooks/useClusterStreams'
import type { NodeLiveState } from './types'

function formatRate(value: number) {
  if (!Number.isFinite(value)) return '0.0'
  return value.toFixed(1)
}

function formatTime(value?: string) {
  if (!value) return '—'
  return new Date(value).toLocaleTimeString()
}

function Sparkline({ values }: { values: number[] }) {
  const max = Math.max(1, ...values)
  return (
    <div className="flex h-12 items-end gap-1" aria-hidden="true">
      {values.map((value, index) => (
        <span
          key={`${index}-${value}`}
          className="w-1.5 origin-bottom rounded-sm bg-[var(--accent)]/80"
          style={{
            height: `${Math.max(8, (value / max) * 100)}%`,
            animation: 'bar-grow 320ms ease-out',
          }}
        />
      ))}
    </div>
  )
}

function NodeRow({
  node,
  onExit,
  onRestart,
  exiting,
  restarting,
}: {
  node: NodeLiveState
  onExit: (baseUrl: string) => void
  onRestart: (nodeId: string) => void
  exiting: boolean
  restarting: boolean
}) {
  const metrics = node.metrics
  const live = node.connected && metrics !== null
  const nodeId = metrics?.node_id ?? node.endpoint.id

  return (
    <article
      className="grid gap-4 border-b border-[var(--line)] py-5 last:border-b-0 md:grid-cols-[minmax(0,1.1fr)_minmax(0,1.4fr)_auto]"
      style={{ animation: 'rise-in 420ms ease-out' }}
    >
      <div className="space-y-2">
        <div className="flex items-center gap-3">
          <span
            className="inline-block h-2.5 w-2.5 rounded-full"
            style={{
              background: live ? 'var(--live)' : 'var(--down)',
              animation: live ? 'pulse-dot 1.6s ease-in-out infinite' : undefined,
            }}
            aria-hidden="true"
          />
          <h2 className="text-2xl font-bold tracking-tight">{nodeId}</h2>
        </div>
        <p className="font-[family-name:var(--mono)] text-sm text-[var(--ink-soft)]">
          {node.endpoint.baseUrl}
        </p>
        <p className="text-sm text-[var(--ink-soft)]">
          {live ? 'streaming metrics' : node.error ?? 'waiting for heartbeat'}
        </p>
      </div>

      <div className="grid gap-3 sm:grid-cols-3">
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-soft)]">events/sec</p>
          <p className="mt-1 font-[family-name:var(--mono)] text-3xl font-medium">
            {formatRate(metrics?.events_per_sec ?? 0)}
          </p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-soft)]">accepted</p>
          <p className="mt-1 font-[family-name:var(--mono)] text-3xl font-medium">
            {metrics?.events_accepted ?? 0}
          </p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-soft)]">rows flushed</p>
          <p className="mt-1 font-[family-name:var(--mono)] text-3xl font-medium">
            {metrics?.rows_flushed ?? 0}
          </p>
        </div>
        <div className="sm:col-span-3">
          <Sparkline values={node.history.length ? node.history : [0]} />
          <p className="mt-2 font-[family-name:var(--mono)] text-xs text-[var(--ink-soft)]">
            last flush {formatTime(metrics?.last_flush_at)} · pending {metrics?.pending_keys ?? 0} ·
            forwarded {metrics?.events_forwarded ?? 0}
            {metrics?.events_per_row
              ? ` · ${metrics.events_per_row.toFixed(1)} events/row`
              : ''}
          </p>
        </div>
      </div>

      <div className="flex items-start justify-end gap-2">
        <button
          type="button"
          disabled={live || restarting}
          onClick={() => onRestart(nodeId)}
          className="border border-[var(--accent-deep)] bg-transparent px-4 py-2 font-[family-name:var(--mono)] text-sm text-[var(--accent-deep)] transition enabled:hover:-translate-y-0.5 enabled:hover:bg-[var(--accent)] enabled:hover:text-[var(--paper)] disabled:cursor-not-allowed disabled:opacity-40"
        >
          {restarting ? 'starting…' : 'restart'}
        </button>
        <button
          type="button"
          disabled={!live || exiting}
          onClick={() => onExit(node.endpoint.baseUrl)}
          className="border border-[var(--ink)] bg-[var(--ink)] px-4 py-2 font-[family-name:var(--mono)] text-sm text-[var(--paper)] transition enabled:hover:-translate-y-0.5 enabled:hover:bg-[var(--accent-deep)] disabled:cursor-not-allowed disabled:opacity-40"
        >
          {exiting ? 'stopping…' : 'kill node'}
        </button>
      </div>
    </article>
  )
}

export default function App() {
  const nodes = useClusterStreams()
  const [exitingUrl, setExitingUrl] = useState<string | null>(null)
  const [restartingId, setRestartingId] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)

  const liveCount = nodes.filter((node) => node.connected).length
  const totalRate = nodes.reduce((sum, node) => sum + (node.metrics?.events_per_sec ?? 0), 0)
  const members = useMemo(() => {
    const seen = new Map<string, string>()
    for (const node of nodes) {
      for (const member of node.metrics?.members ?? []) {
        seen.set(member.id, member.url)
      }
    }
    return [...seen.entries()].sort(([a], [b]) => a.localeCompare(b))
  }, [nodes])

  async function handleExit(baseUrl: string) {
    setActionError(null)
    setExitingUrl(baseUrl)
    try {
      await requestExit(baseUrl)
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'exit failed')
    } finally {
      setExitingUrl(null)
    }
  }

  async function handleRestart(nodeId: string) {
    setActionError(null)
    setRestartingId(nodeId)
    try {
      await requestStart(nodeId)
    } catch (error) {
      setActionError(error instanceof Error ? error.message : 'restart failed')
    } finally {
      setRestartingId(null)
    }
  }

  return (
    <main className="relative mx-auto min-h-screen max-w-6xl px-5 pb-16 pt-10 sm:px-8">
      <header className="mb-10 space-y-4" style={{ animation: 'rise-in 500ms ease-out' }}>
        <p className="font-[family-name:var(--mono)] text-xs uppercase tracking-[0.28em] text-[var(--accent-deep)]">
          usage ingest mesh
        </p>
        <h1 className="max-w-3xl text-5xl font-bold tracking-tight text-[var(--ink)] sm:text-6xl">
          MeterMesh
        </h1>
        <p className="max-w-2xl text-lg text-[var(--ink-soft)]">
          Live view of which nodes are up, how fast they are accepting usage, and when they last
          flushed into Postgres. Kill a node to watch ownership move, then restart it from here.
        </p>
      </header>

      <section
        className="mb-10 grid gap-6 border-y border-[var(--line)] py-6 sm:grid-cols-3"
        style={{ animation: 'rise-in 560ms ease-out' }}
      >
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-soft)]">live nodes</p>
          <p className="mt-2 font-[family-name:var(--mono)] text-4xl">{liveCount}</p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-soft)]">cluster events/sec</p>
          <p className="mt-2 font-[family-name:var(--mono)] text-4xl">{formatRate(totalRate)}</p>
        </div>
        <div>
          <p className="text-xs uppercase tracking-[0.18em] text-[var(--ink-soft)]">membership</p>
          <p className="mt-2 font-[family-name:var(--mono)] text-sm leading-7 text-[var(--ink-soft)]">
            {members.length
              ? members.map(([id, url]) => `${id} → ${url}`).join('\n')
              : 'waiting for Redis heartbeats'}
          </p>
        </div>
      </section>

      {actionError ? (
        <p className="mb-4 font-[family-name:var(--mono)] text-sm text-[var(--warn)]">{actionError}</p>
      ) : null}

      <section aria-label="Cluster nodes">
        {nodes.map((node) => (
          <NodeRow
            key={node.endpoint.baseUrl}
            node={node}
            onExit={handleExit}
            onRestart={handleRestart}
            exiting={exitingUrl === node.endpoint.baseUrl}
            restarting={restartingId === (node.metrics?.node_id ?? node.endpoint.id)}
          />
        ))}
      </section>
    </main>
  )
}
