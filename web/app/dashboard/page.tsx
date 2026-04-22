'use client'

import { useEffect, useState } from 'react'
import { EquityChart, ScoreChart } from '@/components/Charts'
import { apiFetch } from '@/lib/api'

export default function DashboardPage() {
  const [microcap, setMicrocap] = useState<any>(null)
  const [btt, setBtt] = useState<any>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    apiFetch('/api/public/microcap').then(setMicrocap).catch((e) => setError(e.message))
    apiFetch('/api/public/btt/latest').then(setBtt).catch(() => null)
  }, [])

  const overview = microcap?.dashboard?.overview
  const latest = btt?.latest

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Dashboard unificata</h1>
        <p className="section-sub">Vista unica per demo paper Microcap e ultimi report BTT Capital.</p>
      </div>
      {error ? <div className="bad">{error}</div> : null}
      <div className="kpi-grid">
        <div className="kpi"><span className="muted">Cash</span><strong>${overview?.cash?.toFixed?.(2) || '0.00'}</strong></div>
        <div className="kpi"><span className="muted">Peak equity</span><strong>${overview?.peak_equity?.toFixed?.(2) || '0.00'}</strong></div>
        <div className="kpi"><span className="muted">Watchlist</span><strong>{overview?.watchlist_count ?? 0}</strong></div>
        <div className="kpi"><span className="muted">BTT latest</span><strong>{latest?.status || 'none'}</strong></div>
      </div>
      <div className="grid-2">
        <div className="card">
          <div className="row"><h2 className="section-title">Microcap equity view</h2><span className="pill">{microcap?.public_mode || 'paper'}</span></div>
          <EquityChart data={microcap?.dashboard?.equity_curve || []} />
        </div>
        <div className="card">
          <div className="row"><h2 className="section-title">Top signals</h2><span className="pill">watchlist</span></div>
          <ScoreChart data={microcap?.dashboard?.top_candidates || []} />
        </div>
      </div>
      <div className="grid-2">
        <div className="card">
          <h2 className="section-title">Entry last</h2>
          <pre className="log">{JSON.stringify(overview?.entry_last || {}, null, 2)}</pre>
        </div>
        <div className="card">
          <h2 className="section-title">Ultimo report BTT</h2>
          <pre className="log">{JSON.stringify(latest?.summary || {}, null, 2)}</pre>
        </div>
      </div>
    </div>
  )
}
