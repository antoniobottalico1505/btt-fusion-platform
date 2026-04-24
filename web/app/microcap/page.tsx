'use client'

import { useEffect, useState } from 'react'
import { EquityChart } from '@/components/Charts'
import { apiFetch } from '@/lib/api'

export default function MicrocapPage() {
  const [data, setData] = useState<any>(null)
  const [err, setErr] = useState('')

  async function load() {
    try {
      const res = await apiFetch('/api/public/microcap')
      setData(res)
      setErr('')
    } catch (e: any) {
      setErr(e.message || 'Errore caricamento microcap')
    }
  }

  useEffect(() => {
    load()
    const t = setInterval(load, 6000)
    return () => clearInterval(t)
  }, [])

  const process = data?.process || {}
  const overview = data?.dashboard?.overview
  const candidates = data?.dashboard?.top_candidates || []
  const trades = data?.dashboard?.trades || []
  const positions = data?.dashboard?.positions || []

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">Microcap Bot</h1>
          <p className="section-sub">Demo paper osservabile. Parametri e config rimangono lato server.</p>
        </div>
        <span className="pill">process {process.running ? 'running' : 'stopped'}</span>
      </div>

      {err ? <div className="bad">{err}</div> : null}

      {!process.running ? (
        <div className="card stack">
          <h2 className="section-title">Stato processo</h2>
          <pre className="log">{JSON.stringify({
            running: process.running ?? false,
            pid: process.pid ?? null,
            exit_code: process.exit_code ?? null,
            last_error: process.last_error || '',
            mode: process.mode || '',
            uptime_sec: process.uptime_sec || 0,
          }, null, 2)}</pre>
          <h3 className="section-title">Log microcap</h3>
          <pre className="log">{process.log_tail || 'Nessun log disponibile'}</pre>
        </div>
      ) : null}

      <div className="kpi-grid">
        <div className="kpi"><span className="muted">Cash</span><strong>${overview?.cash?.toFixed?.(2) || '0.00'}</strong></div>
        <div className="kpi"><span className="muted">Drawdown</span><strong>{((overview?.drawdown_pct || 0) * 100).toFixed(2)}%</strong></div>
        <div className="kpi"><span className="muted">Trades</span><strong>{overview?.trades_count ?? 0}</strong></div>
        <div className="kpi"><span className="muted">Positions</span><strong>{overview?.positions_count ?? 0}</strong></div>
      </div>

      <div className="card">
        <h2 className="section-title">Equity curve</h2>
        <EquityChart data={data?.dashboard?.equity_curve || []} />
      </div>

      <div className="grid-2">
        <div className="card">
          <h2 className="section-title">Top candidates</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr><th>Token</th><th>Chain</th><th>Score</th><th>Price</th><th>Liq</th><th>Vol5m</th></tr>
              </thead>
              <tbody>
                {candidates.length === 0 ? (
                  <tr><td colSpan={6}>Nessun candidato disponibile</td></tr>
                ) : candidates.map((row: any) => (
                  <tr key={row.key}>
                    <td>{row.token}</td>
                    <td>{row.chain}</td>
                    <td>{row.score ?? '-'}</td>
                    <td>{row.price_usd ?? '-'}</td>
                    <td>{row.liq_usd ?? '-'}</td>
                    <td>{row.vol_m5 ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Posizioni</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr><th>Token</th><th>Chain</th><th>Entry</th><th>Avg</th><th>Peak</th><th>Trail stop</th></tr>
              </thead>
              <tbody>
                {positions.length === 0 ? (
                  <tr><td colSpan={6}>Nessuna posizione aperta</td></tr>
                ) : positions.map((row: any) => (
                  <tr key={row.key}>
                    <td>{row.token}</td>
                    <td>{row.chain}</td>
                    <td>{row.entry_px}</td>
                    <td>{row.avg_px}</td>
                    <td>{row.peak_px}</td>
                    <td>{row.trail_stop_px}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Trades</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr><th>Time</th><th>Side</th><th>Chain</th><th>Token</th><th>USD</th><th>Reason</th></tr>
            </thead>
            <tbody>
              {trades.length === 0 ? (
                <tr><td colSpan={6}>Nessun trade disponibile</td></tr>
              ) : trades.map((row: any, idx: number) => (
                <tr key={`${row.ts}-${idx}`}>
                  <td>{row.ts ? new Date(row.ts * 1000).toLocaleString() : '-'}</td>
                  <td>{row.side}</td>
                  <td>{row.chain}</td>
                  <td>{row.token}</td>
                  <td>{row.usd_value}</td>
                  <td>{row.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}