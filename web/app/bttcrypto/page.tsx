'use client'

import { useEffect, useMemo, useState } from 'react'
import { apiFetch } from '@/lib/api'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
} from 'recharts'

function pct(v: number) {
  return `${Number(v || 0).toFixed(2)}%`
}

export default function BTTcryptoPage() {
  const [data, setData] = useState<any>(null)
  const [err, setErr] = useState('')

  async function load() {
    try {
      const res = await apiFetch('/api/public/microcap')
      setData(res)
      setErr('')
    } catch (e: any) {
      setErr(e.message || 'Errore caricamento BTTcrypto')
    }
  }

  useEffect(() => {
    load()
    const t = setInterval(load, 3000)
    return () => clearInterval(t)
  }, [])

  const dashboard = data?.dashboard || {}
  const overview = dashboard?.overview || {}
  const trades = dashboard?.trades || []
  const positions = dashboard?.positions || []
  const watchlist = dashboard?.top_candidates || []
  const summary = data?.summary || {}

  const closedTrades = useMemo(() => {
    return trades.filter((x: any) =>
      ['sell', 'exit'].includes(String(x?.side || '').toLowerCase())
    )
  }, [trades])

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">BTTcrypto</h1>
          <p className="section-sub">
            Analisi crypto osservabile, andamento operativo e risultati aggregati.
          </p>
        </div>
        <span className="pill">{data?.process?.running ? 'Attivo' : 'Non attivo'}</span>
      </div>

      {err ? <div className="bad">{err}</div> : null}

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Cash disponibile</span>
          <strong>${overview?.cash?.toFixed?.(2) || '0.00'}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento stimato</span>
          <strong>{pct(summary?.profit_pct || 0)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Operazioni chiuse</span>
          <strong>{closedTrades.length}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Posizioni aperte</span>
          <strong>{positions.length}</strong>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Performance BTTcrypto</h2>

        <div className="kpi-grid">
          <div className="kpi">
            <span className="muted">Profitto / Perdita</span>
            <strong>${summary?.profit_money?.toFixed?.(2) || '0.00'}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Rendimento %</span>
            <strong>{summary?.profit_pct?.toFixed?.(2) || '0.00'}%</strong>
          </div>
          <div className="kpi">
            <span className="muted">Operazioni positive</span>
            <strong>{summary?.wins ?? 0}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Operazioni negative</span>
            <strong>{summary?.losses ?? 0}</strong>
          </div>
        </div>

        <div style={{ width: '100%', height: 320, marginTop: 16 }}>
          <ResponsiveContainer>
            <LineChart data={summary?.chart || []}>
              <XAxis dataKey="x" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="profit_money" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="profit_pct" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Andamento BTTcrypto</h2>
        <div style={{ width: '100%', height: 300 }}>
          <ResponsiveContainer>
            <LineChart data={dashboard?.equity_curve || []}>
              <XAxis dataKey="ts" hide />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="equity" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h2 className="section-title">Posizioni in corso</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Asset</th>
                  <th>Chain</th>
                  <th>Entry</th>
                  <th>Media</th>
                  <th>Massimo</th>
                  <th>Stato</th>
                </tr>
              </thead>
              <tbody>
                {positions.length === 0 ? (
                  <tr>
                    <td colSpan={6}>Nessuna posizione aperta</td>
                  </tr>
                ) : (
                  positions.map((row: any) => {
                    const entry = Number(row?.entry_px || 0)
                    const peak = Number(row?.peak_px || 0)
                    const gain = entry > 0 ? ((peak - entry) / entry) * 100 : 0

                    return (
                      <tr key={row.key}>
                        <td>{row.token}</td>
                        <td>{row.chain}</td>
                        <td>{row.entry_px}</td>
                        <td>{row.avg_px}</td>
                        <td>{row.peak_px}</td>
                        <td>{gain.toFixed(2)}%</td>
                      </tr>
                    )
                  })
                )}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Opportunità monitorate</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Asset</th>
                  <th>Chain</th>
                  <th>Score</th>
                  <th>Prezzo</th>
                  <th>Liquidità</th>
                  <th>Volume 5m</th>
                </tr>
              </thead>
              <tbody>
                {watchlist.length === 0 ? (
                  <tr>
                    <td colSpan={6}>Nessuna opportunità disponibile</td>
                  </tr>
                ) : (
                  watchlist.map((row: any) => (
                    <tr key={row.key}>
                      <td>{row.token}</td>
                      <td>{row.chain}</td>
                      <td>{row.score ?? '-'}</td>
                      <td>{row.price_usd ?? '-'}</td>
                      <td>{row.liq_usd ?? '-'}</td>
                      <td>{row.vol_m5 ?? '-'}</td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Storico operativo</h2>
        <p className="section-sub">
          Operazioni in profitto o perdita visualizzate nel tempo.
        </p>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Data</th>
                <th>Tipo</th>
                <th>Asset</th>
                <th>Chain</th>
                <th>Valore</th>
                <th>Motivo</th>
              </tr>
            </thead>
            <tbody>
              {trades.length === 0 ? (
                <tr>
                  <td colSpan={6}>Nessuna operazione disponibile</td>
                </tr>
              ) : (
                trades.map((row: any, idx: number) => (
                  <tr key={`${row.ts}-${idx}`}>
                    <td>{row.ts ? new Date(row.ts * 1000).toLocaleString() : '-'}</td>
                    <td>{row.side}</td>
                    <td>{row.token}</td>
                    <td>{row.chain}</td>
                    <td>{row.usd_value}</td>
                    <td>{row.reason}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}