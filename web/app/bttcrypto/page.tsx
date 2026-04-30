'use client'

import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
} from 'recharts'

function n(v: unknown): number {
  const x = Number(v)
  return Number.isFinite(x) ? x : 0
}

function moneySignedNullable(v: unknown): string {
  const num = Number(v)
  if (!Number.isFinite(num)) return '—'
  const sign = num >= 0 ? '+' : '-'
  return `${sign}$${Math.abs(num).toFixed(2)}`
}

function pctSignedNullable(v: unknown): string {
  const num = Number(v)
  if (!Number.isFinite(num)) return '—'
  const sign = num >= 0 ? '+' : '-'
  return `${sign}${Math.abs(num).toFixed(2)}%`
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
  const summary = dashboard?.summary || data?.summary || {}
  const performanceChart = summary?.chart || dashboard?.equity_curve || []

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">BTTcrypto</h1>
          <p className="section-sub">
            Quadro operativo crypto coerente con i valori reali inviati dal bot.
          </p>
        </div>
        <span className="pill">{data?.process?.running ? 'Attivo' : 'Non attivo'}</span>
      </div>

      {err ? <div className="bad">{err}</div> : null}

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Cash disponibile</span>
          <strong>${n(overview?.cash).toFixed(2)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Capitale iniziale</span>
          <strong>${n(summary?.start_equity || overview?.start_equity).toFixed(2)}</strong>
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

      <div className="card">
        <h2 className="section-title">Performance BTTcrypto</h2>

        <div className="kpi-grid">
          <div className="kpi">
            <span className="muted">Profitto / Perdita totale</span>
            <strong>{moneySignedNullable(summary?.profit_money)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Profitto / Perdita ultima operazione</span>
            <strong>{moneySignedNullable(summary?.last_profit_money)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Rendimento stimato totale</span>
            <strong>{pctSignedNullable(summary?.profit_pct)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Rendimento ultima operazione</span>
            <strong>{pctSignedNullable(summary?.last_profit_pct)}</strong>
          </div>
        </div>

        <div style={{ width: '100%', height: 340, marginTop: 16 }}>
          <ResponsiveContainer>
            <LineChart data={performanceChart}>
              <XAxis dataKey="x" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="profit_money" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="profit_pct" strokeWidth={2} dot={false} />
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
                  <th>Trail stop</th>
                </tr>
              </thead>
              <tbody>
                {positions.length === 0 ? (
                  <tr>
                    <td colSpan={6}>Nessuna posizione aperta</td>
                  </tr>
                ) : (
                  positions.map((row: any) => (
                    <tr key={row.key}>
                      <td>{row.token}</td>
                      <td>{row.chain}</td>
                      <td>{row.entry_px}</td>
                      <td>{row.avg_px}</td>
                      <td>{row.peak_px}</td>
                      <td>{row.trail_stop_px}</td>
                    </tr>
                  ))
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