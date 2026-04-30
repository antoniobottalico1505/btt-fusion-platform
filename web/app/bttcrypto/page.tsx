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

function n(v: any): number {
  const x = Number(v)
  return Number.isFinite(x) ? x : 0
}

function moneySigned(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}$${Math.abs(v).toFixed(2)}`
}

function pctSigned(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
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
  const equityCurve = dashboard?.equity_curve || []

  const performanceChart = useMemo(() => {
    if (!equityCurve.length) return []

    const firstEquity = n(equityCurve[0]?.equity)

    return equityCurve.map((row: any, idx: number) => {
      const eq = n(row?.equity)
      const profitMoney = eq - firstEquity
      const profitPct = firstEquity > 0 ? (profitMoney / firstEquity) * 100 : 0

      return {
        x: idx + 1,
        equity: eq,
        profit_money: Number(profitMoney.toFixed(2)),
        profit_pct: Number(profitPct.toFixed(2)),
      }
    })
  }, [equityCurve])

  const totalProfitMoney = useMemo(() => {
    if (!performanceChart.length) return 0
    return n(performanceChart[performanceChart.length - 1]?.profit_money)
  }, [performanceChart])

  const totalProfitPct = useMemo(() => {
    if (!performanceChart.length) return 0
    return n(performanceChart[performanceChart.length - 1]?.profit_pct)
  }, [performanceChart])

  const lastProfitMoney = useMemo(() => {
    if (equityCurve.length >= 2) {
      const lastEq = n(equityCurve[equityCurve.length - 1]?.equity)
      const prevEq = n(equityCurve[equityCurve.length - 2]?.equity)
      return Number((lastEq - prevEq).toFixed(2))
    }
    if (performanceChart.length === 1) {
      return n(performanceChart[0]?.profit_money)
    }
    return 0
  }, [equityCurve, performanceChart])

  const lastProfitPct = useMemo(() => {
    if (equityCurve.length >= 2) {
      const lastEq = n(equityCurve[equityCurve.length - 1]?.equity)
      const prevEq = n(equityCurve[equityCurve.length - 2]?.equity)
      return prevEq > 0 ? Number((((lastEq - prevEq) / prevEq) * 100).toFixed(2)) : 0
    }
    if (performanceChart.length === 1) {
      return n(performanceChart[0]?.profit_pct)
    }
    return 0
  }, [equityCurve, performanceChart])

  const tradeStats = useMemo(() => {
    if (equityCurve.length < 2) {
      return { wins: 0, losses: 0 }
    }

    let wins = 0
    let losses = 0

    for (let i = 1; i < equityCurve.length; i++) {
      const curr = n(equityCurve[i]?.equity)
      const prev = n(equityCurve[i - 1]?.equity)
      const diff = curr - prev
      if (diff > 0) wins += 1
      if (diff < 0) losses += 1
    }

    return { wins, losses }
  }, [equityCurve])

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">BTTcrypto</h1>
          <p className="section-sub">
            Quadro operativo crypto con andamento, risultato totale e impatto dell’ultima operazione.
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
          <span className="muted">Posizioni aperte</span>
          <strong>{positions.length}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Operazioni positive</span>
          <strong>{tradeStats.wins}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Operazioni negative</span>
          <strong>{tradeStats.losses}</strong>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Performance BTTcrypto</h2>

        <div className="kpi-grid">
          <div className="kpi">
            <span className="muted">Profitto / Perdita totale</span>
            <strong>{moneySigned(totalProfitMoney)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Profitto / Perdita ultima operazione</span>
            <strong>{moneySigned(lastProfitMoney)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Rendimento stimato totale</span>
            <strong>{pctSigned(totalProfitPct)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Rendimento ultima operazione</span>
            <strong>{pctSigned(lastProfitPct)}</strong>
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
                  <th>Variazione</th>
                </tr>
              </thead>
              <tbody>
                {positions.length === 0 ? (
                  <tr>
                    <td colSpan={6}>Nessuna posizione aperta</td>
                  </tr>
                ) : (
                  positions.map((row: any) => {
                    const entry = n(row?.entry_px)
                    const peak = n(row?.peak_px)
                    const deltaPct = entry > 0 ? ((peak - entry) / entry) * 100 : 0

                    return (
                      <tr key={row.key}>
                        <td>{row.token}</td>
                        <td>{row.chain}</td>
                        <td>{row.entry_px}</td>
                        <td>{row.avg_px}</td>
                        <td>{row.peak_px}</td>
                        <td>{pctSigned(deltaPct)}</td>
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
          Elenco delle operazioni registrate, con dettaglio dell’asset e della motivazione.
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