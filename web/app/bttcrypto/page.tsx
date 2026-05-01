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

type ChartPoint = {
  x: number
  equity: number
  profit_money: number
  profit_pct: number
}

function n(v: unknown): number {
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
  const summary = dashboard?.summary || data?.summary || {}
  const performanceChart: ChartPoint[] = (summary?.chart || dashboard?.equity_curve || []) as ChartPoint[]

  const closedOps = Number(summary?.wins || 0) + Number(summary?.losses || 0)
  const winRate = closedOps > 0 ? (Number(summary?.wins || 0) / closedOps) * 100 : 0
  const lossRate = closedOps > 0 ? (Number(summary?.losses || 0) / closedOps) * 100 : 0
  const avgPnlPerClosed = closedOps > 0 ? Number(summary?.profit_money || 0) / closedOps : 0
  const avgPctPerClosed = closedOps > 0 ? Number(summary?.profit_pct || 0) / closedOps : 0
  const peakEquity = Number(overview?.peak_equity || 0)
  const currentCash = Number(overview?.cash || 0)
  const capitalEfficiency = peakEquity > 0 ? (currentCash / peakEquity) * 100 : 0

  const chartStats = useMemo(() => {
    const bestMoney = performanceChart.length
      ? Math.max(...performanceChart.map((p: ChartPoint) => n(p.profit_money)))
      : 0
    const worstMoney = performanceChart.length
      ? Math.min(...performanceChart.map((p: ChartPoint) => n(p.profit_money)))
      : 0
    const bestPct = performanceChart.length
      ? Math.max(...performanceChart.map((p: ChartPoint) => n(p.profit_pct)))
      : 0
    const worstPct = performanceChart.length
      ? Math.min(...performanceChart.map((p: ChartPoint) => n(p.profit_pct)))
      : 0

    return { bestMoney, worstMoney, bestPct, worstPct }
  }, [performanceChart])

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">BTTcrypto</h1>
          <p className="section-sub">
            Vista pubblica aggregata senza esposizione di token, nomi asset o numeri asset-specifici.
          </p>
        </div>
        <span className="pill">{data?.process?.running ? 'Attivo' : 'Non attivo'}</span>
      </div>

      {err ? <div className="bad">{err}</div> : null}

      <div className="card">
        <h2 className="section-title">Base temporale dei risultati</h2>
        <p className="section-sub">
          I risultati pubblici mostrati in questa sezione partono dal <strong>29 aprile 2016 alle 9.30 p.m.</strong>
        </p>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Cash disponibile</span>
          <strong>${currentCash.toFixed(2)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Capitale iniziale</span>
          <strong>${n(summary?.start_equity || overview?.start_equity).toFixed(2)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Operazioni chiuse</span>
          <strong>{closedOps}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Peak equity</span>
          <strong>${peakEquity.toFixed(2)}</strong>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Performance BTTcrypto</h2>

        <div className="kpi-grid">
          <div className="kpi">
            <span className="muted">Profitto / Perdita totale</span>
            <strong>{moneySigned(n(summary?.profit_money))}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Profitto / Perdita ultima operazione</span>
            <strong>{moneySigned(n(summary?.last_profit_money))}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Rendimento totale</span>
            <strong>{pctSigned(n(summary?.profit_pct))}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Rendimento ultima operazione</span>
            <strong>{pctSigned(n(summary?.last_profit_pct))}</strong>
          </div>
        </div>

        <div className="kpi-grid" style={{ marginTop: 14 }}>
          <div className="kpi">
            <span className="muted">Win rate</span>
            <strong>{pctSigned(winRate)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Loss rate</span>
            <strong>{pctSigned(lossRate)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Drawdown</span>
            <strong>{pctSigned(n(overview?.drawdown_pct) * 100)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Capital efficiency</span>
            <strong>{pctSigned(capitalEfficiency)}</strong>
          </div>
        </div>

        <div className="kpi-grid" style={{ marginTop: 14 }}>
          <div className="kpi">
            <span className="muted">Media P/L per operazione chiusa</span>
            <strong>{moneySigned(avgPnlPerClosed)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Media rendimento per operazione chiusa</span>
            <strong>{pctSigned(avgPctPerClosed)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Miglior valore aggregato</span>
            <strong>{moneySigned(chartStats.bestMoney)}</strong>
          </div>

          <div className="kpi">
            <span className="muted">Peggior valore aggregato</span>
            <strong>{moneySigned(chartStats.worstMoney)}</strong>
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
          <h2 className="section-title">Copertura pubblica del motore</h2>
          <div className="stack muted">
            <span>Universe monitorato: {Number(overview?.watchlist_count || 0)} elementi aggregati</span>
            <span>Posizioni correnti aggregate: {Number(overview?.positions_count || 0)}</span>
            <span>Snapshot disponibili: {Number(overview?.snapshots_count || 0)}</span>
            <span>Entry status pubblico: {overview?.entry_last ? 'Disponibile' : 'Non disponibile'}</span>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Policy di visualizzazione</h2>
          <div className="stack muted">
            <span>Nessun token mostrato pubblicamente.</span>
            <span>Nessun valore per singolo asset mostrato pubblicamente.</span>
            <span>Output limitato a metriche aggregate, percentuali, andamento e rischio.</span>
            <span>Logica proprietaria e dettagli operativi mantenuti privati.</span>
          </div>
        </div>
      </div>
    </div>
  )
}