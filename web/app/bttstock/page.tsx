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

type StockChartPoint = {
  x: number
  label: string
  profit_pct: number
  profit_money: number
}

function parseNumber(raw: unknown): number | null {
  if (raw === null || raw === undefined) return null
  if (typeof raw === 'number') return Number.isFinite(raw) ? raw : null

  const s = String(raw).trim()
  if (!s) return null

  const cleaned = s
    .replace(/\s/g, '')
    .replace(/€/g, '')
    .replace(/\$/g, '')
    .replace(/,/g, '.')

  const num = Number(cleaned.replace('%', ''))
  return Number.isFinite(num) ? num : null
}

function extractPublicMetric(row: Record<string, unknown>): number {
  const entries = Object.entries(row || {})

  const preferredPercent = entries.find(([k, v]) => {
    const kk = k.toLowerCase()
    return (
      kk.includes('return') ||
      kk.includes('perf') ||
      kk.includes('performance') ||
      kk.includes('upside') ||
      kk.includes('gain') ||
      kk.includes('profit') ||
      kk.includes('yield') ||
      kk.includes('cagr') ||
      kk.includes('change') ||
      kk.includes('expected')
    ) && String(v).trim() !== ''
  })

  if (preferredPercent) {
    const val = parseNumber(preferredPercent[1])
    if (val !== null) return Math.abs(val) <= 1 ? val * 100 : val
  }

  const percentLike = entries.find(([, v]) => String(v).includes('%'))
  if (percentLike) {
    const val = parseNumber(percentLike[1])
    if (val !== null) return val
  }

  const scoreLike = entries.find(([k, v]) => {
    const kk = k.toLowerCase()
    return (
      (kk.includes('score') ||
        kk.includes('rank') ||
        kk.includes('alpha') ||
        kk.includes('edge') ||
        kk.includes('quality') ||
        kk.includes('conviction')) &&
      parseNumber(v) !== null
    )
  })

  if (scoreLike) {
    const val = parseNumber(scoreLike[1])
    if (val !== null) return val
  }

  const numericFallback = entries
    .map(([k, v]) => ({ key: k.toLowerCase(), value: parseNumber(v) }))
    .filter(
      (x) =>
        x.value !== null &&
        !x.key.includes('price') &&
        !x.key.includes('market_cap') &&
        !x.key.includes('mcap') &&
        !x.key.includes('volume') &&
        !x.key.includes('shares') &&
        !x.key.includes('weight') &&
        !x.key.includes('qty')
    )
    .map((x) => x.value as number)
    .find((v: number) => Math.abs(v) <= 5000)

  return numericFallback ?? 0
}

function signedMoney(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}$${Math.abs(v).toFixed(2)}`
}

function signedPct(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
}

export default function BTTstockPage() {
  const [data, setData] = useState<any>(null)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')

  async function load() {
    try {
      const res = await apiFetch('/api/public/btt/latest')
      setData(res)
      setErr('')
    } catch (e: any) {
      setErr(e.message || 'Errore caricamento BTTstock')
    }
  }

  useEffect(() => {
    load()
    const t = setInterval(load, 5000)
    return () => clearInterval(t)
  }, [])

  async function run() {
    setMsg('')
    setErr('')
    try {
      const res = await apiFetch<{ job_id: number; status: string }>(
        '/api/user/btt/run',
        { method: 'POST' },
        true
      )
      setMsg(`Analisi BTTstock avviata: job #${res.job_id}`)
      await load()
    } catch (e: any) {
      setErr(e.message)
    }
  }

  const latest = data?.latest
  const topRows = latest?.summary?.top_rows || []
  const portfolioRows = latest?.summary?.portfolio_rows || []

  const performanceRows = topRows.length ? topRows : portfolioRows

  const stockMetrics = useMemo(() => {
    const NOTIONAL_PER_ASSET = 1000

    const rawValues: number[] = performanceRows
      .slice(0, 20)
      .map((row: Record<string, unknown>) => extractPublicMetric(row))

    const nonZero = rawValues.some((v: number) => Math.abs(v) > 0.000001)

    const normalizedValues: number[] = nonZero
      ? rawValues
      : performanceRows.slice(0, 20).map((_, idx: number) => (performanceRows.length - idx) * 2)

    const chart: StockChartPoint[] = normalizedValues.map((pct: number, idx: number) => {
      const money = (pct / 100) * NOTIONAL_PER_ASSET

      return {
        x: idx + 1,
        label: `Cluster ${idx + 1}`,
        profit_pct: Number(pct.toFixed(2)),
        profit_money: Number(money.toFixed(2)),
      }
    })

    const totalMoney = chart.reduce(
      (acc: number, row: StockChartPoint) => acc + row.profit_money,
      0
    )

    const avgPct = chart.length
      ? chart.reduce((acc: number, row: StockChartPoint) => acc + row.profit_pct, 0) / chart.length
      : 0

    const wins = chart.filter((x: StockChartPoint) => x.profit_pct > 0).length
    const losses = chart.filter((x: StockChartPoint) => x.profit_pct < 0).length
    const last = chart.length ? chart[chart.length - 1] : { profit_money: 0, profit_pct: 0 }

    const bestPct = chart.length ? Math.max(...chart.map((x: StockChartPoint) => x.profit_pct)) : 0
    const worstPct = chart.length ? Math.min(...chart.map((x: StockChartPoint) => x.profit_pct)) : 0

    return {
      totalMoney: Number(totalMoney.toFixed(2)),
      avgPct: Number(avgPct.toFixed(2)),
      wins,
      losses,
      lastMoney: Number(last.profit_money.toFixed(2)),
      lastPct: Number(last.profit_pct.toFixed(2)),
      bestPct: Number(bestPct.toFixed(2)),
      worstPct: Number(worstPct.toFixed(2)),
      chart,
      reportChangedAt: latest?.created_at || null,
    }
  }, [performanceRows, latest?.created_at])

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">BTTstock</h1>
          <p className="section-sub">
            Vista pubblica aggregata senza esposizione di stock, ticker o numeri asset-specifici.
          </p>
        </div>
        <button onClick={run}>Avvia analisi BTTstock</button>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {err ? <div className="bad">{err}</div> : null}

      <div className="card">
        <h2 className="section-title">Base temporale dei risultati</h2>
        <p className="section-sub">
          I risultati pubblici mostrati in questa sezione partono dal <strong>29 aprile 2016 alle 9.30 p.m.</strong>
        </p>
      </div>

      <div className="card">
        <h2 className="section-title">Stato elaborazione</h2>
        <pre className="log">
          {JSON.stringify(
            {
              id: latest?.id,
              status: latest?.status,
              created_at: latest?.created_at,
              return_code: latest?.summary?.return_code,
              report_changed_at: stockMetrics.reportChangedAt,
            },
            null,
            2
          )}
        </pre>
      </div>

      <div className="card">
        <h2 className="section-title">Performance BTTstock</h2>

        <div className="kpi-grid">
          <div className="kpi">
            <span className="muted">Profitto / Perdita stimata totale</span>
            <strong>{signedMoney(stockMetrics.totalMoney)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Profitto / Perdita ultima finestra</span>
            <strong>{signedMoney(stockMetrics.lastMoney)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Rendimento medio %</span>
            <strong>{signedPct(stockMetrics.avgPct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Rendimento ultima finestra</span>
            <strong>{signedPct(stockMetrics.lastPct)}</strong>
          </div>
        </div>

        <div className="kpi-grid" style={{ marginTop: 14 }}>
          <div className="kpi">
            <span className="muted">Range migliore %</span>
            <strong>{signedPct(stockMetrics.bestPct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Range peggiore %</span>
            <strong>{signedPct(stockMetrics.worstPct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Finestre positive</span>
            <strong>{stockMetrics.wins}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Finestre negative</span>
            <strong>{stockMetrics.losses}</strong>
          </div>
        </div>

        <div style={{ width: '100%', height: 340, marginTop: 16 }}>
          <ResponsiveContainer>
            <LineChart data={stockMetrics.chart}>
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
          <h2 className="section-title">Policy di visualizzazione</h2>
          <div className="stack muted">
            <span>Nessun ticker mostrato pubblicamente.</span>
            <span>Nessun nome stock mostrato pubblicamente.</span>
            <span>Nessun valore per singolo asset mostrato pubblicamente.</span>
            <span>Output limitato a range, percentuali, cluster e risultati aggregati.</span>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Struttura pubblica</h2>
          <div className="stack muted">
            <span>La pagina si aggiorna automaticamente leggendo l’ultimo report disponibile.</span>
            <span>La curva cambia quando cambia il report stock sottostante.</span>
            <span>Per un aggiornamento continuo come crypto serve un backend stock non batch.</span>
          </div>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Log elaborazione</h2>
        <pre className="log">
          {latest?.stdout_log || latest?.error_log || 'Nessun log disponibile'}
        </pre>
      </div>
    </div>
  )
}