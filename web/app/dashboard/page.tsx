'use client'

import { useEffect, useMemo, useState } from 'react'
import { apiFetch } from '@/lib/api'
import {
  LineChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from 'recharts'

type ChartPoint = {
  x: number
  label?: string
  profit_pct: number
  profit_money: number
}

type PublicMetrics = {
  metric_key: string | null
  point_count: number
  avg_pct: number | null
  best_pct: number | null
  worst_pct: number | null
  last_pct: number | null
  positives: number
  negatives: number
  chart: ChartPoint[]
}

type CombinedPoint = {
  x: number
  crypto_profit_pct: number
  stock_profit_pct: number
  combined_profit_pct: number
}

function n(v: unknown): number {
  const x = Number(v)
  return Number.isFinite(x) ? x : 0
}

function pctSigned(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
}

function maybePct(v: unknown): string {
  const x = Number(v)
  if (!Number.isFinite(x)) return 'N/D'
  return pctSigned(x)
}

export default function DashboardPage() {
  const [crypto, setCrypto] = useState<any>(null)
  const [stock, setStock] = useState<any>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    const loadAll = () => {
      apiFetch('/api/public/microcap')
        .then(setCrypto)
        .catch((e: any) => setError(e.message || 'Errore caricamento BTTcrypto'))

      apiFetch('/api/public/btt/latest')
        .then(setStock)
        .catch(() => null)
    }

    loadAll()
    const t = setInterval(loadAll, 5000)
    return () => clearInterval(t)
  }, [])

  const cryptoSummary = crypto?.dashboard?.summary || crypto?.summary || {}
  const cryptoChart: ChartPoint[] = cryptoSummary?.chart || []

  const stockSummary = stock?.summary_metrics || {}
  const stockPublic: PublicMetrics = stockSummary?.public_metrics || {
    metric_key: null,
    point_count: 0,
    avg_pct: null,
    best_pct: null,
    worst_pct: null,
    last_pct: null,
    positives: 0,
    negatives: 0,
    chart: [],
  }

  const stockChart: ChartPoint[] = Array.isArray(stockPublic.chart) ? stockPublic.chart : []

  const combinedChart = useMemo<CombinedPoint[]>(() => {
    const maxLen = Math.max(cryptoChart.length, stockChart.length)

    return Array.from({ length: maxLen }).map((_, idx: number) => {
      const c = cryptoChart[idx]
      const s = stockChart[idx]

      const cryptoPct = c ? n(c.profit_pct) : 0
      const stockPct = s ? n(s.profit_pct) : 0

      return {
        x: idx + 1,
        crypto_profit_pct: cryptoPct,
        stock_profit_pct: stockPct,
        combined_profit_pct: Number((((cryptoPct + stockPct) / 2)).toFixed(2)),
      }
    })
  }, [cryptoChart, stockChart])

  const combinedNow =
    combinedChart.length > 0
      ? combinedChart[combinedChart.length - 1]
      : {
          x: 0,
          crypto_profit_pct: 0,
          stock_profit_pct: 0,
          combined_profit_pct: 0,
        }

  const spreadPct = n(combinedNow.crypto_profit_pct) - n(combinedNow.stock_profit_pct)

  const sectorLeader =
    n(combinedNow.crypto_profit_pct) > n(combinedNow.stock_profit_pct)
      ? 'BTTcrypto'
      : n(combinedNow.crypto_profit_pct) < n(combinedNow.stock_profit_pct)
        ? 'BTTstock'
        : 'Parità'

  const positiveCombinedPoints = combinedChart.filter(
    (p: CombinedPoint) => n(p.combined_profit_pct) > 0
  ).length

  const consistencyPct = combinedChart.length
    ? (positiveCombinedPoints / combinedChart.length) * 100
    : 0

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Dashboard BTTcapital</h1>
        <p className="section-sub">
          Vista media aggregata di BTTcrypto e BTTstock senza esposizione pubblica di token, stock, ticker o numeri asset-level.
        </p>
      </div>

      {error ? <div className="bad">{error}</div> : null}

      <div className="card">
        <h2 className="section-title">Base temporale dei risultati</h2>
        <p className="section-sub">
          I risultati pubblici mostrati in dashboard partono dal <strong>29 aprile 2016 alle 9.30 p.m.</strong>
        </p>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Media rendimento aggregato</span>
          <strong>{pctSigned(n(combinedNow.combined_profit_pct))}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Differenziale settori %</span>
          <strong>{pctSigned(spreadPct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Settore leader</span>
          <strong>{sectorLeader}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Consistency rate</span>
          <strong>{pctSigned(consistencyPct)}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Rendimento BTTcrypto</span>
          <strong>{maybePct(cryptoSummary?.profit_pct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento medio BTTstock</span>
          <strong>{maybePct(stockPublic?.avg_pct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Migliore % BTTstock</span>
          <strong>{maybePct(stockPublic?.best_pct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Peggiore % BTTstock</span>
          <strong>{maybePct(stockPublic?.worst_pct)}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Titoli stock rilevati</span>
          <strong>{stockPublic?.point_count ?? 0}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Titoli positivi</span>
          <strong>{stockPublic?.positives ?? 0}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Titoli negativi</span>
          <strong>{stockPublic?.negatives ?? 0}</strong>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Curva percentuale aggregata</h2>
        <div style={{ width: '100%', height: 360 }}>
          <ResponsiveContainer>
            <LineChart data={combinedChart}>
              <XAxis dataKey="x" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="crypto_profit_pct" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="stock_profit_pct" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="combined_profit_pct" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h2 className="section-title">Parametri avanzati</h2>
          <pre className="log">
            {JSON.stringify(
              {
                consistency_pct: Number(consistencyPct.toFixed(2)),
                positive_combined_points: positiveCombinedPoints,
                total_combined_points: combinedChart.length,
                sector_leader: sectorLeader,
                current_spread_pct: Number(spreadPct.toFixed(2)),
                stock_points: stockPublic?.point_count ?? 0,
                stock_metric_key: stockPublic?.metric_key ?? null,
              },
              null,
              2
            )}
          </pre>
        </div>

        <div className="card">
          <h2 className="section-title">Policy di esposizione pubblica</h2>
          <div className="stack muted">
            <span>Nessun nome token pubblico.</span>
            <span>Nessun nome stock pubblico.</span>
            <span>Nessuna tabella asset-level pubblica.</span>
            <span>Solo percentuali reali per titolo e sintesi aggregata.</span>
          </div>
        </div>
      </div>
    </div>
  )
}