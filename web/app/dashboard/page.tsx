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

type PublicRow = Record<string, unknown>

type StockPoint = {
  x: number
  label: string
  profit_money: number
  profit_pct: number
}

type CombinedPoint = {
  x: number
  crypto_profit_money: number
  crypto_profit_pct: number
  stock_profit_money: number
  stock_profit_pct: number
  combined_profit_money: number
  combined_profit_pct: number
}

type MetricColumnPick = {
  key: string | null
  values: number[]
}

function n(v: unknown): number {
  const x = Number(v)
  return Number.isFinite(x) ? x : 0
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

function normalizePct(values: number[]): number[] {
  if (!values.length) return values

  const absMax = Math.max(...values.map((v: number) => Math.abs(v)))

  if (absMax <= 3) {
    return values.map((v: number) => v * 100)
  }

  return values
}

function isForbiddenMetricKey(key: string): boolean {
  const k = key.toLowerCase()

  return (
    k.includes('ticker') ||
    k.includes('symbol') ||
    k.includes('name') ||
    k.includes('isin') ||
    k.includes('country') ||
    k.includes('exchange') ||
    k.includes('sector') ||
    k.includes('industry') ||
    k.includes('currency') ||
    k.includes('market_cap') ||
    k.includes('mcap') ||
    k.includes('price') ||
    k.includes('close') ||
    k.includes('open') ||
    k.includes('high') ||
    k.includes('low') ||
    k.includes('volume') ||
    k.includes('shares') ||
    k.includes('qty') ||
    k.includes('weight') ||
    k.includes('rank') ||
    k === 'id'
  )
}

function metricKeyScore(key: string): number {
  const k = key.toLowerCase()

  if (
    k.includes('return') ||
    k.includes('perf') ||
    k.includes('performance') ||
    k.includes('upside') ||
    k.includes('gain') ||
    k.includes('profit') ||
    k.includes('yield') ||
    k.includes('cagr') ||
    k.includes('expected')
  ) {
    return 100
  }

  if (
    k.includes('score') ||
    k.includes('alpha') ||
    k.includes('edge') ||
    k.includes('quality') ||
    k.includes('conviction')
  ) {
    return 50
  }

  return 0
}

function chooseRealMetricColumn(rows: PublicRow[]): MetricColumnPick {
  if (!rows.length) return { key: null, values: [] }

  const keys = Array.from(
    new Set(rows.flatMap((row: PublicRow) => Object.keys(row || {})))
  )

  let bestKey: string | null = null
  let bestValues: number[] = []
  let bestScore = -Infinity

  for (const key of keys) {
    if (isForbiddenMetricKey(key)) continue

    const rawValues = rows
      .map((row: PublicRow) => parseNumber(row[key]))
      .filter((v: number | null): v is number => v !== null)

    if (rawValues.length < 3) continue

    const values = normalizePct(rawValues)
    const minV = Math.min(...values)
    const maxV = Math.max(...values)
    const spread = maxV - minV

    if (spread === 0) continue
    if (Math.max(...values.map((v: number) => Math.abs(v))) > 1000) continue

    const score = metricKeyScore(key) * 10 + spread + rawValues.length

    if (score > bestScore) {
      bestScore = score
      bestKey = key
      bestValues = values
    }
  }

  return {
    key: bestKey,
    values: bestValues,
  }
}

function moneySigned(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}$${Math.abs(v).toFixed(2)}`
}

function pctSigned(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
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
  const cryptoChart = cryptoSummary?.chart || []

  const stockRows: PublicRow[] =
    stock?.latest?.summary?.top_rows?.length
      ? stock?.latest?.summary?.top_rows
      : stock?.latest?.summary?.portfolio_rows || []

  const stockMetrics = useMemo(() => {
    const NOTIONAL_PER_ASSET = 1000
    const picked = chooseRealMetricColumn(stockRows)
    const values = picked.values

    const chart: StockPoint[] = values.map((pct: number, idx: number) => {
      const money = (pct / 100) * NOTIONAL_PER_ASSET
      return {
        x: idx + 1,
        label: `Cluster ${idx + 1}`,
        profit_money: Number(money.toFixed(2)),
        profit_pct: Number(pct.toFixed(2)),
      }
    })

    const totalMoney = chart.reduce(
      (acc: number, row: StockPoint) => acc + n(row.profit_money),
      0
    )

    const avgPct = chart.length
      ? chart.reduce((acc: number, row: StockPoint) => acc + n(row.profit_pct), 0) / chart.length
      : 0

    return {
      pickedKey: picked.key,
      chart,
      totalMoney,
      avgPct,
    }
  }, [stockRows, stock?.latest?.created_at])

  const combinedChart = useMemo<CombinedPoint[]>(() => {
    const maxLen = Math.max(cryptoChart.length, stockMetrics.chart.length)

    return Array.from({ length: maxLen }).map((_, idx: number) => {
      const c = cryptoChart[idx]
      const s = stockMetrics.chart[idx]

      const cryptoMoney = c ? n(c.profit_money) : 0
      const cryptoPct = c ? n(c.profit_pct) : 0
      const stockMoney = s ? n(s.profit_money) : 0
      const stockPct = s ? n(s.profit_pct) : 0

      return {
        x: idx + 1,
        crypto_profit_money: cryptoMoney,
        crypto_profit_pct: cryptoPct,
        stock_profit_money: stockMoney,
        stock_profit_pct: stockPct,
        combined_profit_money: Number((((cryptoMoney + stockMoney) / 2)).toFixed(2)),
        combined_profit_pct: Number((((cryptoPct + stockPct) / 2)).toFixed(2)),
      }
    })
  }, [cryptoChart, stockMetrics.chart])

  const combinedNow =
    combinedChart.length > 0
      ? combinedChart[combinedChart.length - 1]
      : {
          x: 0,
          crypto_profit_money: 0,
          crypto_profit_pct: 0,
          stock_profit_money: 0,
          stock_profit_pct: 0,
          combined_profit_money: 0,
          combined_profit_pct: 0,
        }

  const spreadMoney = n(combinedNow.crypto_profit_money) - n(combinedNow.stock_profit_money)

  const sectorLeader =
    n(combinedNow.crypto_profit_money) > n(combinedNow.stock_profit_money)
      ? 'BTTcrypto'
      : n(combinedNow.crypto_profit_money) < n(combinedNow.stock_profit_money)
        ? 'BTTstock'
        : 'Parità'

  const positiveCombinedPoints = combinedChart.filter(
    (p: CombinedPoint) => n(p.combined_profit_money) > 0
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
          <span className="muted">Media profitto/perdita</span>
          <strong>{moneySigned(n(combinedNow.combined_profit_money))}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Media rendimento %</span>
          <strong>{pctSigned(n(combinedNow.combined_profit_pct))}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Differenziale settori</span>
          <strong>{moneySigned(spreadMoney)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Settore leader</span>
          <strong>{sectorLeader}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Totale BTTcrypto</span>
          <strong>{moneySigned(n(cryptoSummary?.profit_money))}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento BTTcrypto</span>
          <strong>{pctSigned(n(cryptoSummary?.profit_pct))}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Totale BTTstock</span>
          <strong>{stockMetrics.chart.length ? moneySigned(stockMetrics.totalMoney) : 'N/D'}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento medio BTTstock</span>
          <strong>{stockMetrics.chart.length ? pctSigned(stockMetrics.avgPct) : 'N/D'}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Consistency rate</span>
          <strong>{pctSigned(consistencyPct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Finestre stock aggregate</span>
          <strong>{stockMetrics.chart.length}</strong>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Media grafica BTTcrypto + BTTstock</h2>
        <div style={{ width: '100%', height: 360 }}>
          <ResponsiveContainer>
            <LineChart data={combinedChart}>
              <XAxis dataKey="x" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="combined_profit_money" strokeWidth={2} dot={false} />
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
                current_spread_money: Number(spreadMoney.toFixed(2)),
                stock_windows: stockMetrics.chart.length,
                detected_stock_metric_key: stockMetrics.pickedKey,
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
            <span>Solo risultati, percentuali, valori aggregati e sintesi premium.</span>
          </div>
        </div>
      </div>
    </div>
  )
}