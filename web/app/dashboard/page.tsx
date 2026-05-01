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

function parsePctFromRow(row: Record<string, unknown>): number | null {
  const entries = Object.entries(row || {})

  const preferred = entries.find(([k]) => {
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
    )
  })

  if (preferred) {
    const val = parseNumber(preferred[1])
    if (val !== null) return Math.abs(val) <= 1 ? val * 100 : val
  }

  const withPercent = entries.find(([, v]) => String(v).includes('%'))
  if (withPercent) {
    const val = parseNumber(withPercent[1])
    if (val !== null) return val
  }

  return null
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
    apiFetch('/api/public/microcap')
      .then(setCrypto)
      .catch((e: any) => setError(e.message || 'Errore caricamento BTTcrypto'))

    apiFetch('/api/public/btt/latest')
      .then(setStock)
      .catch(() => null)
  }, [])

  const cryptoSummary = crypto?.dashboard?.summary || crypto?.summary || {}
  const cryptoChart = cryptoSummary?.chart || []

  const stockRows: Record<string, unknown>[] =
    stock?.latest?.summary?.top_rows?.length
      ? stock?.latest?.summary?.top_rows
      : stock?.latest?.summary?.portfolio_rows || []

  const stockChart = useMemo<StockPoint[]>(() => {
    const NOTIONAL_PER_ASSET = 1000

    return stockRows.slice(0, 20).map((row: Record<string, unknown>, idx: number) => {
      const pct = parsePctFromRow(row) ?? 0
      const money = (pct / 100) * NOTIONAL_PER_ASSET

      return {
        x: idx + 1,
        label: `Cluster ${idx + 1}`,
        profit_money: Number(money.toFixed(2)),
        profit_pct: Number(pct.toFixed(2)),
      }
    })
  }, [stockRows])

  const stockTotalMoney = stockChart.reduce(
    (acc: number, row: StockPoint) => acc + n(row.profit_money),
    0
  )

  const stockAvgPct = stockChart.length
    ? stockChart.reduce(
        (acc: number, row: StockPoint) => acc + n(row.profit_pct),
        0
      ) / stockChart.length
    : 0

  const combinedChart = useMemo<CombinedPoint[]>(() => {
    const maxLen = Math.max(cryptoChart.length, stockChart.length)

    return Array.from({ length: maxLen }).map((_, idx: number) => {
      const c = cryptoChart[idx]
      const s = stockChart[idx]

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
  }, [cryptoChart, stockChart])

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

  const combinedPositiveMoney = combinedChart.filter((p: CombinedPoint) => n(p.combined_profit_money) > 0)
  const combinedNegativeMoney = combinedChart.filter((p: CombinedPoint) => n(p.combined_profit_money) < 0)

  const bestCombined = combinedChart.length
    ? Math.max(...combinedChart.map((p: CombinedPoint) => n(p.combined_profit_money)))
    : 0

  const worstCombined = combinedChart.length
    ? Math.min(...combinedChart.map((p: CombinedPoint) => n(p.combined_profit_money)))
    : 0

  const avgPositiveCombined =
    combinedPositiveMoney.length > 0
      ? combinedPositiveMoney.reduce((acc: number, p: CombinedPoint) => acc + n(p.combined_profit_money), 0) / combinedPositiveMoney.length
      : 0

  const avgNegativeCombined =
    combinedNegativeMoney.length > 0
      ? combinedNegativeMoney.reduce((acc: number, p: CombinedPoint) => acc + n(p.combined_profit_money), 0) / combinedNegativeMoney.length
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
          <strong>{moneySigned(stockTotalMoney)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento medio BTTstock</span>
          <strong>{pctSigned(stockAvgPct)}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Consistency rate</span>
          <strong>{pctSigned(consistencyPct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Best combined value</span>
          <strong>{moneySigned(bestCombined)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Worst combined value</span>
          <strong>{moneySigned(worstCombined)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Combined windows</span>
          <strong>{combinedChart.length}</strong>
        </div>
      </div>

      <div className="kpi-grid">
        <div className="kpi">
          <span className="muted">Average positive combined</span>
          <strong>{moneySigned(avgPositiveCombined)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Average negative combined</span>
          <strong>{moneySigned(avgNegativeCombined)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Positive combined windows</span>
          <strong>{combinedPositiveMoney.length}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Negative combined windows</span>
          <strong>{combinedNegativeMoney.length}</strong>
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
                positive_combined_windows: combinedPositiveMoney.length,
                negative_combined_windows: combinedNegativeMoney.length,
                total_combined_windows: combinedChart.length,
                sector_leader: sectorLeader,
                current_spread_money: Number(spreadMoney.toFixed(2)),
                best_combined_value: Number(bestCombined.toFixed(2)),
                worst_combined_value: Number(worstCombined.toFixed(2)),
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