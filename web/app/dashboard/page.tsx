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

function n(v: any): number {
  const x = Number(v)
  return Number.isFinite(x) ? x : 0
}

function parseNumber(raw: any): number | null {
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

function parsePctFromRow(row: Record<string, any>): number | null {
  const entries = Object.entries(row || {})

  const preferred = entries.find(([k]) => {
    const kk = k.toLowerCase()
    return (
      kk.includes('return') ||
      kk.includes('perf') ||
      kk.includes('upside') ||
      kk.includes('gain') ||
      kk.includes('profit') ||
      kk.includes('yield') ||
      kk.includes('cagr')
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
      .catch((e) => setError(e.message || 'Errore caricamento BTTcrypto'))

    apiFetch('/api/public/btt/latest')
      .then(setStock)
      .catch(() => null)
  }, [])

  const cryptoCurve = crypto?.dashboard?.equity_curve || []
  const cryptoPerformanceChart = useMemo(() => {
    if (!cryptoCurve.length) return []
    const first = n(cryptoCurve[0]?.equity)

    return cryptoCurve.map((row: any, idx: number) => {
      const eq = n(row?.equity)
      const profitMoney = eq - first
      const profitPct = first > 0 ? (profitMoney / first) * 100 : 0
      return {
        x: idx + 1,
        profit_money: Number(profitMoney.toFixed(2)),
        profit_pct: Number(profitPct.toFixed(2)),
      }
    })
  }, [cryptoCurve])

  const cryptoTotalMoney = cryptoPerformanceChart.length
    ? n(cryptoPerformanceChart[cryptoPerformanceChart.length - 1]?.profit_money)
    : 0

  const cryptoTotalPct = cryptoPerformanceChart.length
    ? n(cryptoPerformanceChart[cryptoPerformanceChart.length - 1]?.profit_pct)
    : 0

  const stockRows = stock?.latest?.summary?.portfolio_rows?.length
    ? stock?.latest?.summary?.portfolio_rows
    : stock?.latest?.summary?.top_rows || []

  const stockPerformanceChart = useMemo(() => {
    const NOTIONAL_PER_ASSET = 1000

    return stockRows.slice(0, 20).map((row: any, idx: number) => {
      const pct = parsePctFromRow(row) ?? 0
      const money = (pct / 100) * NOTIONAL_PER_ASSET

      return {
        x: idx + 1,
        label: row?.ticker || row?.symbol || row?.name || `Asset ${idx + 1}`,
        profit_money: Number(money.toFixed(2)),
        profit_pct: Number(pct.toFixed(2)),
      }
    })
  }, [stockRows])

  const stockAvgMoney = stockPerformanceChart.length
    ? stockPerformanceChart.reduce((acc, row) => acc + n(row.profit_money), 0)
    : 0

  const stockAvgPct = stockPerformanceChart.length
    ? stockPerformanceChart.reduce((acc, row) => acc + n(row.profit_pct), 0) / stockPerformanceChart.length
    : 0

  const combinedChart = useMemo(() => {
    const maxLen = Math.max(cryptoPerformanceChart.length, stockPerformanceChart.length)

    return Array.from({ length: maxLen }).map((_, idx) => {
      const cryptoPoint = cryptoPerformanceChart[idx]
      const stockPoint = stockPerformanceChart[idx]

      const cryptoMoney = cryptoPoint ? n(cryptoPoint.profit_money) : 0
      const cryptoPct = cryptoPoint ? n(cryptoPoint.profit_pct) : 0
      const stockMoney = stockPoint ? n(stockPoint.profit_money) : 0
      const stockPct = stockPoint ? n(stockPoint.profit_pct) : 0

      const combinedMoney = (cryptoMoney + stockMoney) / 2
      const combinedPct = (cryptoPct + stockPct) / 2

      return {
        x: idx + 1,
        crypto_profit_money: cryptoMoney,
        crypto_profit_pct: cryptoPct,
        stock_profit_money: stockMoney,
        stock_profit_pct: stockPct,
        combined_profit_money: Number(combinedMoney.toFixed(2)),
        combined_profit_pct: Number(combinedPct.toFixed(2)),
      }
    })
  }, [cryptoPerformanceChart, stockPerformanceChart])

  const combinedNow = combinedChart.length
    ? combinedChart[combinedChart.length - 1]
    : {
        combined_profit_money: 0,
        combined_profit_pct: 0,
        crypto_profit_money: 0,
        stock_profit_money: 0,
      }

  const spreadMoney = n(combinedNow.crypto_profit_money) - n(combinedNow.stock_profit_money)
  const sectorLeader =
    n(combinedNow.crypto_profit_money) > n(combinedNow.stock_profit_money)
      ? 'BTTcrypto'
      : n(combinedNow.crypto_profit_money) < n(combinedNow.stock_profit_money)
        ? 'BTTstock'
        : 'Parità'

  const positiveCombinedPoints = combinedChart.filter((p) => n(p.combined_profit_money) > 0).length
  const consistencyPct = combinedChart.length
    ? (positiveCombinedPoints / combinedChart.length) * 100
    : 0

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Dashboard BTTcapital</h1>
        <p className="section-sub">
          Vista media aggregata di BTTcrypto e BTTstock con parametri aggiuntivi di sintesi.
        </p>
      </div>

      {error ? <div className="bad">{error}</div> : null}

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
          <strong>{moneySigned(cryptoTotalMoney)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento BTTcrypto</span>
          <strong>{pctSigned(cryptoTotalPct)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Totale BTTstock</span>
          <strong>{moneySigned(stockAvgMoney)}</strong>
        </div>
        <div className="kpi">
          <span className="muted">Rendimento medio BTTstock</span>
          <strong>{pctSigned(stockAvgPct)}</strong>
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
              },
              null,
              2
            )}
          </pre>
        </div>

        <div className="card">
          <h2 className="section-title">Confronto settoriale diretto</h2>
          <div style={{ width: '100%', height: 320 }}>
            <ResponsiveContainer>
              <LineChart data={combinedChart}>
                <XAxis dataKey="x" />
                <YAxis />
                <Tooltip />
                <Line type="monotone" dataKey="crypto_profit_money" strokeWidth={2} dot={false} />
                <Line type="monotone" dataKey="stock_profit_money" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>
    </div>
  )
}