'use client'

import { useEffect, useMemo, useState } from 'react'
import { apiFetch } from '@/lib/api'
import { LineChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

export default function DashboardPage() {
  const [crypto, setCrypto] = useState<any>(null)
  const [stock, setStock] = useState<any>(null)

  useEffect(() => {
    apiFetch('/api/public/microcap').then(setCrypto).catch(() => null)
    apiFetch('/api/public/btt/latest').then(setStock).catch(() => null)
    apiFetch('/api/public/combined/summary').then(setCombined)
  }, [])

  const cryptoSeries = crypto?.dashboard?.equity_curve || []
  const stockRows = stock?.latest?.summary?.top_rows || []

  const stockSeries = stockRows.slice(0, 12).map((row: any, idx: number) => ({
    ts: idx + 1,
    stock: idx + 1,
  }))

  const combined = useMemo(() => {
    const maxLen = Math.max(cryptoSeries.length, stockSeries.length)
    return Array.from({ length: maxLen }).map((_, idx) => ({
      label: `${idx + 1}`,
      crypto: cryptoSeries[idx]?.equity ?? null,
      stock: stockSeries[idx]?.stock ?? null,
    }))
  }, [cryptoSeries, stockSeries])

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Dashboard BTTcapital</h1>
        <p className="section-sub">Vista aggregata di BTTcrypto e BTTstock.</p>
      </div>

      <div className="card">
        <h2 className="section-title">Grafico aggregato settori</h2>
        <div style={{ width: '100%', height: 320 }}>
          <ResponsiveContainer>
            <LineChart data={combined}>
              <XAxis dataKey="label" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="crypto" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey="stock" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>

<div className="card">
  <h2 className="section-title">Confronto BTTcrypto + BTTstock</h2>
  <div style={{ width: '100%', height: 340 }}>
    <ResponsiveContainer>
      <LineChart data={combined?.combined?.chart || []}>
        <XAxis dataKey="x" />
        <YAxis />
        <Tooltip />
        <Line type="monotone" dataKey="crypto_profit_money" strokeWidth={2} dot={false} />
        <Line type="monotone" dataKey="stock_profit_money" strokeWidth={2} dot={false} />
        <Line type="monotone" dataKey="combined_profit_money" strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  </div>
</div>
  )
}