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

type PublicRow = Record<string, unknown>

type StockChartPoint = {
  x: number
  label: string
  profit_pct: number
  profit_money: number
}

type MetricColumnPick = {
  key: string | null
  values: number[]
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
    new Set(
      rows.flatMap((row: PublicRow) => Object.keys(row || {}))
    )
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

    // escludi numeri enormi che non sono plausibili come performance %
    if (Math.max(...values.map((v: number) => Math.abs(v))) > 1000) continue

    const score =
      metricKeyScore(key) * 10 +
      spread +
      rawValues.length

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

function signedMoney(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}$${Math.abs(v).toFixed(2)}`
}

function signedPct(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
}

function maybeMoney(v: number | null): string {
  if (v === null || !Number.isFinite(v)) return 'N/D'
  return signedMoney(v)
}

function maybePct(v: number | null): string {
  if (v === null || !Number.isFinite(v)) return 'N/D'
  return signedPct(v)
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
  const topRows: PublicRow[] = latest?.summary?.top_rows || []
  const portfolioRows: PublicRow[] = latest?.summary?.portfolio_rows || []
  const performanceRows: PublicRow[] = topRows.length ? topRows : portfolioRows

  const stockMetrics = useMemo(() => {
    const NOTIONAL_PER_ASSET = 1000

    const picked = chooseRealMetricColumn(performanceRows)
    const values = picked.values

    const chart: StockChartPoint[] = values.map((pct: number, idx: number) => {
      const money = (pct / 100) * NOTIONAL_PER_ASSET
      return {
        x: idx + 1,
        label: `Cluster ${idx + 1}`,
        profit_pct: Number(pct.toFixed(2)),
        profit_money: Number(money.toFixed(2)),
      }
    })

    if (!chart.length) {
      return {
        pickedKey: null as string | null,
        totalMoney: null as number | null,
        avgPct: null as number | null,
        wins: 0,
        losses: 0,
        lastMoney: null as number | null,
        lastPct: null as number | null,
        bestPct: null as number | null,
        worstPct: null as number | null,
        chart: [] as StockChartPoint[],
        reportChangedAt: latest?.created_at || null,
        usablePoints: 0,
      }
    }

    const totalMoney = chart.reduce(
      (acc: number, row: StockChartPoint) => acc + row.profit_money,
      0
    )

    const avgPct = chart.reduce(
      (acc: number, row: StockChartPoint) => acc + row.profit_pct,
      0
    ) / chart.length

    const wins = chart.filter((x: StockChartPoint) => x.profit_pct > 0).length
    const losses = chart.filter((x: StockChartPoint) => x.profit_pct < 0).length
    const last = chart[chart.length - 1]

    const bestPct = Math.max(...chart.map((x: StockChartPoint) => x.profit_pct))
    const worstPct = Math.min(...chart.map((x: StockChartPoint) => x.profit_pct))

    return {
      pickedKey: picked.key,
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
      usablePoints: chart.length,
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
              usable_public_points: stockMetrics.usablePoints,
              detected_metric_key: stockMetrics.pickedKey,
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
            <strong>{maybeMoney(stockMetrics.totalMoney)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Profitto / Perdita ultima finestra</span>
            <strong>{maybeMoney(stockMetrics.lastMoney)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Rendimento medio %</span>
            <strong>{maybePct(stockMetrics.avgPct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Rendimento ultima finestra</span>
            <strong>{maybePct(stockMetrics.lastPct)}</strong>
          </div>
        </div>

        <div className="kpi-grid" style={{ marginTop: 14 }}>
          <div className="kpi">
            <span className="muted">Range migliore %</span>
            <strong>{maybePct(stockMetrics.bestPct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Range peggiore %</span>
            <strong>{maybePct(stockMetrics.worstPct)}</strong>
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
            <span>Se il report non contiene una metrica reale leggibile, la pagina mostra N/D.</span>
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