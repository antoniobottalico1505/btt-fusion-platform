'use client'

import { useEffect, useState } from 'react'
import { apiFetch, getToken, goToLogin, isAuthMissingOrExpired } from '@/lib/api'
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
  label: string
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

function signedPct(v: number): string {
  const sign = v >= 0 ? '+' : '-'
  return `${sign}${Math.abs(v).toFixed(2)}%`
}

function maybePct(v: unknown): string {
  const n = Number(v)
  if (!Number.isFinite(n)) return 'N/D'
  return signedPct(n)
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
      if (!getToken()) {
        goToLogin('/bttstock')
        return
      }
      const res = await apiFetch<{ job_id: number; status: string }>(
        '/api/user/btt/run',
        { method: 'POST' },
        true
      )
      setMsg(`Analisi BTTstock avviata: job #${res.job_id}`)
      await load()
    } catch (e: any) {
      if (isAuthMissingOrExpired(e)) {
        goToLogin('/bttstock')
        return
      }

      setErr(e.message)
    }
  }

  const latest = data?.latest
  const summaryMetrics = data?.summary_metrics || {}
  const publicMetrics: PublicMetrics = summaryMetrics?.public_metrics || {
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

  const chart: ChartPoint[] = Array.isArray(publicMetrics.chart) ? publicMetrics.chart : []

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
              detected_metric_key: publicMetrics?.metric_key || null,
              point_count: publicMetrics?.point_count || 0,
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
            <span className="muted">Rendimento medio %</span>
            <strong>{maybePct(publicMetrics?.avg_pct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Rendimento ultimo titolo</span>
            <strong>{maybePct(publicMetrics?.last_pct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Range migliore %</span>
            <strong>{maybePct(publicMetrics?.best_pct)}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Range peggiore %</span>
            <strong>{maybePct(publicMetrics?.worst_pct)}</strong>
          </div>
        </div>

        <div className="kpi-grid" style={{ marginTop: 14 }}>
          <div className="kpi">
            <span className="muted">Titoli positivi</span>
            <strong>{publicMetrics?.positives ?? 0}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Titoli negativi</span>
            <strong>{publicMetrics?.negatives ?? 0}</strong>
          </div>
          <div className="kpi">
            <span className="muted">Titoli rilevati</span>
            <strong>{publicMetrics?.point_count ?? 0}</strong>
          </div>
        </div>

        <div style={{ width: '100%', height: 340, marginTop: 16 }}>
          <ResponsiveContainer>
            <LineChart data={chart}>
              <XAxis dataKey="x" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="profit_pct" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Percentuali dei titoli rilevati</h2>
        <div className="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Titolo</th>
                <th>Rendimento %</th>
                <th>Stato</th>
              </tr>
            </thead>
            <tbody>
              {chart.length === 0 ? (
                <tr>
                  <td colSpan={3}>Nessun dato disponibile</td>
                </tr>
              ) : (
                chart.map((row: ChartPoint) => (
                  <tr key={row.x}>
                    <td>{row.label}</td>
                    <td>{signedPct(row.profit_pct)}</td>
                    <td>{row.profit_pct > 0 ? 'Positivo' : row.profit_pct < 0 ? 'Negativo' : 'Neutro'}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h2 className="section-title">Policy di visualizzazione</h2>
          <div className="stack muted">
            <span>Nessun ticker mostrato pubblicamente.</span>
            <span>Nessun nome stock mostrato pubblicamente.</span>
            <span>Ogni riga rappresenta un titolo rilevato in forma anonima.</span>
            <span>Output limitato a percentuali reali per titolo e risultati aggregati.</span>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Significato dei dati</h2>
          <div className="stack muted">
            <span>Ogni punto della serie rappresenta un titolo trovato dal programma.</span>
            <span>La media % è la media reale delle percentuali dei titoli trovati.</span>
            <span>Titoli positivi/negativi indica quanti titoli hanno percentuale sopra o sotto zero.</span>
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