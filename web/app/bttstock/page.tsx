'use client'

import { useEffect, useMemo, useState } from 'react'
import { apiFetch } from '@/lib/api'
import { LineChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'

function percentFromRow(row: any) {
  const keys = Object.keys(row || {})
  const found = keys.find((k) => k.toLowerCase().includes('return') || k.toLowerCase().includes('perf') || k.toLowerCase().includes('upside'))
  if (!found) return null
  const raw = String(row[found] ?? '').replace('%', '').trim()
  const n = Number(raw)
  if (Number.isNaN(n)) return null
  return n
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

  const perfSeries = useMemo(() => {
    return topRows.slice(0, 12).map((row: any, idx: number) => ({
      name: row.ticker || row.symbol || row.name || `Row ${idx + 1}`,
      perf: percentFromRow(row) ?? 0,
    }))
  }, [topRows])

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">BTTstock</h1>
          <p className="section-sub">Analisi azionaria server-side con ranking, rendimento potenziale e risultati consultabili.</p>
        </div>
        <button onClick={run}>Avvia analisi BTTstock</button>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {err ? <div className="bad">{err}</div> : null}

      <div className="card">
        <h2 className="section-title">Stato elaborazione</h2>
        <pre className="log">{JSON.stringify({
          id: latest?.id,
          status: latest?.status,
          created_at: latest?.created_at,
          return_code: latest?.summary?.return_code,
        }, null, 2)}</pre>
      </div>

      <div className="card">
        <h2 className="section-title">Grafico rendimento BTTstock</h2>
        <div style={{ width: '100%', height: 300 }}>
          <ResponsiveContainer>
            <LineChart data={perfSeries}>
              <XAxis dataKey="name" />
              <YAxis />
              <Tooltip />
              <Line type="monotone" dataKey="perf" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </div>

      <div className="grid-2">
        <div className="card">
          <h2 className="section-title">Titoli con potenziale positivo</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {topRows[0] ? Object.keys(topRows[0]).map((k) => <th key={k}>{k}</th>) : <th>Nessun dato</th>}
                </tr>
              </thead>
              <tbody>
                {topRows.length === 0 ? (
                  <tr><td>Nessun dato</td></tr>
                ) : topRows.map((row: any, idx: number) => (
                  <tr key={idx}>
                    {Object.keys(topRows[0] || {}).map((k) => <td key={k}>{String(row[k] ?? '')}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="card">
          <h2 className="section-title">Portafoglio suggerito</h2>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  {portfolioRows[0] ? Object.keys(portfolioRows[0]).map((k) => <th key={k}>{k}</th>) : <th>Nessun dato</th>}
                </tr>
              </thead>
              <tbody>
                {portfolioRows.length === 0 ? (
                  <tr><td>Nessun dato</td></tr>
                ) : portfolioRows.map((row: any, idx: number) => (
                  <tr key={idx}>
                    {Object.keys(portfolioRows[0] || {}).map((k) => <td key={k}>{String(row[k] ?? '')}</td>)}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="card">
        <h2 className="section-title">Log elaborazione</h2>
        <pre className="log">{latest?.stdout_log || latest?.error_log || 'Nessun log disponibile'}</pre>
      </div>
    </div>

<div className="card">
  <h2 className="section-title">Performance BTTstock</h2>
  <div className="kpi-grid">
    <div className="kpi"><span className="muted">Profitto / Perdita stimata</span><strong>${data?.summary_metrics?.profit_money?.toFixed?.(2) || '0.00'}</strong></div>
    <div className="kpi"><span className="muted">Rendimento medio %</span><strong>{data?.summary_metrics?.profit_pct?.toFixed?.(2) || '0.00'}%</strong></div>
    <div className="kpi"><span className="muted">Titoli positivi</span><strong>{data?.summary_metrics?.wins ?? 0}</strong></div>
    <div className="kpi"><span className="muted">Titoli negativi</span><strong>{data?.summary_metrics?.losses ?? 0}</strong></div>
  </div>

  <div style={{ width: '100%', height: 320, marginTop: 16 }}>
    <ResponsiveContainer>
      <LineChart data={data?.summary_metrics?.chart || []}>
        <XAxis dataKey="label" />
        <YAxis />
        <Tooltip />
        <Line type="monotone" dataKey="profit_money" strokeWidth={2} dot={false} />
        <Line type="monotone" dataKey="profit_pct" strokeWidth={2} dot={false} />
      </LineChart>
    </ResponsiveContainer>
  </div>
</div>
  )
}