'use client'

import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function BttPage() {
  const [data, setData] = useState<any>(null)
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')

  useEffect(() => {
    apiFetch('/api/public/btt/latest').then(setData).catch((e) => setErr(e.message))
  }, [])

  async function run() {
    setMsg('')
    setErr('')
    try {
      const res = await apiFetch<{ job_id: number; status: string }>('/api/user/btt/run', { method: 'POST' }, true)
      setMsg(`Run avviato: job #${res.job_id}`)
    } catch (e: any) {
      setErr(e.message)
    }
  }

  const latest = data?.latest
  const topRows = latest?.summary?.top_rows || []
  const portfolioRows = latest?.summary?.portfolio_rows || []

  return (
    <div className="shell section stack">
      <div className="row">
        <div>
          <h1 className="section-title">BTT Capital</h1>
          <p className="section-sub">Ranking azionario server-side con report HTML e CSV. Nessuna logica interna esposta al client.</p>
        </div>
        <button onClick={run}>Lancia run demo</button>
      </div>
      {msg ? <div className="good">{msg}</div> : null}
      {err ? <div className="bad">{err}</div> : null}
      {!data?.has_job ? (
        <div className="card"><h2 className="section-title">Nessun report ancora disponibile</h2><p className="muted">Il motore è già collegato. Il primo report apparirà qui dopo il primo run server-side.</p></div>
      ) : (
        <>
          <div className="grid-2">
            <div className="card"><h2 className="section-title">Stato ultimo job</h2><pre className="log">{JSON.stringify({ id: latest.id, status: latest.status, created_at: latest.created_at, preset: latest.summary?.preset }, null, 2)}</pre></div>
            <div className="card"><h2 className="section-title">Log</h2><pre className="log">{latest.stdout_log || latest.error_log || 'Nessun log'}</pre></div>
          </div>
          <div className="card">
            <div className="row">
              <h2 className="section-title">Top ranking</h2>
              {latest?.id ? (
                <a href={`/api/public/btt/report/${latest.id}`} target="_blank" rel="noreferrer">
                  Apri report HTML
                </a>
              ) : null}
            </div>
            <div className="table-wrap"><table><thead><tr>{topRows[0] ? Object.keys(topRows[0]).map((k) => <th key={k}>{k}</th>) : <th>Nessun dato</th>}</tr></thead><tbody>
              {topRows.map((row: any, idx: number) => <tr key={idx}>{Object.keys(topRows[0] || {}).map((k) => <td key={k}>{String(row[k] ?? '')}</td>)}</tr>)}
            </tbody></table></div>
          </div>
          <div className="card">
            <h2 className="section-title">Portafoglio suggerito</h2>
            <div className="table-wrap"><table><thead><tr>{portfolioRows[0] ? Object.keys(portfolioRows[0]).map((k) => <th key={k}>{k}</th>) : <th>Nessun dato</th>}</tr></thead><tbody>
              {portfolioRows.map((row: any, idx: number) => <tr key={idx}>{Object.keys(portfolioRows[0] || {}).map((k) => <td key={k}>{String(row[k] ?? '')}</td>)}</tr>)}
            </tbody></table></div>
          </div>
        </>
      )}
    </div>
  )
}
