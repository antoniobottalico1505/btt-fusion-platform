'use client'

import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'

type AdminOverview = {
  users?: number
  paid_users?: number
  btt_jobs?: number
  microcap_process?: {
    running?: boolean
  }
}

type ValueResponseString = {
  value?: string
}

type ValueResponseObject = {
  value?: Record<string, any>
}

type JobsResponse = any[]

export default function AdminPage() {
  const [overview, setOverview] = useState<AdminOverview | null>(null)
  const [microcapConfig, setMicrocapConfig] = useState('')
  const [microcapEnv, setMicrocapEnv] = useState('{}')
  const [bttPreset, setBttPreset] = useState('{}')
  const [jobs, setJobs] = useState<any[]>([])
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')

  async function load() {
    try {
      const [ov, cfg, env, preset, jobsRes] = await Promise.all([
        apiFetch<AdminOverview>('/api/admin/overview', undefined, true),
        apiFetch<ValueResponseString>('/api/admin/microcap/config', undefined, true),
        apiFetch<ValueResponseObject>('/api/admin/microcap/env', undefined, true),
        apiFetch<ValueResponseObject>('/api/admin/btt/preset', undefined, true),
        apiFetch<JobsResponse>('/api/admin/btt/jobs', undefined, true),
      ])

      setOverview(ov)
      setMicrocapConfig(cfg.value || '')
      setMicrocapEnv(JSON.stringify(env.value || {}, null, 2))
      setBttPreset(JSON.stringify(preset.value || {}, null, 2))
      setJobs(jobsRes || [])
    } catch (e: any) {
      setErr(e.message)
    }
  }

  useEffect(() => { load() }, [])

  async function saveMicrocapConfig() {
    setErr('')
    setMsg('')
    try {
      await apiFetch('/api/admin/microcap/config', { method: 'PUT', body: JSON.stringify({ value: microcapConfig }) }, true)
      setMsg('Config Microcap salvata')
    } catch (e: any) {
      setErr(e.message)
    }
  }

  async function saveMicrocapEnv() {
    setErr('')
    setMsg('')
    try {
      await apiFetch('/api/admin/microcap/env', { method: 'PUT', body: JSON.stringify({ value: JSON.parse(microcapEnv) }) }, true)
      setMsg('Env Microcap salvate')
    } catch (e: any) {
      setErr(e.message)
    }
  }

  async function saveBttPreset() {
    setErr('')
    setMsg('')
    try {
      await apiFetch('/api/admin/btt/preset', { method: 'PUT', body: JSON.stringify({ value: JSON.parse(bttPreset) }) }, true)
      setMsg('Preset BTT salvato')
    } catch (e: any) {
      setErr(e.message)
    }
  }

  async function control(path: string, body?: any) {
    setErr('')
    setMsg('')
    try {
      await apiFetch(path, { method: 'POST', body: body ? JSON.stringify(body) : '{}' }, true)
      setMsg('Comando eseguito')
      load()
    } catch (e: any) {
      setErr(e.message)
    }
  }

  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Admin</h1>
        <p className="section-sub">Controllo completo dei due engine senza esporre parametri al pubblico.</p>
      </div>

      {msg ? <div className="good">{msg}</div> : null}
      {err ? <div className="bad">{err}</div> : null}

      <div className="grid-3">
        <div className="kpi"><span className="muted">Users</span><strong>{overview?.users ?? 0}</strong></div>
        <div className="kpi"><span className="muted">Paid users</span><strong>{overview?.paid_users ?? 0}</strong></div>
        <div className="kpi"><span className="muted">BTT jobs</span><strong>{overview?.btt_jobs ?? 0}</strong></div>
      </div>

      <div className="grid-2">
        <div className="card stack">
          <h2 className="section-title">Microcap process</h2>
          <div className="row"><span className="muted">Running</span><strong>{overview?.microcap_process?.running ? 'yes' : 'no'}</strong></div>
          <div className="actions">
            <button onClick={() => control('/api/admin/microcap/start', { mode: 'paper' })}>Start paper</button>
            <button className="secondary" onClick={() => control('/api/admin/microcap/restart', { mode: 'paper' })}>Restart</button>
            <button className="ghost" onClick={() => control('/api/admin/microcap/stop')}>Stop</button>
          </div>
        </div>

        <div className="card stack">
          <h2 className="section-title">BTT engine</h2>
          <p className="muted">Lancia un nuovo job con i preset server-side.</p>
          <button onClick={() => control('/api/admin/btt/run')}>Run BTT now</button>
        </div>
      </div>

      <div className="grid-2">
        <div className="card stack">
          <h2 className="section-title">Microcap config.yaml</h2>
          <textarea rows={24} value={microcapConfig} onChange={(e) => setMicrocapConfig(e.target.value)} />
          <button onClick={saveMicrocapConfig}>Salva config</button>
        </div>

        <div className="card stack">
          <h2 className="section-title">Microcap runtime env</h2>
          <textarea rows={24} value={microcapEnv} onChange={(e) => setMicrocapEnv(e.target.value)} />
          <button onClick={saveMicrocapEnv}>Salva env</button>
        </div>
      </div>

      <div className="card stack">
        <h2 className="section-title">BTT preset</h2>
        <textarea rows={16} value={bttPreset} onChange={(e) => setBttPreset(e.target.value)} />
        <button onClick={saveBttPreset}>Salva preset BTT</button>
      </div>

      <div className="card stack">
        <h2 className="section-title">Storico jobs BTT</h2>
        <pre className="log">{JSON.stringify(jobs, null, 2)}</pre>
      </div>
    </div>
  )
}