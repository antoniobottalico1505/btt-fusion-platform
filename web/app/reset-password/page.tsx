'use client'

import { useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function ResetPasswordPage() {
  const [password, setPassword] = useState('')
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setMsg('')
    setErr('')

    const token = new URLSearchParams(window.location.search).get('token') || ''

    try {
      const res = await apiFetch<{ message: string }>('/api/auth/reset-password', {
        method: 'POST',
        body: JSON.stringify({ token, password }),
      })
      setMsg(res.message || 'Password aggiornata')
    } catch (e: any) {
      setErr(e.message)
    }
  }

  return (
    <div className="shell section">
      <div className="auth-card panel">
        <h1 className="section-title">Nuova password</h1>
        <form className="stack" onSubmit={submit}>
          <input type="password" placeholder="Nuova password" value={password} onChange={(e) => setPassword(e.target.value)} />
          {msg ? <div className="good">{msg}</div> : null}
          {err ? <div className="bad">{err}</div> : null}
          <button type="submit">Aggiorna password</button>
        </form>
      </div>
    </div>
  )
}