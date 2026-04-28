'use client'

import { useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function ForgotPasswordPage() {
  const [email, setEmail] = useState('')
  const [msg, setMsg] = useState('')
  const [err, setErr] = useState('')

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setMsg('')
    setErr('')
    try {
      const res = await apiFetch<{ message: string }>('/api/auth/forgot-password', {
        method: 'POST',
        body: JSON.stringify({ email }),
      })
      setMsg(res.message || 'Controlla la tua email')
    } catch (e: any) {
      setErr(e.message)
    }
  }

  return (
    <div className="shell section">
      <div className="auth-card panel">
        <h1 className="section-title">Recupero password</h1>
        <form className="stack" onSubmit={submit}>
          <input placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} />
          {msg ? <div className="good">{msg}</div> : null}
          {err ? <div className="bad">{err}</div> : null}
          <button type="submit">Invia link reset</button>
        </form>
      </div>
    </div>
  )
}