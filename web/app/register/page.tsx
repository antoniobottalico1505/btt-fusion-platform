'use client'

import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function RegisterPage() {
  const router = useRouter()
  const [fullName, setFullName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [msg, setMsg] = useState('')

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setMsg('')

    try {
      const res = await apiFetch<{ message: string }>('/api/auth/register', {
        method: 'POST',
        body: JSON.stringify({ email, password, full_name: fullName }),
      })

      setMsg(res.message || 'Account creato. Verifica la tua email.')
      setTimeout(() => {
        router.push('/login?verify=1')
      }, 1200)
    } catch (err: any) {
      setError(err.message)
    }
  }

  return (
    <div className="auth-card panel">
      <h1 className="section-title">Registrazione</h1>
      <p className="section-sub">Accesso illimitato dopo verifica email.</p>

      <form className="stack" onSubmit={submit}>
        <input placeholder="Nome" value={fullName} onChange={(e) => setFullName(e.target.value)} />
        <input placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} />
        <input placeholder="Password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />

        {msg ? <div className="good">{msg}</div> : null}
        {error ? <div className="bad">{error}</div> : null}

        <button type="submit">Crea account</button>
      </form>

      <p className="muted">Hai già un account? <Link href="/login">Login</Link></p>
    </div>
  )
}