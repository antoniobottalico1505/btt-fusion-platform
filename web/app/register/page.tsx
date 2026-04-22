'use client'

import Link from 'next/link'
import { useRouter } from 'next/navigation'
import { useState } from 'react'
import { apiFetch, setToken } from '@/lib/api'

export default function RegisterPage() {
  const router = useRouter()
  const [fullName, setFullName] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')

  async function submit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    try {
      const res = await apiFetch<{ access_token: string }>('/api/auth/register', {
        method: 'POST',
        body: JSON.stringify({ email, password, full_name: fullName }),
      })
      setToken(res.access_token)
      router.push('/dashboard')
    } catch (err: any) {
      setError(err.message)
    }
  }

  return (
    <div className="auth-card panel">
      <h1 className="section-title">Registrazione</h1>
      <p className="section-sub">La prova gratuita dura 24 ore dall’attivazione.</p>
      <form className="stack" onSubmit={submit}>
        <input placeholder="Nome" value={fullName} onChange={(e) => setFullName(e.target.value)} />
        <input placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} />
        <input placeholder="Password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
        {error ? <div className="bad">{error}</div> : null}
        <button type="submit">Crea account</button>
      </form>
      <p className="muted">Hai già un account? <Link href="/login">Login</Link></p>
    </div>
  )
}
