'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function HomePage() {
  const [site, setSite] = useState<any>(null)
  const [microcap, setMicrocap] = useState<any>(null)
  const [btt, setBtt] = useState<any>(null)

  useEffect(() => {
    apiFetch('/api/public/site').then(setSite).catch(() => null)
    apiFetch('/api/public/microcap').then(setMicrocap).catch(() => null)
    apiFetch('/api/public/btt/latest').then(setBtt).catch(() => null)
  }, [])

  const overview = microcap?.dashboard?.overview

  return (
    <div className="shell">
      <section className="hero">
        <div className="panel hero-copy">
          <span className="eyebrow">Quant platform / server-side intelligence</span>
          <h1 className="h1">{site?.copy?.hero_title || 'BTT Fusion'}</h1>
          <p className="lead">
            {site?.copy?.hero_subtitle || 'Un contenitore deployabile che unisce BTT Capital e Microcap Bot senza esporre logica, parametri o codice al pubblico.'}
          </p>
          <div className="actions">
            <Link href="/dashboard"><button>Apri dashboard</button></Link>
            <Link href="/pricing"><button className="secondary">Prova 24h / abbonati</button></Link>
          </div>
        </div>
        <div className="hero-stats">
          <div className="metric"><div className="metric-label">Microcap mode</div><div className="metric-value">{microcap?.public_mode || 'paper'}</div></div>
          <div className="metric"><div className="metric-label">Cash stimata</div><div className="metric-value">${overview?.cash?.toFixed?.(2) || '0.00'}</div></div>
          <div className="metric"><div className="metric-label">Watchlist attuale</div><div className="metric-value">{overview?.watchlist_count ?? 0}</div></div>
          <div className="metric"><div className="metric-label">Ultimo report BTT</div><div className="metric-value">{btt?.has_job ? btt.latest.status : 'Nessuno'}</div></div>
        </div>
      </section>

      <section className="section grid-3">
        <div className="card">
          <h3 className="section-title">Microcap Bot</h3>
          <p className="section-sub">{site?.copy?.microcap_tagline}</p>
          <div className="stack muted">
            <span>Paper demo pubblica osservabile</span>
            <span>Config editabile solo da admin</span>
            <span>Dashboard web collegata a bot.db</span>
          </div>
        </div>
        <div className="card">
          <h3 className="section-title">BTT Capital</h3>
          <p className="section-sub">{site?.copy?.btt_tagline}</p>
          <div className="stack muted">
            <span>Run server-side con risultati CSV + HTML</span>
            <span>Preset nascosti configurabili</span>
            <span>Ranking e portfolio visuali</span>
          </div>
        </div>
        <div className="card">
          <h3 className="section-title">Monetizzazione</h3>
          <p className="section-sub">Trial 24 ore + Stripe mensile/annuale.</p>
          <div className="stack muted">
            <span>Accesso demo immediato</span>
            <span>Checkout Stripe</span>
            <span>Owner admin panel</span>
          </div>
        </div>
      </section>
    </div>
  )
}
