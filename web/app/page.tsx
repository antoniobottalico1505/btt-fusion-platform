'use client'

import Link from 'next/link'
import { useEffect, useState } from 'react'
import { apiFetch } from '@/lib/api'

export default function HomePage() {
  const [site, setSite] = useState<any>(null)
  const [crypto, setCrypto] = useState<any>(null)
  const [stock, setStock] = useState<any>(null)

  useEffect(() => {
    apiFetch('/api/public/site').then(setSite).catch(() => null)
    apiFetch('/api/public/bttcrypto').then(setCrypto).catch(() => null)
    apiFetch('/api/public/bttstock/latest').then(setStock).catch(() => null)
  }, [])

  const overview = crypto?.dashboard?.overview

  return (
    <div className="shell">
      <section className="hero">
        <div className="panel hero-copy">
          <span className="eyebrow">Premium market intelligence platform</span>
          <h1 className="h1">{site?.copy?.hero_title || 'BTTcapital'}</h1>
          <p className="lead">
            {site?.copy?.hero_subtitle || 'BTTcapital unisce BTTcrypto e BTTstock in un’unica esperienza premium per analisi, osservazione e operatività assistita.'}
          </p>
          <div className="actions">
            <Link href="/dashboard"><button>Apri dashboard</button></Link>
            <Link href="/pricing"><button className="secondary">Abbonati</button></Link>
          </div>
        </div>

        <div className="hero-stats">
          <div className="metric"><div className="metric-label">BTTcrypto mode</div><div className="metric-value">{crypto?.public_mode || 'paper'}</div></div>
          <div className="metric"><div className="metric-label">Cash stimata</div><div className="metric-value">${overview?.cash?.toFixed?.(2) || '0.00'}</div></div>
          <div className="metric"><div className="metric-label">Watchlist attuale</div><div className="metric-value">{overview?.watchlist_count ?? 0}</div></div>
          <div className="metric"><div className="metric-label">Ultimo report BTTstock</div><div className="metric-value">{stock?.has_job ? stock.latest.status : 'Nessuno'}</div></div>
        </div>
      </section>

      <section className="section grid-3">
        <div className="card">
          <h3 className="section-title">BTTcrypto</h3>
          <p className="section-sub">Motore crypto osservabile lato piattaforma.</p>
          <div className="stack muted">
            <span>Monitoraggio crypto</span>
            <span>Dashboard operativa</span>
            <span>Dati lato server / heartbeat esterno</span>
          </div>
        </div>

        <div className="card">
          <h3 className="section-title">BTTstock</h3>
          <p className="section-sub">Motore stock server-side con ranking e report.</p>
          <div className="stack muted">
            <span>Analisi azionaria multi-country</span>
            <span>Report HTML e CSV</span>
            <span>Output premium</span>
          </div>
        </div>

        <div className="card">
          <h3 className="section-title">Abbonamenti</h3>
          <p className="section-sub">Crypto, Stock o Bundle, mensile o annuale.</p>
          <div className="stack muted">
            <span>BTTcrypto</span>
            <span>BTTstock</span>
            <span>BTTcapital bundle</span>
          </div>
        </div>
      </section>
    </div>
  )
}