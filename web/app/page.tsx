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
    apiFetch('/api/public/microcap').then(setCrypto).catch(() => null)
    apiFetch('/api/public/btt/latest').then(setStock).catch(() => null)
  }, [])

  const overview = crypto?.dashboard?.overview || {}
  const cryptoSummary = crypto?.dashboard?.summary || crypto?.summary || {}

  return (
    <div className="shell">
      <section className="hero">
        <div className="panel hero-copy">
          <span className="eyebrow">Premium market intelligence platform</span>
          <h1 className="h1">{site?.copy?.hero_title || 'BTTcapital'}</h1>
          <p className="lead">
            {site?.copy?.hero_subtitle ||
              'BTTcapital unisce BTTcrypto e BTTstock in un’unica esperienza premium per analisi, osservazione e operatività assistita.'}
          </p>
          <div className="actions">
            <Link href="/dashboard"><button>Apri dashboard</button></Link>
            <Link href="/pricing"><button className="secondary">Abbonati</button></Link>
          </div>
        </div>

        <div className="hero-stats">
          <div className="metric">
            <div className="metric-label">BTTcrypto mode</div>
            <div className="metric-value">{crypto?.public_mode || 'paper'}</div>
          </div>

          <div className="metric">
            <div className="metric-label">Cash stimata</div>
            <div className="metric-value">${Number(overview?.cash || 0).toFixed(2)}</div>
          </div>

          <div className="metric">
            <div className="metric-label">Profitto totale crypto</div>
            <div className="metric-value">
              {Number(cryptoSummary?.profit_money || 0) >= 0 ? '+' : '-'}$
              {Math.abs(Number(cryptoSummary?.profit_money || 0)).toFixed(2)}
            </div>
          </div>

          <div className="metric">
            <div className="metric-label">Ultimo report BTTstock</div>
            <div className="metric-value">{stock?.has_job ? stock.latest.status : 'Nessuno'}</div>
          </div>
        </div>
      </section>

      <section className="section grid-3">
        <div className="card">
          <h3 className="section-title">BTTcrypto</h3>
          <p className="section-sub">Motore crypto osservabile lato piattaforma.</p>
          <div className="stack muted">
            <span>Monitoraggio crypto</span>
            <span>Dashboard operativa</span>
            <span>Dati reali lato server / heartbeat</span>
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
          <h3 className="section-title">Abbonamento unico</h3>
          <p className="section-sub">Un solo piano per l’accesso completo a tutto BTTcapital.</p>
          <div className="stack muted">
            <span>Mensile: €99</span>
            <span>Annuale: €990</span>
            <span>Accesso completo a BTTcrypto e BTTstock</span>
          </div>
        </div>
      </section>
    </div>
  )
}