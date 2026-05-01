export default function MethodologyPage() {
  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">Methodology</h1>
        <p className="section-sub">
          BTTcapital pubblica risultati aggregati, indicatori di performance e visualizzazioni di sintesi senza esporre il motore proprietario.
        </p>
      </div>

      <div className="grid-2">
        <div className="card stack">
          <h2 className="section-title">BTTcrypto</h2>
          <div className="stack muted">
            <span>Metriche aggregate di cash, profitto/perdita, rendimento, operazioni positive/negative.</span>
            <span>Andamento basato su equity curve e risultati effettivi disponibili nella sorgente dati.</span>
            <span>Presentazione orientata a qualità del risultato e controllo del rischio.</span>
          </div>
        </div>

        <div className="card stack">
          <h2 className="section-title">BTTstock</h2>
          <div className="stack muted">
            <span>Ranking e performance aggregate estratte dal report server-side.</span>
            <span>Focalizzazione su opportunità, rendimento atteso/aggregate performance e visualizzazione premium.</span>
            <span>Esposizione pubblica limitata ai risultati e non alla logica di selezione.</span>
          </div>
        </div>
      </div>

      <div className="card stack">
        <h2 className="section-title">Approccio pubblico</h2>
        <div className="stack muted">
          <span>Pubblico: risultati aggregati e metriche di qualità.</span>
          <span>Privato: motore, parametri, segnali interni, logica di selezione, infrastruttura.</span>
          <span>Design e copy orientati a una percezione fund-tech / institutional style.</span>
        </div>
      </div>
    </div>
  )
}