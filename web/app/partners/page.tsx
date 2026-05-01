export default function PartnersPage() {
  return (
    <div className="shell section stack">
      <div>
        <h1 className="section-title">For Partners</h1>
        <p className="section-sub">
          BTTcapital è pensato per una struttura hosted-only, partner-ready e orientata alla tutela totale del know-how.
        </p>
      </div>

      <div className="grid-2">
        <div className="card stack">
          <h2 className="section-title">Modello corretto</h2>
          <div className="stack muted">
            <span>Motore privato mantenuto esclusivamente da BTTcapital.</span>
            <span>Partner autorizzato come perimetro commerciale/regolato verso il cliente finale.</span>
            <span>Licensing, recurring B2B e possibile white-label senza trasferimento del codice.</span>
            <span>Struttura adatta a setup fee, minimum guarantee e revenue share B2B.</span>
          </div>
        </div>

        <div className="card stack">
          <h2 className="section-title">Cosa non viene mai ceduto</h2>
          <div className="stack muted">
            <span>Nessun sorgente.</span>
            <span>Nessun accesso repository.</span>
            <span>Nessun accesso shell/server/DB.</span>
            <span>Nessuna installazione on-premise.</span>
            <span>Nessuna documentazione che riveli la logica proprietaria.</span>
          </div>
        </div>
      </div>

      <div className="card stack">
        <h2 className="section-title">Principi contrattuali</h2>
        <div className="stack muted">
          <span>Hosted-only black box.</span>
          <span>No IP transfer.</span>
          <span>No reverse engineering.</span>
          <span>No sublicenza non autorizzata.</span>
          <span>No source escrow salvo accordi eccezionali e fortemente onerosi.</span>
          <span>Accesso limitato a dashboard, reportistica e API controllata.</span>
        </div>
      </div>
    </div>
  )
}