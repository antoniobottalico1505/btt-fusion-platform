# BTT Fusion Platform

Monorepo pronto per deploy separato:

- `backend/` → FastAPI per Render
- `web/` → Next.js per Vercel

Obiettivo operativo:

- inglobare **BTT Capital** e **Microcap Bot** sotto un unico prodotto;
- mostrare demo controllate senza esporre codice o parametri;
- permettere all'owner di modificare config ed env dal pannello admin;
- gestire trial 24h e abbonamenti Stripe.

## Stato del pacchetto

### Microcap
- esegue `microcap_bot_v4.py` come subprocess server-side;
- usa `config.yaml` privato, non pubblico;
- legge/scrive `bot.db` su storage persistente;
- espone dashboard web con overview, trades, posizioni, watchlist e stato processo;
- admin può start/stop/restartare il motore e modificare config/env.

### BTT Capital
- esegue `btt_capital_bomb_final.py` come job server-side;
- usa preset nascosti configurabili solo da admin;
- salva CSV/HTML per ogni run;
- pubblica ranking e portfolio senza mostrare la logica interna.

### Deploy

#### Backend su Render
- Root dir: `backend`
- Build command: `pip install -r requirements.txt`
- Start command: `bash start.sh`
- Aggiungi un Persistent Disk montato su `backend/storage`

#### Frontend su Vercel
- Root dir: `web`
- Env minima: `NEXT_PUBLIC_API_BASE_URL=https://TUO-BACKEND.onrender.com`

## Primo accesso admin
L'utente admin viene creato/aggiornato automaticamente con:
- `ADMIN_EMAIL`
- `ADMIN_PASSWORD`

## Note importanti
- Il frontend non riceve mai `config.yaml` o le env sensibili.
- Le env sensibili possono essere modificate solo da admin e vengono mascherate in lettura.
- Ho incluso un seed leggero di `bot.db` derivato dal DB caricato, così la dashboard Microcap non parte vuota.
