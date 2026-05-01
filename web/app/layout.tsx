import './globals.css'
import { Nav } from '@/components/Nav'

export const metadata = {
  title: 'BTTcapital',
  description: 'BTTcapital unisce BTTcrypto e BTTstock in un’unica piattaforma premium.',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="it">
      <body>
        <Nav />
        <main>{children}</main>
        <div className="site-copyright">BTTcapital 2026 <span aria-label="copyright">©</span></div>
      </body>
    </html>
  )
}