import './globals.css'
import { Nav } from '@/components/Nav'

export const metadata = {
  title: 'BTT Fusion',
  description: 'BTT Capital + Microcap Bot under one deployable product shell.',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="it">
      <body>
        <Nav />
        <main>{children}</main>
      </body>
    </html>
  )
}
