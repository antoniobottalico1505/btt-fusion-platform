export type EthereumProvider = {
  request: (args: { method: string; params?: any[] }) => Promise<any>
  enable?: () => Promise<any>
  disconnect?: () => Promise<void>
}

declare global {
  interface Window {
    ethereum?: EthereumProvider
  }
}

const BASE_CHAIN_ID = 8453
const BASE_CHAIN_HEX = '0x2105'
const BASE_RPC_URL = 'https://mainnet.base.org'
const SITE_URL = 'https://bttcapital.cc'

let cachedProvider: EthereumProvider | null = null

function getInjectedProvider(): EthereumProvider | null {
  if (typeof window === 'undefined') return null
  return window.ethereum || null
}

export function hasWalletProvider(): boolean {
  return !!getInjectedProvider()
}

export function getMetaMaskMobileLink(path: string = '/wallet'): string {
  const cleanPath = path.startsWith('/') ? path : `/${path}`
  return `https://metamask.app.link/dapp/bttcapital.cc${cleanPath}`
}

async function getWalletConnectProvider(): Promise<EthereumProvider> {
  const projectId = process.env.NEXT_PUBLIC_WALLETCONNECT_PROJECT_ID

  if (!projectId) {
    throw new Error(
      'Wallet non trovato in questo browser. Apri questa pagina nel browser di MetaMask/Rabby/Coinbase Wallet oppure imposta NEXT_PUBLIC_WALLETCONNECT_PROJECT_ID su Vercel.'
    )
  }

  const mod: any = await import('@walletconnect/ethereum-provider')
  const EthereumProvider = mod.default || mod.EthereumProvider

  const provider = await EthereumProvider.init({
    projectId,
    chains: [BASE_CHAIN_ID],
    optionalChains: [1, 10, 56, 137, 42161],
    showQrModal: true,
    rpcMap: {
      [BASE_CHAIN_ID]: BASE_RPC_URL,
    },
    metadata: {
      name: 'BTTcapital',
      description: 'BTTcapital non-custodial wallet execution',
      url: SITE_URL,
      icons: [`${SITE_URL}/favicon.ico`],
    },
  })

  await provider.enable()
  cachedProvider = provider
  return provider
}

async function getProvider(): Promise<EthereumProvider> {
  if (cachedProvider) return cachedProvider

  const injected = getInjectedProvider()
  if (injected) {
    cachedProvider = injected
    return injected
  }

  return await getWalletConnectProvider()
}

function normalizeAddress(value: string): string {
  const address = String(value || '').trim()

  if (!/^0x[a-fA-F0-9]{40}$/.test(address)) {
    throw new Error('Address wallet non valido')
  }

  return address
}

function toPaddedUint256Hex(value: string): string {
  const raw = BigInt(String(value || '0'))

  if (raw <= 0n) {
    throw new Error('Importo non valido')
  }

  return raw.toString(16).padStart(64, '0')
}

function encodeApprove(spender: string, amountRaw: string): string {
  const cleanSpender = normalizeAddress(spender).replace(/^0x/, '').toLowerCase()
  const cleanAmount = toPaddedUint256Hex(amountRaw)

  // approve(address,uint256) = 0x095ea7b3
  return `0x095ea7b3${cleanSpender.padStart(64, '0')}${cleanAmount}`
}

export async function switchToBase(provider?: EthereumProvider) {
  const p = provider || (await getProvider())

  try {
    await p.request({
      method: 'wallet_switchEthereumChain',
      params: [{ chainId: BASE_CHAIN_HEX }],
    })
  } catch (e: any) {
    const code = Number(e?.code || 0)

    if (code !== 4902) {
      throw e
    }

    await p.request({
      method: 'wallet_addEthereumChain',
      params: [
        {
          chainId: BASE_CHAIN_HEX,
          chainName: 'Base',
          nativeCurrency: {
            name: 'Ether',
            symbol: 'ETH',
            decimals: 18,
          },
          rpcUrls: [BASE_RPC_URL],
          blockExplorerUrls: ['https://basescan.org'],
        },
      ],
    })
  }
}

export async function requestWalletConnection() {
  const provider = await getProvider()

  await switchToBase(provider)

  const accounts: string[] = await provider.request({
    method: 'eth_requestAccounts',
  })

  const address = normalizeAddress(accounts?.[0] || '')

  const chainIdHex: string = await provider.request({
    method: 'eth_chainId',
  })

  const chainId = parseInt(chainIdHex, 16)
  const message = [
    'BTTcapital wallet link',
    `Address: ${address}`,
    `Chain ID: ${chainId}`,
    `Timestamp: ${new Date().toISOString()}`,
    '',
    'Firma questo messaggio per collegare il wallet. Non è una transazione e non sposta fondi.',
  ].join('\n')

  const signature: string = await provider.request({
    method: 'personal_sign',
    params: [message, address],
  })

  return { address, chainId, message, signature }
}

export async function sendErc20Approval(params: {
  token: string
  spender: string
  amountRaw: string
  from: string
}): Promise<string> {
  const provider = await getProvider()
  await switchToBase(provider)

  const tx = {
    from: normalizeAddress(params.from),
    to: normalizeAddress(params.token),
    data: encodeApprove(params.spender, params.amountRaw),
    value: '0x0',
  }

  return await provider.request({
    method: 'eth_sendTransaction',
    params: [tx],
  })
}

export async function sendWalletTransaction(tx: any): Promise<string> {
  const provider = await getProvider()
  await switchToBase(provider)

  if (!tx?.to || !tx?.data) {
    throw new Error('Transazione non valida')
  }

  const cleanTx: Record<string, string> = {
    from: normalizeAddress(tx.from),
    to: normalizeAddress(tx.to),
    data: String(tx.data),
    value: tx.value || '0x0',
  }

  for (const key of ['gas', 'gasPrice', 'maxFeePerGas', 'maxPriorityFeePerGas']) {
    if (tx[key]) cleanTx[key] = String(tx[key])
  }

  return await provider.request({
    method: 'eth_sendTransaction',
    params: [cleanTx],
  })
}