export type EthereumProvider = {
  request: (args: { method: string; params?: any[] }) => Promise<any>
}

declare global {
  interface Window {
    ethereum?: EthereumProvider
  }
}

export function hasWalletProvider(): boolean {
  return typeof window !== 'undefined' && !!window.ethereum
}

export async function requestWalletConnection() {
  if (!hasWalletProvider()) {
    throw new Error('Wallet non trovato. Installa MetaMask o un wallet EVM compatibile.')
  }

  const accounts: string[] = await window.ethereum!.request({
    method: 'eth_requestAccounts',
  })

  const address = accounts?.[0]

  if (!address) {
    throw new Error('Nessun account wallet selezionato')
  }

  const chainIdHex: string = await window.ethereum!.request({
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

  const signature: string = await window.ethereum!.request({
    method: 'personal_sign',
    params: [message, address],
  })

  return { address, chainId, message, signature }
}

export async function sendWalletTransaction(tx: any): Promise<string> {
  if (!hasWalletProvider()) {
    throw new Error('Wallet non trovato')
  }

  if (!tx?.to || !tx?.data) {
    throw new Error('Transazione non valida')
  }

  const cleanTx: Record<string, string> = {
    from: tx.from,
    to: tx.to,
    data: tx.data,
    value: tx.value || '0x0',
  }

  for (const key of ['gas', 'gasPrice', 'maxFeePerGas', 'maxPriorityFeePerGas']) {
    if (tx[key]) cleanTx[key] = tx[key]
  }

  return await window.ethereum!.request({
    method: 'eth_sendTransaction',
    params: [cleanTx],
  })
}