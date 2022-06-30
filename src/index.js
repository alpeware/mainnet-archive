/**
 * Copyright (c) Alpeware
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 *
 */

'use strict'

import { ethers } from 'ethers'
import fs from 'fs/promises'
import path from 'path'

const CHAIN_ID = 1
const RPC_URL = 'https://cloudflare-eth.com/v1/mainnet'
const DATA = 'data'
const METADATA_DIR = `${DATA}/metadata`
const METADATA_NAME = `metadata.json`
const METADATA = `${METADATA_DIR}/${METADATA_NAME}`

const MINUTES_MS = 60 * 1000
const RUNTIME_MAX_MS = 15 * MINUTES_MS
const MAX_RETRY = 7


const now = () => Date.now()

const wait = (ms) => new Promise((res) => setTimeout(res, ms))

const callWithRetry = async (fn, depth = 0) => {
  try {
    return await fn()
  } catch(error) {
    if (depth > MAX_RETRY) {
      throw error
    }
    console.debug('retry: ', depth)	
    await wait(2 ** depth * 10)
    return callWithRetry(fn, depth + 1)
  }
}

const dirname = path.resolve()

// create rel dir
const mkdir = async (p) => {
  try {
   const createDir = await fs.mkdir(path.resolve(dirname, p), { recursive: true }) 
  } catch (error) {
    if (error.code !== 'EEXIST' && error.code !== 'EROFS') {
      throw error
    }
  }
}

const spit = async (p, name, obj) => {
  const file = path.resolve(p, name)
  try {
    await mkdir(p)
    const content = JSON.stringify(obj, null, 2)
    return fs.writeFile(file, content)
  } catch (error) {
    console.log(`unable to write file ${file}`)
  }
}

const loadMeta = async () => {
  try {
    const content = await fs.readFile(METADATA)
    return JSON.parse(content)
  } catch(error) {
    return {}
  }
}

const saveMeta = async (height) => {
  const content = { height }
  return spit(METADATA_DIR, METADATA_NAME, content)
}

const connectionInfo = { url: RPC_URL, throttleLimit: 10 } 
const defaultProvider = new ethers.providers.JsonRpcProvider(connectionInfo, CHAIN_ID)

const currentHeight = async (provider) => provider.getBlockNumber()

const block = async (provider, height) =>
    provider.getBlockWithTransactions(height)

const receipts = async (provider, transactions, process) =>
    Promise.all(transactions.map((e) => callWithRetry(provider.getTransactionReceipt.bind(null, e.hash)))
        .then((f) => Object.assign(e, f)).then((g) => process(g)))

const processReceipt = async (receipt) => {
  const { to, from, blockNumber, transactionIndex, hash, logs, nonce } = receipt
  const paddedHeight = blockNumber.toString().padStart(10, '0')
  const paddedIndex = transactionIndex.toString().padStart(4, '0')
  const paddedNonce = nonce.toString().padStart(10, '0')

  const name = `${hash}.json`
  const hashPath = `${DATA}/transactions/hash/${hash}`
  const toPath = `${DATA}/transactions/to/${to}/${paddedHeight}-${paddedIndex}/`
  const fromPath = `${DATA}/transactions/from/${from}/${paddedNonce}`
  const paths = [hashPath, toPath, fromPath]
  const content = receipt
  return Promise.all(paths.map((p) => spit(p.toLowerCase(), name, content)))
}

const processBlock = async (block) => {
  const { hash, number } = block
  const paddedHeight = number.toString().padStart(10, '0')

  const name = `${hash}.json`
  const hashPath = `${DATA}/blocks/hash/${hash}`
  const heightPath = `${DATA}/blocks/height/${paddedHeight}`
  const paths = [hashPath, heightPath]
  const content = block
  return Promise.all(paths.map((p) => spit(p.toLowerCase(), name, content)))
}

const processAtHeight = async (provider, height) => {
  console.debug('Processing block: ', height)
  const currentBlock = await block(provider, height)
  const { transactions } = currentBlock 
  return Promise.all([
    processBlock(currentBlock),
    receipts(provider, transactions, processReceipt)
  ]).then((_) => console.debug('Processed block: ', height))
}

const keepProcessing = (i, max, start) => (i <= max) && (now() - start) < RUNTIME_MAX_MS

const main = async () => {
  const start = now()
  const provider = defaultProvider
  const meta = await loadMeta(METADATA)

  let height = await currentHeight(provider)
  let i = meta.height ? meta.height : height - 5
  while (keepProcessing(i, height, start)) {
    console.debug('Processing time left: ', (RUNTIME_MAX_MS - now() + start)/MINUTES_MS)

    await callWithRetry(processAtHeight.bind(null, provider, i))
    await saveMeta(i)

    height = await currentHeight(provider)
    i++
  }
}

main()
