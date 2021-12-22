import { Account, Commitment, Connection, PublicKey } from '@solana/web3.js'
import { Market } from '@project-serum/serum'
import cors from 'cors'
import express from 'express'
import { Tedis, TedisPool } from 'tedis'
import { URL } from 'url'
import { decodeRecentEvents } from './events'
import { MarketConfig, Trade, TradeSide } from './interfaces'
import { RedisConfig, RedisStore, createRedisStore } from './redis'
import { resolutions, sleep } from './time'
import {
  Config,
  getMarketByBaseSymbolAndKind,
  GroupConfig,
  MangoClient,
  PerpMarketConfig,
  FillEvent,
} from '@blockworks-foundation/mango-client'
import BN from 'bn.js'
import notify from './notify'
import LRUCache from 'lru-cache'
import * as dayjs from 'dayjs'

const redisUrl = new URL(process.env.REDISCLOUD_URL || 'redis://localhost:6379')
const host = redisUrl.hostname
const port = parseInt(redisUrl.port)
let password: string | undefined
if (redisUrl.password !== '') {
  password = redisUrl.password
}

const network = 'mainnet-beta'
const clusterUrl =
  // process.env.RPC_ENDPOINT_URL || 'https://api.mainnet-beta.solana.com'
  process.env.RPC_ENDPOINT_URL || 'https://solana-api.projectserum.com'
const fetchInterval = process.env.INTERVAL ? parseInt(process.env.INTERVAL) : 30

console.log({ clusterUrl, fetchInterval })

const programIdV3 = '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'

const nativeMarketsV3: Record<string, string> = {
  // USDT
  'AAVE/USDT': '6bxuB5N3bt3qW8UnPNLgMMzDq5sEH8pFmYJYGgzvE11V',
  'AKRO/USDT': 'HLvRdctRB48F9yLnu9E24LUTRt89D48Z35yi1HcxayDf',
  'ALEPH/USDT':'Gyp1UGRgbrb6z8t7fpssxEKQgEmcJ4pVnWW3ds2p6ZPY',
  'BTC/USDT': 'C1EuT9VokAKLiW7i2ASnZUvxDoKuKkCpDDeNxAptuNe4', // heavy market
  'CEL/USDT': 'cgani53cMZgYfRMgSrNekJTMaLmccRfspsfTbXWRg7u',
  'CREAM/USDT':'4ztJEvQyryoYagj2uieep3dyPwG2pyEwb2dKXTwmXe82',
  'ETH/USDT': '7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF', // heavy market
  'ETHV/USDT': 'HrgkuJryyKRserkoz7LBFYkASzhXHWp9XA6fRYCA6PHb',
  'FIDA/USDT': 'EbV7pPpEvheLizuYX3gUCvWM8iySbSRAhu2mQ5Vz2Mxf',
  'FRONT/USDT': 'CGC4UgWwqA9PET6Tfx6o6dLv94EK2coVkPtxgNHuBtxj',
  'FTT/USDT': 'Hr3wzG8mZXNHV7TuL6YqtgfVUesCqMxGYCEyP3otywZE', // heavy market
  'HGET/USDT': 'ErQXxiNfJgd4fqQ58PuEw5xY35TZG84tHT6FXf5s4UxY',
  'HNT/USDT': '8FpuMGLtMZ7Wt9ZvyTGuTVwTwwzLYfS5NZWcHxbP1Wuh',
  'HXRO/USDT': '4absuMsgemvdjfkgdLQq1zKEjw3dHBoCWkzKoctndyqd',
  'IETHV/USDT': '5aoLj1bySDhhWjo7cLfT3pF2gqNGd63uEJ9HMSfASESL',
  'KEEP/USDT': 'HEGnaVL5i48ubPBqWAhodnZo8VsSLzEM3Gfc451DnFj9',
  'KIN/USDT': '4nCFQr8sahhhL4XJ7kngGFBmpkmyf3xLzemuMhn6mWTm',
  'LINK/USDT': '3yEZ9ZpXSQapmKjLAGKZEzUNA1rcupJtsDp5mPBWmGZR',
  'LUA/USDT': '35tV8UsHH8FnSAi3YFRrgCu4K9tb883wKnAXpnihot5r',
  'MAPS/USDT': '7cknqHAuGpfVXPtFoJpFvUjJ8wkmyEfbFusmwMfNy3FE',
  'MATH/USDT': '2WghiBkDL2yRhHdvm8CpprrkmfguuQGJTCDfPSudKBAZ',
  'MER/USDT': '6HwcY27nbeb933UkEcxqJejtjWLfNQFWkGCjAVNes6g7',
  'MSRM/USDT': '5nLJ22h1DUfeCfwbFxPYK8zbfbri7nA9bXoDcR8AcJjs',
  'OXY/USDT': 'GKLev6UHeX1KSDCyo2bzyG6wqhByEzDBkmYTxEdmYJgB',
  'RAY/USDT': 'teE55QrL4a4QSfydR9dnHF97jgCfptpuigbb53Lo95g', // heavy market
  'RSR/USDT': 'FcPet5fz9NLdbXwVM6kw2WTHzRAD7mT78UjwTpawd7hJ',
  'SOL/USDT': 'HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1', // heavy market
  'SRM/USDT': 'AtNnsY1AyRERWJ8xCskfz38YdvruWVJQUVXgScC1iPb', // heavy market
  'SUSHI/USDT': '6DgQRTpJTnAYBSShngAVZZDq7j9ogRN1GfSQ3cq9tubW',
  'SWAG/USDT': 'J2XSt77XWim5HwtUM8RUwQvmRXNZsbMKpp5GTKpHafvf',
  'SXP/USDT': '8afKwzHR3wJE7W7Y5hvQkngXh6iTepSZuutRMMy96MjR',
  'TOMO/USDT': 'GnKPri4thaGipzTbp8hhSGSrHgG4F8MFiZVrbRn16iG2',
  'TRYB/USDT': 'AADohBGxvf7bvixs2HKC3dG2RuU3xpZDwaTzYFJThM8U',
  'UBXT/USDT': 'F1T7b6pnR8Pge3qmfNUfW6ZipRDiGpMww6TKTrRU4NiL',
  'UNI/USDT': '2SSnWNrc83otLpfRo792P6P3PESZpdr8cu2r8zCE6bMD',
  'YFI/USDT': '3Xg9Q4VtZhD4bVYJbTfgGWFV5zjE3U7ztSHa938zizte',
  'NOCH/USDT': '9GgM6YwhdzmoY4VtVsrS3jcCx2NSRraCX1EPGvezukAz',
  // USDC
  'AKRO/USDC': '5CZXTTgVZKSzgSA3AFMN5a2f3hmwmmJ6hU8BHTEJ3PX8',
  'ALEPH/USDC': 'GcoKtAmTy5QyuijXSmJKBtFdt99e6Buza18Js7j9AJ6e',
  'ATLAS/USDC': 'Di66GTLsV64JgCCYGVcY21RZ173BHkjJVgPyezNN7P1K',
  'BTC/USDC': 'A8YFbxQYFVqKZaoYJLLUVcQiWP7G2MeEgW5wsAQgMvFw', //heavy market
  'COPE/USDC':'6fc7v3PmjZG9Lk2XTot6BywGyYLkBQuzuFKd4FpCsPxk',
  'CREAM/USDC': '7nZP6feE94eAz9jmfakNJWPwEKaeezuKKC5D1vrnqyo2',
  'CYS/USDC': '6V6y6QFi17QZC9qNRpVp7SaPiHpCTp2skbRQkUyZZXPW',
  'DXL/USDC': 'DYfigimKWc5VhavR4moPBibx9sMcWYVSjVdWvPztBPTa',
  'ETH/USDC': '4tSvZvnbyzHXLMTiFonMyxZoHmFqau1XArcRCVHLZ5gX',
  'FIDA/USDC': 'E14BKBhDWD4EuTkWj1ooZezesGxMW8LPCps4W5PuzZJo',
  'FRONT/USDC': '9Zx1CvxSVdroKMMWf2z8RwrnrLiQZ9VkQ7Ex3syQqdSH',
  'FTT/USDC': '2Pbh1CvRVku1TgewMfycemghf6sU9EyuFDcNXqvRmSxc', // heavy market
  'HGET/USDC': '88vztw7RTN6yJQchVvxrs6oXUDryvpv9iJaFa1EEmg87',
  'HNT/USDC': 'CnUV42ZykoKUnMDdyefv5kP6nDSJf7jFd7WXAecC6LYr',
  'HXRO/USDC': '6Pn1cSiRos3qhBf54uBP9ZQg8x3JTardm1dL3n4p29tA',
  'KEEP/USDC': '3rgacody9SvM88QR83GHaNdEEx4Fe2V2ed5GJp2oeKDr',
  'KIN/USDC': 'Bn6NPyr6UzrFAwC4WmvPvDr2Vm8XSUnFykM2aQroedgn',
  'LIKE/USDC': '3WptgZZu34aiDrLMUiPntTYZGNZ72yT1yxHYxSdbTArX',
  'LINK/USDC': '3hwH1txjJVS8qv588tWrjHfRxdqNjBykM1kMcit484up',
  'LUA/USDC': '4xyWjQ74Eifq17vbue5Ut9xfFNfuVB116tZLEpiZuAn8',
  'MAPS/USDC': '3A8XQRWXC7BjLpgLDDBhQJLT5yPCzS16cGYRKHkKxvYo',
  'MATH/USDC': 'J7cPYBrXVy8Qeki2crZkZavcojf2sMRyQU7nx438Mf8t',
  'MER/USDC': 'G4LcexdCzzJUKZfqyVDQFzpkjhB1JoCNL8Kooxi9nJz5',
  'MNGO/USDC': '3d4rzwpy9iGdCZvgxcu7B1YocYffVLsQXPXkBZKt2zLc',
  'MSOL/USDC': '6oGsL2puUgySccKzn9XA9afqF217LfxP5ocq4B3LWsjy',
  'MSRM/USDC': '4VKLSYdvrQ5ngQrt1d2VS8o4ewvb2MMUZLiejbnGPV33',
  'OXY/USDC': 'GZ3WBFsqntmERPwumFEYgrX2B7J7G11MzNZAy7Hje27X',
  'POLIS/USDC': 'HxFLKUAmAMLz1jtT3hbvCMELwH5H9tpM2QugP8sKyfhW',
  'SBR/USDC': 'HXBi8YBwbh4TXF6PjVw81m8Z3Cc4WBofvauj5SBFdgUs',
  'SLRS/USDC': '2Gx3UfV831BAh8uQv1FKSPKS9yajfeeD8GJ4ZNb2o2YP',
  'SNY/USDC': 'DPfj2jYwPaezkCmUNm5SSYfkrkz8WFqwGLcxDDUsN3gA',
  'SOL/USDC': '9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT', // heavy market
  'SRM/USDC': 'ByRys5tuUWDgL73G8JBAEfkdFf8JWBzPBDHsBVQ5vbQA', // heavy market
  'SUSHI/USDC': 'A1Q9iJDVVS8Wsswr9ajeZugmj64bQVCYLZQLra2TMBMo',
  'SXP/USDC': '4LUro5jaPaTurXK737QAxgJywdhABnFAMQkXX4ZyqqaZ',
  'TOMO/USDC': '8BdpjpSD5n3nk8DQLqPUyTZvVqFu6kcff5bzUX5dqDpy',
  'UBXT/USDC': '2wr3Ab29KNwGhtzr5HaPCyfU1qGJzTUAN4amCLZWaD1H',
  'UNI/USDC': '6JYHjaQBx6AtKSSsizDMwozAEDEZ5KBsSUzH7kRjGJon',
  'USDT/USDC': '77quYg4MGneUdjgXCunt9GgM1usmrxKY31twEy3WHwcS',
  'YFI/USDC': '7qcCo8jqepnjjvB5swP4Afsr3keVBs6gNpBTNubd1Kr2',
  'renBTC/USDC': '74Ciu5yRzhe8TFTHvQuEVbFZJrbnCMRoohBK33NNiPtv',
  'renDOGE/USDC': '5FpKCWYXgHWZ9CdDMHjwxAfqxJLdw2PRXuAmtECkzADk',
  'xCOPE/USDC': '7MpMwArporUHEGW7quUpkPZp5L5cHPs9eKUfKCdaPHq2',
  
}

const symbolsByPk = Object.assign(
  {},
  ...Object.entries(nativeMarketsV3).map(([a, b]) => ({ [b]: a }))
)

async function collectEventQueue(m: MarketConfig, r: RedisConfig) {
  try {
    const store = await createRedisStore(r, m.marketName)
    const marketAddress = new PublicKey(m.marketPk)
    const programKey = new PublicKey(m.programId)
    const connection = new Connection(m.clusterUrl)
    const market = await Market.load(
      connection,
      marketAddress,
      undefined,
      programKey
    )

    async function fetchTrades(
      lastSeqNum?: number
    ): Promise<[Trade[], number]> {
      const now = Date.now()
      const accountInfo = await connection.getAccountInfo(
        market['_decoded'].eventQueue
      )
      if (accountInfo === null) {
        throw new Error(
          `Event queue account for market ${m.marketName} not found`
        )
      }
      const { header, events } = decodeRecentEvents(
        accountInfo.data,
        lastSeqNum
      )
      const takerFills = events.filter(
        (e) => e.eventFlags.fill && !e.eventFlags.maker
      )
      const trades = takerFills
        .map((e) => market.parseFillEvent(e))
        .map((e) => {
          return {
            price: e.price,
            side: e.side === 'buy' ? TradeSide.Buy : TradeSide.Sell,
            size: e.size,
            ts: now,
          }
        })
      /*
    if (trades.length > 0)
      console.log({e: events.map(e => e.eventFlags), takerFills, trades})
    */
      return [trades, header.seqNum]
    }

    async function storeTrades(ts: Trade[]) {
      if (ts.length > 0) {
        console.log(m.marketName, ts.length)
        for (let i = 0; i < ts.length; i += 1) {
          await store.storeTrade(ts[i])
        }
      }
    }

    while (true) {
      try {
        const lastSeqNum = await store.loadNumber('LASTSEQ')
        const [trades, currentSeqNum] = await fetchTrades(lastSeqNum)
        storeTrades(trades)
        store.storeNumber('LASTSEQ', currentSeqNum)
      } catch (e) {
        notify(`collectEventQueue ${m.marketName} ${e.toString()}`)
      }
      await sleep({ Seconds: fetchInterval })
    }
  } catch (e) {
    notify(`collectEventQueue ${m.marketName} ${e.toString()}`)
  }
}

function collectMarketData(programId: string, markets: Record<string, string>) {
  if (process.env.ROLE === 'web') {
    console.warn('ROLE=web detected. Not collecting market data.')
    return
  }

  Object.entries(markets).forEach((e) => {
    const [marketName, marketPk] = e
    const marketConfig = {
      clusterUrl,
      programId,
      marketName,
      marketPk,
    } as MarketConfig
    collectEventQueue(marketConfig, { host, port, password, db: 0 })
  })
}

collectMarketData(programIdV3, nativeMarketsV3)

const groupConfig = Config.ids().getGroup('mainnet', 'mainnet.1') as GroupConfig

async function collectPerpEventQueue(r: RedisConfig, m: PerpMarketConfig) {
  const connection = new Connection(clusterUrl, 'processed' as Commitment)

  const store = await createRedisStore(r, m.name)
  const mangoClient = new MangoClient(connection, groupConfig!.mangoProgramId)
  const mangoGroup = await mangoClient.getMangoGroup(groupConfig!.publicKey)
  const perpMarket = await mangoGroup.loadPerpMarket(
    connection,
    m.marketIndex,
    m.baseDecimals,
    m.quoteDecimals
  )

  async function fetchTrades(lastSeqNum?: BN): Promise<[Trade[], BN]> {
    lastSeqNum ||= new BN(0)
    const now = Date.now()

    const eventQueue = await perpMarket.loadEventQueue(connection)
    const events = eventQueue.eventsSince(lastSeqNum)

    const trades = events
      .map((e) => e.fill)
      .filter((e) => !!e)
      .map((e) => perpMarket.parseFillEvent(e))
      .map((e) => {
        return {
          price: e.price,
          side: e.takerSide === 'buy' ? TradeSide.Buy : TradeSide.Sell,
          size: e.quantity,
          ts: e.timestamp.toNumber() * 1000,
        }
      })

    if (events.length > 0) {
      const last = events[events.length - 1]
      const latestSeqNum =
        last.fill?.seqNum || last.liquidate?.seqNum || last.out?.seqNum
      lastSeqNum = latestSeqNum
    }

    return [trades, lastSeqNum as BN]
  }

  async function storeTrades(ts: Trade[]) {
    if (ts.length > 0) {
      console.log(m.name, ts.length)
      for (let i = 0; i < ts.length; i += 1) {
        await store.storeTrade(ts[i])
      }
    }
  }

  while (true) {
    try {
      const lastSeqNum = await store.loadNumber('LASTSEQ')
      const [trades, currentSeqNum] = await fetchTrades(new BN(lastSeqNum || 0))
      storeTrades(trades)
      store.storeNumber('LASTSEQ', currentSeqNum.toString() as any)
    } catch (err) {
      notify(`collectPerpEventQueue ${m.name} ${err.toString()}`)
    }

    await sleep({ Seconds: fetchInterval })
  }
}

const priceScales: any = {
  'BTC/USDT': 1,
  'ETH/USDT': 10,
  'SOL/USDT': 1000,
  'RAY/USDT': 1000,
  'SRM/USDT': 1000,
  'FTT/USDT': 1000,
  'AAVE/USDT': 1000,
  'AKRO/USDT': 1000000,
  'ALEPH/USDT':10000,
  'CEL/USDT': 10000,
  'CREAM/USDT':1000,
  'ETHV/USDT': 1,
  'FIDA/USDT': 10000,
  'FRONT/USDT': 10000,
  'HGET/USDT': 1000,
  'HNT/USDT': 10000,
  'HXRO/USDT': 10000,
  'IETHV/USDT': 1,
  'KEEP/USDT': 10000,
  'KIN/USDT': 10000000,
  'LINK/USDT': 1000,
  'LUA/USDT': 100000,
  'MAPS/USDT': 10000,
  'MATH/USDT': 100000,
  'MER/USDT': 100000,
  'MSRM/USDT': 10000,
  'OXY/USDT': 10000,
  'RSR/USDT': 100000,
  'SUSHI/USDT': 1000,
  'SWAG/USDT': 1000,
  'SXP/USDT': 1000,
  'TOMO/USDT': 10000,
  'TRYB/USDT': 10000,
  'UBXT/USDT': 1000000,
  'UNI/USDT': 10000,
  'YFI/USDT': 1,
  // USDC
  'BTC/USDC': 1,
  'ETH/USDC': 10,
  'SOL/USDC': 1000,
  'SRM/USDC': 1000,
  'FTT/USDC': 1000,
  'COPE/USDC': 1000,
  'MNGO/USDC': 10000,
  'USDT/USDC': 10000,
  'AKRO/USDC': 1000000,
  'ALEPH/USDC': 10000,
  'ATLAS/USDC': 100000,
  'CREAM/USDC': 100,
  'CYS/USDC': 1000,
  'DXL/USDC': 10000,
  'FIDA/USDC': 10000,
  'FRONT/USDC': 10000,
  'HGET/USDC': 1000,
  'HNT/USDC': 10000,
  'HXRO/USDC': 10000,
  'KEEP/USDC': 10000,
  'KIN/USDC': 10000000,
  'LIKE/USDC': 100000,
  'LINK/USDC': 1000,
  'LUA/USDC': 100000,
  'MAPS/USDC': 10000,
  'MATH/USDC': 100000,
  'MER/USDC': 1000,
  'MSOL/USDC': 1000,
  'MSRM/USDC': 1000000,
  'OXY/USDC': 10000,
  'POLIS/USDC': 10000,
  'SBR/USDC': 10000,
  'SLRS/USDC': 10000,
  'SNY/USDC': 10000,
  'SUSHI/USDC': 1000,
  'SXP/USDC': 1000,
  'TOMO/USDC': 10000,
  'UBXT/USDC': 1000000,
  'UNI/USDC': 10000,
  'YFI/USDC': 1,
  'renBTC/USDC': 100,
  'renDOGE/USDC': 100000,
  'xCOPE/USDC': 100,
  //USDC

}

const cache = new LRUCache<string, Trade[]>(
  parseInt(process.env.CACHE_LIMIT ?? '500')
)

const marketStores = {} as any

Object.keys(priceScales).forEach((marketName) => {
  const conn = new Tedis({
    host,
    port,
    password,
  })

  const store = new RedisStore(conn, marketName)
  marketStores[marketName] = store

  // preload markets
  /* if (['SOL/USDC'].includes(marketName)) {
    for (let i = 1; i < 60; ++i) {
      new Promise( resolve => setTimeout(resolve, 100) );;
      const day = dayjs.default().subtract(i, 'days')
      const key = store.keyForDay(+day)
      store
        .loadTrades(key, cache)
        .then(() => console.log('loaded', key))
        .catch(() => console.error('could not cache', key))
    }
  }*/
  prefetch(store);
})

async function prefetch (store: RedisStore){
  for (let i = 1; i < 60; ++i) {
    new Promise( resolve => setTimeout(resolve, 100) );;
    const day = dayjs.default().subtract(i, 'days')
    const key = store.keyForDay(+day)
    store
      .loadTrades(key, cache)
      .then(() => console.log('loaded', key))
      .catch(() => console.error('could not cache', key))
  }
}

const app = express()

var corsOptions = {
  origin: 'https://nodebunch.finance',
  optionsSuccessStatus: 200 // some legacy browsers (IE11, various SmartTVs) choke on 204
}

app.use(cors(corsOptions))

app.get('/tv/config', async (req, res) => {
  const response = {
    supported_resolutions: Object.keys(resolutions),
    supports_group_request: false,
    supports_marks: false,
    supports_search: true,
    supports_timescale_marks: false,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

app.get('/tv/symbols', async (req, res) => {
  const symbol = req.query.symbol as string
  const response = {
    name: symbol,
    ticker: symbol,
    description: symbol,
    type: 'Spot',
    session: '24x7',
    exchange: 'Nodebunch',
    listed_exchange: 'Nodebunch',
    timezone: 'Etc/UTC',
    has_intraday: true,
    supported_resolutions: Object.keys(resolutions),
    minmov: 1,
    pricescale: priceScales[symbol] || 100,
  }
  res.set('Cache-control', 'public, max-age=360')
  res.send(response)
})

app.get('/tv/history', async (req, res) => {
  // parse
  const marketName = req.query.symbol as string
  const market =
    nativeMarketsV3[marketName] ||
    groupConfig.perpMarkets.find((m) => m.name === marketName)
  const resolution = resolutions[req.query.resolution as string] as number
  let from = parseInt(req.query.from as string) * 1000
  let to = parseInt(req.query.to as string) * 1000

  // validate
  const validSymbol = market != undefined
  const validResolution = resolution != undefined
  const validFrom = true || new Date(from).getFullYear() >= 2021
  if (!(validSymbol && validResolution && validFrom)) {
    const error = { s: 'error', validSymbol, validResolution, validFrom }
    console.error({ marketName, error })
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const store = marketStores[marketName] as RedisStore

    // snap candle boundaries to exact hours
    from = Math.floor(from / resolution) * resolution
    to = Math.ceil(to / resolution) * resolution

    // ensure the candle is at least one period in length
    if (from == to) {
      to += resolution
    }
    const candles = await store.loadCandles(resolution, from, to, cache)
    const response = {
      s: 'ok',
      t: candles.map((c) => c.start / 1000),
      c: candles.map((c) => c.close),
      o: candles.map((c) => c.open),
      h: candles.map((c) => c.high),
      l: candles.map((c) => c.low),
      v: candles.map((c) => c.volume),
    }
    res.set('Cache-control', 'public, max-age=1')
    res.send(response)
    return
  } catch (e) {
    notify(`tv/history ${marketName} ${e.toString()}`)
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

app.get('/trades/address/:marketPk', async (req, res) => {
  // parse
  const marketPk = req.params.marketPk as string
  const marketName =
    symbolsByPk[marketPk] ||
    groupConfig.perpMarkets.find((m) => m.publicKey.toBase58() === marketPk)
      ?.name

  // validate
  const validPk = marketName != undefined
  if (!validPk) {
    const error = { s: 'error', validPk }
    res.status(404).send(error)
    return
  }

  // respond
  try {
    const store = marketStores[marketName] as RedisStore
    const trades = await store.loadRecentTrades()
    const response = {
      success: true,
      data: trades.map((t) => {
        return {
          market: marketName,
          marketAddress: marketPk,
          price: t.price,
          size: t.size,
          side: t.side == TradeSide.Buy ? 'buy' : 'sell',
          time: t.ts,
          orderId: '',
          feeCost: 0,
        }
      }),
    }
    res.set('Cache-control', 'public, max-age=5')
    res.send(response)
    return
  } catch (e) {
    notify(`trades ${marketName} ${e.toString()}`)
    const error = { s: 'error' }
    res.status(500).send(error)
  }
})

const httpPort = parseInt(process.env.PORT || '5000')
app.listen(httpPort)