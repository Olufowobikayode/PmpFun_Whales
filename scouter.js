/**
 * Crypto Alpha Finder Backend Engine - Professional API Strategy
 *
 * This version uses a professional data pipeline and features a rewritten,
 * efficient vetting process that correctly utilizes all available data
 * to perform a complete security analysis, including liquidity lock checks.
 * This is the complete, unabridged source code file.
 *
 * Author: Gemini & User Collaboration
 * Version: 7.1 (Efficient Vetting)
 */

require('dotenv').config();

// --- DEPENDENCIES ---
const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const { Connection, PublicKey, clusterApiUrl } = require('@solana/web3.js');
const { getMint } = require('@solana/spl-token');
const mongoose = require('mongoose');
const { createClient } = require('redis');

// --- CONFIGURATION ---
const PORT = process.env.PORT || 8080;
const SOLANA_RPC_URL = process.env.RPC_URL || clusterApiUrl('mainnet-beta');
const MONGO_URI = process.env.MONGO_URI;
const UPSTASH_REDIS_URL = process.env.UPSTASH_REDIS_URL;
const BIRDEYE_API_KEY = process.env.BIRDEYE_API_KEY;
const GECKO_TERMINAL_API_URL = 'https://api.geckoterminal.com/api/v2';
const BIRDEYE_API_URL = 'https://public-api.birdeye.so';
const PUMP_PORTAL_WS_URL = 'wss://pumpportal.fun/api/data';

// --- STRATEGY & VETTING TUNING (Version 7.1) ---
const WHALE_DISCOVERY_INTERVAL = 7 * 24 * 60 * 60 * 1000;
const MIN_HOLD_DURATION_SECONDS = 3 * 24 * 60 * 60;
const MIN_LIQUIDITY_USD = 15000;
const MIN_PRICE_CHANGE_H6 = 200;
const MIN_VOLUME_H6_USD = 50000;
const MAX_TOKEN_AGE_DAYS = 14;
const MIN_TOKEN_AGE_DAYS = 3;
const MIN_CORRELATED_SUCCESSES = 2;
const ALPHA_WHALE_COUNT = 50;
const MULTI_WHALE_CONFIRMATION_COUNT = 2;
const MULTI_WHALE_CONFIRMATION_WINDOW_MS = 30 * 60 * 1000;
const MAX_HOLDER_CONCENTRATION_PERCENT = 20;
const MIN_LIQUIDITY_LOCKED_PERCENT = 95;
const SOLANA_BURN_ADDRESS = '11111111111111111111111111111111';
const KNOWN_BUNDLER_PROGRAMS = new Set(['BUDDYtQp322nPXSv9hS2Smd4LBYf1s7mQYJgTL2t2d2', 'BUNd1Gmtipd8bT5i598acEaVz1sM2gUv1nKjPjr6a5a']);

// --- DATABASE & REDIS CLIENTS ---
let redisClient, redisPublisher, redisSubscriber;
const solanaConnection = new Connection(SOLANA_RPC_URL, { commitment: 'confirmed' });
const whaleSchema = new mongoose.Schema({ address: { type: String, required: true, unique: true }, successes: Number, discoveredAt: { type: Date, default: Date.now } });
const Whale = mongoose.model('Whale', whaleSchema);

// --- SERVER SETUP ---
const app = express();
const server = http.createServer(app);
const localWss = new WebSocket.Server({ server });
app.get('/config', (req, res) => res.json({ featureFlags: { showConfidenceScore: true, showAlertForMediumConfidence: true }, displayRules: { messageOfTheDay: "Alpha tracking is active. Stay sharp!", highConfidenceColor: "#29b6f6", mediumConfidenceColor: "#ffee58" } }));
localWss.on('connection', ws => console.log('[SERVER] Extension client connected.'));
server.listen(PORT, () => console.log(`[SERVER] Alpha Engine listening on port ${PORT}`));

// ==========================================================================================
//                                  UTILITY & HELPER FUNCTIONS
// ==========================================================================================
const retryWithBackoff = async (asyncFn, retries = 3, delay = 1000) => {
    for (let i = 0; i < retries; i++) {
        try { return await asyncFn(); } catch (error) {
            if (i === retries - 1) throw error;
            console.warn(`[RETRY] Operation failed. Retrying in ${delay / 1000}s... (Attempt ${i + 1}/${retries})`);
            await new Promise(res => setTimeout(res, delay));
            delay *= 2;
        }
    }
};

async function checkLiquidityLock(pairAddress) {
    try {
        const lpMintPublicKey = new PublicKey(pairAddress);
        const burnTokenAccounts = await solanaConnection.getTokenAccountsByOwner(new PublicKey(SOLANA_BURN_ADDRESS), { mint: lpMintPublicKey });
        let burnedAmount = 0;
        if (burnTokenAccounts.value.length > 0) {
            const burnAccountInfo = await solanaConnection.getTokenAccountBalance(burnTokenAccounts.value[0].pubkey);
            burnedAmount = parseFloat(burnAccountInfo.value.uiAmountString || '0');
        }
        const totalSupplyData = await retryWithBackoff(() => solanaConnection.getTokenSupply(lpMintPublicKey));
        const totalSupply = parseFloat(totalSupplyData.value.uiAmountString || '1');
        if (totalSupply === 0) return { isLocked: false, percentage: 0 };
        const percentage = (burnedAmount / totalSupply) * 100;
        return { isLocked: percentage >= MIN_LIQUIDITY_LOCKED_PERCENT, percentage: parseFloat(percentage.toFixed(2)) };
    } catch (error) {
        console.warn(`[WARN] Could not check liquidity lock for ${pairAddress}: ${error.message}`);
        return { isLocked: false, percentage: 0 };
    }
}

// ==========================================================================================
//                                  MAIN APPLICATION LOGIC
// ==========================================================================================
async function main() {
    console.log("🚀 Initializing Alpha Finder Engine v7.1 (Efficient Vetting) 🚀");
    await connectToServices();
    await runWhaleDiscoveryCycle();
    setInterval(runWhaleDiscoveryCycle, WHALE_DISCOVERY_INTERVAL);
    connectToPumpPortal();
}

async function connectToServices() {
    try {
        await mongoose.connect(MONGO_URI);
        console.log('[DB] ✅ Connected to MongoDB Atlas.');
    } catch (error) { console.error('[DB] ❌ Could not connect to MongoDB Atlas.', error); process.exit(1); }
    try {
        redisClient = createClient({ url: UPSTASH_REDIS_URL });
        redisClient.on('error', (err) => console.error('[REDIS CLIENT ERROR]', err));
        redisPublisher = redisClient.duplicate();
        redisPublisher.on('error', (err) => console.error('[REDIS PUBLISHER ERROR]', err));
        redisSubscriber = redisClient.duplicate();
        redisSubscriber.on('error', (err) => console.error('[REDIS SUBSCRIBER ERROR]', err));
        await Promise.all([redisClient.connect(), redisPublisher.connect(), redisSubscriber.connect()]);
        console.log('[REDIS] ✅ Connected to Upstash Redis.');
        await redisSubscriber.subscribe('alpha-alerts', (message) => {
            const report = JSON.parse(message);
            console.log(`[PUBSUB] Received alert for ${report.symbol} from Redis. Broadcasting to ${localWss.clients.size} clients.`);
            localWss.clients.forEach(client => {
                if (client.readyState === WebSocket.OPEN) client.send(JSON.stringify({ type: 'NEW_ALPHA_ALERT', data: report }));
            });
        });
    } catch (error) { console.error('[REDIS] ❌ Could not connect to Upstash Redis.', error); process.exit(1); }
}

// ==========================================================================================
// PHASE 1: PERIODIC & CORRELATED WHALE DISCOVERY
// ==========================================================================================
async function runWhaleDiscoveryCycle() {
    console.log("\n[PHASE 1] Starting new weekly whale discovery cycle...");
    try {
        const successfulTokens = await findSuccessfulTokens();
        if (successfulTokens.length === 0) {
            console.log("[PHASE 1] No successful tokens found in this cycle. Waiting for the next run.");
            console.log("[PHASE 1] Whale discovery cycle finished.");
            return;
        }
        const tokenToPatientBuyers = new Map();
        for (const token of successfulTokens) {
            console.log(`[PHASE 1] Analyzing token: ${token.baseToken.symbol} (${token.baseToken.address})`);
            const patientBuyers = await retryWithBackoff(() => findAndFilterEarlyBuyers(token.baseToken.address));
            if (patientBuyers.size > 0) {
                tokenToPatientBuyers.set(token.baseToken.address, Array.from(patientBuyers));
                console.log(`[PHASE 1]  -> Found ${patientBuyers.size} patient early buyers who held for >3 days.`);
            }
        }
        const buyerSuccessCounts = new Map();
        for (const buyers of tokenToPatientBuyers.values()) {
            for (const buyer of buyers) {
                buyerSuccessCounts.set(buyer, (buyerSuccessCounts.get(buyer) || 0) + 1);
            }
        }
        const correlatedWhalesData = Array.from(buyerSuccessCounts.entries())
            .filter(([_, count]) => count >= MIN_CORRELATED_SUCCESSES)
            .map(([address, successes]) => ({ address, successes }));
        correlatedWhalesData.sort((a, b) => b.successes - a.successes);
        const newAlphaWhalesData = correlatedWhalesData.slice(0, ALPHA_WHALE_COUNT);
        if (newAlphaWhalesData.length > 0) {
            console.log(`🏆 [PHASE 1] Top ${newAlphaWhalesData.length} correlated whales found. Updating database... 🏆`);
            const currentWhales = (await Whale.find({}).select('address')).map(w => w.address);
            const session = await mongoose.startSession();
            session.startTransaction();
            try {
                await Whale.deleteMany({}, { session });
                await Whale.insertMany(newAlphaWhalesData, { session });
                await session.commitTransaction();
                console.log("[DB] Whale list successfully updated in MongoDB.");
                const newWhales = newAlphaWhalesData.map(w => w.address);
                if (typeof pumpPortalWs !== 'undefined' && pumpPortalWs) {
                    updatePumpPortalSubscriptions(currentWhales, newWhales, pumpPortalWs);
                }
            } catch (error) { await session.abortTransaction(); throw error; } finally { session.endSession(); }
        } else {
            console.log("[PHASE 1] No new correlated whales found meeting the criteria.");
        }
    } catch (error) {
        console.error("[ERROR] Critical error during whale discovery cycle:", error);
    }
    console.log("[PHASE 1] Whale discovery cycle finished.");
}

async function findSuccessfulTokens() {
    console.log('[PHASE 1] Starting new token discovery pipeline...');
    try {
        const { data: geckoData } = await axios.get(`${GECKO_TERMINAL_API_URL}/networks/solana/new_pools`);
        const newPools = geckoData.data;
        if (!newPools || newPools.length === 0) {
            console.log(`[PHASE 1] Step 1/4: GeckoTerminal returned no new pools.`);
            return [];
        }
        console.log(`[PHASE 1] Step 1/4: Fetched ${newPools.length} new pools from GeckoTerminal.`);
        const now = Date.now();
        const maxAgeTimestamp = now - (MIN_TOKEN_AGE_DAYS * 24 * 60 * 60 * 1000);
        const minAgeTimestamp = now - (MAX_TOKEN_AGE_DAYS * 24 * 60 * 60 * 1000);
        const ageFilteredPools = newPools.filter(pool => {
            const creationTime = new Date(pool.attributes.pool_created_at).getTime();
            return creationTime > minAgeTimestamp && creationTime < maxAgeTimestamp;
        });
        if (ageFilteredPools.length === 0) {
            console.log(`[PHASE 1] Step 2/4: No pools found within the ${MIN_TOKEN_AGE_DAYS}-${MAX_TOKEN_AGE_DAYS} day age window.`);
            return [];
        }
        console.log(`[PHASE 1] Step 2/4: ${ageFilteredPools.length} pools match our age criteria.`);
        const tokenAddresses = ageFilteredPools.map(pool => pool.relationships.base_token.data.id.split('_')[1]);
        const { data: birdeyeData } = await axios.get(`${BIRDEYE_API_URL}/defi/multi_price?list_address=${tokenAddresses.join(',')}`, {
            headers: { 'X-API-KEY': BIRDEYE_API_KEY }
        });
        if (!birdeyeData.data || Object.keys(birdeyeData.data).length === 0) {
            console.log(`[PHASE 1] Step 3/4: Birdeye returned no performance data for the candidates.`);
            return [];
        }
        console.log(`[PHASE 1] Step 3/4: Fetched detailed stats for ${Object.keys(birdeyeData.data).length} tokens from Birdeye.`);
        const successfulTokens = [];
        for (const tokenAddress in birdeyeData.data) {
            const tokenData = birdeyeData.data[tokenAddress];
            const poolInfo = ageFilteredPools.find(p => p.relationships.base_token.data.id.endsWith(tokenAddress));
            if (!tokenData || !poolInfo) continue;
            const priceChangeH6 = (tokenData.priceChange6h || 0); // Birdeye price change is a multiplier, not percentage
            if (
                tokenData.liquidity > MIN_LIQUIDITY_USD &&
                (priceChangeH6 * 100) > MIN_PRICE_CHANGE_H6 &&
                tokenData.v6hUSD > MIN_VOLUME_H6_USD
            ) {
                successfulTokens.push({
                    pairAddress: poolInfo.id.split('_')[1],
                    baseToken: { address: tokenAddress, name: poolInfo.attributes.name, symbol: tokenData.symbol },
                    quoteToken: { symbol: 'SOL' },
                    liquidity: { usd: tokenData.liquidity },
                    priceChange: { h6: priceChangeH6 * 100 },
                    volume: { h6: tokenData.v6hUSD },
                    pairCreatedAt: new Date(poolInfo.attributes.pool_created_at).getTime(),
                    info: {} 
                });
            }
        }
        console.log(`[PHASE 1] Step 4/4: Found ${successfulTokens.length} high-quality candidates matching all criteria.`);
        return successfulTokens;
    } catch (error) {
        console.error("[ERROR] Failed to fetch successful tokens during pipeline:", error.response ? error.response.data : error.message);
        return [];
    }
}

async function findAndFilterEarlyBuyers(tokenMintAddress) {
    try {
        const pk = new PublicKey(tokenMintAddress);
        const signatures = await solanaConnection.getSignaturesForAddress(pk, { limit: 1000 });
        if (!signatures || signatures.length === 0) return new Set();
        const signatureStrings = signatures.map(s => s.signature);
        const transactions = (await solanaConnection.getParsedTransactions(signatureStrings, { maxSupportedTransactionVersion: 0 })).filter(Boolean);
        const buyerTimestamps = new Map();
        for (const tx of transactions.reverse()) {
            if (!tx.blockTime || tx.meta.err) continue;
            for (const ix of tx.transaction.message.instructions) {
                if (!ix.parsed || (ix.parsed.type !== 'transfer' && ix.parsed.type !== 'transferChecked')) continue;
                if (!ix.parsed.info.mint || ix.parsed.info.mint !== tokenMintAddress) continue;
                const { source, destination } = ix.parsed.info;
                if (!buyerTimestamps.has(destination)) buyerTimestamps.set(destination, { firstReceiveTime: tx.blockTime, firstSendTime: null });
                const sourceEntry = buyerTimestamps.get(source);
                if (sourceEntry && !sourceEntry.firstSendTime) sourceEntry.firstSendTime = tx.blockTime;
            }
        }
        const patientBuyers = new Set();
        for (const [buyer, times] of buyerTimestamps.entries()) {
            if (times.firstSendTime) {
                if (times.firstSendTime - times.firstReceiveTime >= MIN_HOLD_DURATION_SECONDS) patientBuyers.add(buyer);
            } else { patientBuyers.add(buyer); }
        }
        return patientBuyers;
    } catch (error) {
        console.warn(`[WARN] Could not fully process early buyers for ${tokenMintAddress}: ${error.message}`);
        return new Set();
    }
}

// ==========================================================================================
// PHASE 2: DYNAMIC REAL-TIME MONITORING
// ==========================================================================================
let pumpPortalWs = null;
async function connectToPumpPortal() {
    console.log("[PHASE 2] Connecting to PumpPortal WebSocket...");
    pumpPortalWs = new WebSocket(PUMP_PORTAL_WS_URL);
    pumpPortalWs.on('open', async () => {
        console.log("[PHASE 2] ✅ Connected to PumpPortal.");
        const currentWhales = (await Whale.find({}).select('address')).map(w => w.address);
        if (currentWhales.length > 0) {
            console.log(`[PHASE 2] Subscribing to ${currentWhales.length} alpha whales.`);
            updatePumpPortalSubscriptions([], currentWhales, pumpPortalWs);
        }
    });
    pumpPortalWs.on('message', data => handlePumpPortalMessage(data));
    pumpPortalWs.on('close', () => {
        console.log("[PHASE 2] PumpPortal connection closed. Reconnecting in 5 seconds...");
        pumpPortalWs = null;
        setTimeout(connectToPumpPortal, 5000);
    });
    pumpPortalWs.on('error', (e) => console.error("[ERROR] PumpPortal WebSocket error:", e.message));
}

async function handlePumpPortalMessage(data) {
    try {
        const message = JSON.parse(data.toString());
        if (message.user && message.isBuy) {
            const isAlphaWhale = await Whale.findOne({ address: message.user });
            if (isAlphaWhale) {
                console.log(`[PHASE 2] Detected buy from Alpha Whale: ${message.user}`);
                handleAlphaWhaleTrade(message);
            }
        }
    } catch (error) { /* Ignore non-JSON messages */ }
}

function updatePumpPortalSubscriptions(oldWhales, newWhales, ws) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    console.log("[PHASE 2] 🔄 Updating PumpPortal subscriptions...");
    if (oldWhales && oldWhales.length > 0) {
        ws.send(JSON.stringify({ method: "unsubscribeAccountTrade", keys: oldWhales }));
    }
    if (newWhales && newWhales.length > 0) {
        ws.send(JSON.stringify({ method: "subscribeAccountTrade", keys: newWhales }));
    }
}

// ==========================================================================================
// PHASE 3: MULTI-WHALE CONFIRMATION & VETTING
// ==========================================================================================
async function handleAlphaWhaleTrade(tradeData) {
    const { user: whaleAddress, mint: tokenAddress } = tradeData;
    const redisKey = `pending:${tokenAddress}`;
    try {
        await redisClient.sAdd(redisKey, whaleAddress);
        const buyerCount = await redisClient.sCard(redisKey);
        console.log(`[PHASE 3] Whale #${buyerCount} bought ${tokenAddress}. Waiting for ${MULTI_WHALE_CONFIRMATION_COUNT} total.`);
        if (buyerCount >= MULTI_WHALE_CONFIRMATION_COUNT) {
            await redisClient.del(redisKey);
            vetToken(tokenAddress);
        } else if (buyerCount === 1) {
            await redisClient.expire(redisKey, Math.floor(MULTI_WHALE_CONFIRMATION_WINDOW_MS / 1000));
        }
    } catch (error) {
        console.error(`[REDIS] Error handling trade for ${tokenAddress}:`, error);
    }
}

async function vetToken(tokenAddress) {
    console.log(`[PHASE 3] 🔬 Vetting token: ${tokenAddress}`);
    try {
        const pk = new PublicKey(tokenAddress);
        const report = { tokenAddress, checks: {} };
        let score = 0;
        const [birdeyeOverviewData, onChainData] = await Promise.all([
            axios.get(`${BIRDEYE_API_URL}/defi/token_overview?address=${tokenAddress}`, { headers: { 'X-API-KEY': BIRDEYE_API_KEY } }),
            Promise.all([
                retryWithBackoff(() => getMint(solanaConnection, pk)),
                retryWithBackoff(() => solanaConnection.getTokenLargestAccounts(pk)),
                retryWithBackoff(() => solanaConnection.getTokenSupply(pk)),
                retryWithBackoff(() => solanaConnection.getSignaturesForAddress(pk, { limit: 1 }))
            ])
        ]);
        const tokenInfo = birdeyeOverviewData.data.data;
        if (!tokenInfo) {
             console.warn(`[WARN] Could not find token overview on Birdeye for ${tokenAddress}`);
             return;
        }
        report.name = tokenInfo.name;
        report.symbol = tokenInfo.symbol;
        const [mint, largestAccounts, totalSupplyData, sigs] = onChainData;

        // Use a different Birdeye endpoint to find the pair address efficiently
        const { data: pairsData } = await axios.get(`${BIRDEYE_API_URL}/defi/token_list?sort_by=v24hUSD&sort_type=desc`);
        const primaryPair = pairsData.data.tokens.find(t => t.address === tokenAddress);
        const pairAddress = primaryPair ? primaryPair.liquidityPool : null;
        
        if (pairAddress) {
            const lpCheck = await checkLiquidityLock(pairAddress);
            report.checks.isLiquidityLocked = lpCheck.isLocked;
            report.liquidityLockedPercentage = lpCheck.percentage;
            if (lpCheck.isLocked) score += 30;
        } else {
            report.checks.isLiquidityLocked = 'Unknown';
            console.warn(`[WARN] Could not determine primary pair address for ${tokenAddress} to check LP lock.`);
        }
        report.checks.isMintRevoked = mint.mintAuthority === null;
        if (report.checks.isMintRevoked) score += 25;
        const topHolderAmount = parseFloat(largestAccounts.value[0]?.uiAmountString || '0');
        const totalSupply = parseFloat(totalSupplyData.value.uiAmountString || '1');
        const concentration = totalSupply > 0 ? (topHolderAmount / totalSupply) * 100 : 100;
        report.checks.isConcentrationLow = concentration <= MAX_HOLDER_CONCENTRATION_PERCENT;
        report.topHolderPercentage = concentration.toFixed(2);
        if (report.checks.isConcentrationLow) score += 25;
        const creationTx = await retryWithBackoff(() => solanaConnection.getParsedTransaction(sigs[0].signature, { maxSupportedTransactionVersion: 0 }));
        const isBundled = creationTx.transaction.message.instructions.some(ix => KNOWN_BUNDLER_PROGRAMS.has(ix.programId.toBase58()));
        report.checks.notFromBundler = !isBundled;
        if (report.checks.notFromBundler) score += 10;
        report.checks.hasSocials = tokenInfo.extensions && (tokenInfo.extensions.twitter || tokenInfo.extensions.website);
        if (report.checks.hasSocials) score += 10;
        report.vettingScore = score;
        if (score >= 80) report.confidence = 'High';
        else if (score >= 50) report.confidence = 'Medium';
        else report.confidence = 'Low';
        console.log(`[VETTING] ✅ Vetting Complete for ${report.symbol || tokenAddress}. Score: ${score}/100`);
        if (report.confidence !== 'Low') {
            broadcastAlert(report);
        } else {
            console.log(`[VETTING] 📉 Score too low. Suppressing alert.`);
        }
    } catch (error) {
        console.error(`[ERROR] Failed during vetting for ${tokenAddress}:`, error.response ? error.response.data : error.message);
    }
}

// ==========================================================================================
// PHASE 4: ALERTING THE EXTENSION
// ==========================================================================================
function broadcastAlert(report) {
    console.log(`[PHASE 4] 📡 Publishing '${report.confidence}' confidence alert for ${report.symbol} to Redis channel.`);
    redisPublisher.publish('alpha-alerts', JSON.stringify(report));
}

// --- INITIATE THE APPLICATION ---
main();