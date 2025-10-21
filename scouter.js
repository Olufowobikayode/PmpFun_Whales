/**
 * Crypto Alpha Finder Backend Engine - Hardened & Tuned
 *
 * This version implements paginated data fetching from DexScreener to build a
 * large, high-quality pool of candidates for whale discovery.
 * This is the complete, unabridged source code file.
 *
 * Author: Gemini (Refined with User Feedback)
 * Version: 5.6 (Paginated Fetching)
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
const DEX_SCREENER_API_URL = 'https://api.dexscreener.com/latest';
const PUMP_PORTAL_WS_URL = 'wss://pumpportal.fun/api/data';

// --- STRATEGY & VETTING TUNING (Version 5.6 - Paginated Fetching) ---
const PAGINATION_PAGE_COUNT = 40; // NEW: Fetch 10 pages to get ~300 pairs.
const WHALE_DISCOVERY_INTERVAL = 7 * 24 * 60 * 60 * 1000;
const MIN_HOLD_DURATION_SECONDS = 3 * 24 * 60 * 60;
const MIN_LIQUIDITY_USD = 20000;
const MIN_PRICE_CHANGE_H6 = 300;
const MIN_VOLUME_H6_USD = 40000;
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
let redisClient;
let redisPublisher;
let redisSubscriber;
const solanaConnection = new Connection(SOLANA_RPC_URL, { commitment: 'confirmed' });

const whaleSchema = new mongoose.Schema({
    address: { type: String, required: true, unique: true },
    successes: Number,
    discoveredAt: { type: Date, default: Date.now }
});
const Whale = mongoose.model('Whale', whaleSchema);

// --- SERVER SETUP ---
const app = express();
const server = http.createServer(app);
const localWss = new WebSocket.Server({ server });

app.get('/config', (req, res) => {
    res.json({
        featureFlags: { showConfidenceScore: true, showAlertForMediumConfidence: true },
        displayRules: {
            messageOfTheDay: "Alpha tracking is active. Stay sharp!",
            highConfidenceColor: "#29b6f6",
            mediumConfidenceColor: "#ffee58"
        }
    });
});

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
        return {
            isLocked: percentage >= MIN_LIQUIDITY_LOCKED_PERCENT,
            percentage: parseFloat(percentage.toFixed(2))
        };
    } catch (error) {
        console.warn(`[WARN] Could not check liquidity lock for ${pairAddress}: ${error.message}`);
        return { isLocked: false, percentage: 0 };
    }
}

// ==========================================================================================
//                                  MAIN APPLICATION LOGIC
// ==========================================================================================
async function main() {
    console.log("🚀 Initializing Hardened Alpha Finder Engine v5.6 (Paginated Fetching) 🚀");
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

// UPGRADED: Implemented pagination to fetch a large pool of candidates.
async function findSuccessfulTokens() {
    console.log('[PHASE 1] Starting new token discovery pipeline...');
    try {
        // STEP 1: Fetch multiple pages of SOL pairs to build a large candidate list.
        const allPairs = [];
        console.log(`[PHASE 1] Step 1/4: Fetching ${PAGINATION_PAGE_COUNT} pages of data from DexScreener...`);
        for (let page = 1; page <= PAGINATION_PAGE_COUNT; page++) {
            try {
                const { data: searchData } = await axios.get(`${DEX_SCREENER_API_URL}/dex/search?q=SOL&page=${page}`);
                if (searchData.pairs && searchData.pairs.length > 0) {
                    allPairs.push(...searchData.pairs);
                } else {
                    // Stop if a page returns no pairs
                    break;
                }
                // Be a good API citizen and wait briefly between requests
                await new Promise(res => setTimeout(res, 250));
            } catch (pageError) {
                console.warn(`[PHASE 1] Warning: Could not fetch page ${page}. Continuing...`);
            }
        }
        console.log(`[PHASE 1] Step 1/4: Fetched a total of ${allPairs.length} pairs across ${PAGINATION_PAGE_COUNT} pages.`);

        // STEP 2: Pre-filter by our age window.
        const now = Date.now();
        const maxAgeTimestamp = now - (MIN_TOKEN_AGE_DAYS * 24 * 60 * 60 * 1000);
        const minAgeTimestamp = now - (MAX_TOKEN_AGE_DAYS * 24 * 60 * 60 * 1000);
        const ageFilteredPairs = allPairs.filter(p => p.pairCreatedAt > minAgeTimestamp && p.pairCreatedAt < maxAgeTimestamp);
        if (ageFilteredPairs.length === 0) {
            console.log(`[PHASE 1] Step 2/4: No pairs found within the ${MIN_TOKEN_AGE_DAYS}-${MAX_TOKEN_AGE_DAYS} day age window.`);
            return [];
        }
        console.log(`[PHASE 1] Step 2/4: ${ageFilteredPairs.length} pairs match our age criteria.`);

        // STEP 3: Batch query for detailed stats.
        const tokenAddresses = ageFilteredPairs.map(p => p.baseToken.address);
        const detailedPairs = [];
        for (let i = 0; i < tokenAddresses.length; i += 30) {
            const batch = tokenAddresses.slice(i, i + 30);
            try {
                const { data: batchDetails } = await axios.get(`${DEX_SCREENER_API_URL}/dex/tokens/${batch.join(',')}`);
                if (batchDetails.pairs) {
                    const solPairs = batchDetails.pairs.filter(p => p.quoteToken.symbol === 'SOL');
                    detailedPairs.push(...solPairs);
                }
            } catch (batchError) {
                console.warn(`[PHASE 1] A batch query may have failed. Continuing...`);
            }
        }
        console.log(`[PHASE 1] Step 3/4: Fetched detailed stats for ${detailedPairs.length} pairs.`);

        // STEP 4: Apply our final filters.
        const successfulTokens = detailedPairs.filter(p =>
            p.liquidity?.usd > MIN_LIQUIDITY_USD &&
            p.priceChange?.h6 > MIN_PRICE_CHANGE_H6 &&
            p.volume?.h6 > MIN_VOLUME_H6_USD
        );
        console.log(`[PHASE 1] Step 4/4: Found ${successfulTokens.length} high-quality candidates matching the criteria.`);
        return successfulTokens;
    } catch (error) {
        console.error("[ERROR] Failed to fetch successful tokens during pipeline:", error.message);
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
        const { data } = await axios.get(`${DEX_SCREENER_API_URL}/dex/search?q=${tokenAddress}`);
        const pair = data.pairs?.find(p => p.baseToken.address === tokenAddress);
        if (!pair) {
            console.warn(`[WARN] Could not find pair data on DexScreener for ${tokenAddress}`);
            return;
        }
        report.name = pair.baseToken?.name;
        report.symbol = pair.baseToken?.symbol;
        const lpCheck = await checkLiquidityLock(pair.pairAddress);
        report.checks.isLiquidityLocked = lpCheck.isLocked;
        report.liquidityLockedPercentage = lpCheck.percentage;
        if (lpCheck.isLocked) score += 30;
        const mint = await retryWithBackoff(() => getMint(solanaConnection, pk));
        report.checks.isMintRevoked = mint.mintAuthority === null;
        if (report.checks.isMintRevoked) score += 25;
        const [largestAccounts, totalSupplyData] = await Promise.all([
            retryWithBackoff(() => solanaConnection.getTokenLargestAccounts(pk)),
            retryWithBackoff(() => solanaConnection.getTokenSupply(pk))
        ]);
        const topHolderAmount = parseFloat(largestAccounts.value[0]?.uiAmountString || '0');
        const totalSupply = parseFloat(totalSupplyData.value.uiAmountString || '1');
        const concentration = totalSupply > 0 ? (topHolderAmount / totalSupply) * 100 : 100;
        report.checks.isConcentrationLow = concentration <= MAX_HOLDER_CONCENTRATION_PERCENT;
        report.topHolderPercentage = concentration.toFixed(2);
        if (report.checks.isConcentrationLow) score += 25;
        const sigs = await retryWithBackoff(() => solanaConnection.getSignaturesForAddress(pk, { limit: 1 }));
        const creationTx = await retryWithBackoff(() => solanaConnection.getParsedTransaction(sigs[0].signature, { maxSupportedTransactionVersion: 0 }));
        const isBundled = creationTx.transaction.message.instructions.some(ix => KNOWN_BUNDLER_PROGRAMS.has(ix.programId.toBase58()));
        report.checks.notFromBundler = !isBundled;
        if (report.checks.notFromBundler) score += 10;
        report.checks.hasSocials = (pair.info?.socials?.length || 0) > 0;
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
        console.error(`[ERROR] Failed during vetting for ${tokenAddress}:`, error.message);
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