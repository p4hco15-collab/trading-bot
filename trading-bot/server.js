const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

// Serve the frontend dashboard
app.use(express.static(path.join(__dirname, 'public')));

// --- BOT STATE & LOGIC ---
const PAIRS = {
    major: [
      { symbol:"btcusdt",  display:"BTCUSDT",  tv:"BINANCE:BTCUSDT.P"  },
      { symbol:"ethusdt",  display:"ETHUSDT",  tv:"BINANCE:ETHUSDT.P"  },
      { symbol:"bnbusdt",  display:"BNBUSDT",  tv:"BINANCE:BNBUSDT.P"  },
      { symbol:"solusdt",  display:"SOLUSDT",  tv:"BINANCE:SOLUSDT.P"  },
      { symbol:"xrpusdt",  display:"XRPUSDT",  tv:"BINANCE:XRPUSDT.P"  },
    ],
    alt: [
      { symbol:"hypeusdt", display:"HYPEUSDT", tv:"BINANCE:HYPEUSDT.P" },
      { symbol:"suiusdt",  display:"SUIUSDT",  tv:"BINANCE:SUIUSDT.P"  },
      { symbol:"aptusdt",  display:"APTUSDT",  tv:"BINANCE:APTUSDT.P"  },
      { symbol:"arbusdt",  display:"ARBUSDT",  tv:"BINANCE:ARBUSDT.P"  },
      { symbol:"opusdt",   display:"OPUSDT",   tv:"BINANCE:OPUSDT.P"   },
    ],
    meme: [
      { symbol:"dogeusdt", display:"DOGEUSDT", tv:"BINANCE:DOGEUSDT.P" },
      { symbol:"shibusdt", display:"SHIBUSDT", tv:"BINANCE:SHIBUSDT.P" },
      { symbol:"pepeusdt", display:"PEPEUSDT", tv:"BINANCE:PEPEUSDT.P" },
      { symbol:"flokiusdt",display:"FLOKIUSDT",tv:"BINANCE:FLOKIUSDT.P"},
      { symbol:"bonkusdt", display:"BONKUSDT", tv:"BINANCE:BONKUSDT.P" },
    ]
};

const LEVERAGE = 2;
const FIXED_STAKE = 100.00;
const WIN_PCT = 0.06;
const LOSS_PCT = 0.02;
const STARTING_BALANCE = 100.00;
const THINK_SECONDS = 60;
const round2 = (n) => Math.round(n * 100) / 100;

let state = {
    botActive: true,
    trailingActive: false,
    balance: STARTING_BALANCE,
    currentPair: PAIRS.alt[0],
    lastPrice: 0,
    prevTickPrice: 0,
    position: null,
    tradesCount: 0,
    wins: 0,
    losses: 0,
    tradeHistoryArr: [],
    thinking: false,
    thinkSecondsLeft: THINK_SECONDS,
    thinkBiasSide: null,
    structureText: "Bot initializing..."
};

let ws = null;
let thinkTimer = null;

// --- AI STRATEGY FUNCTIONS ---
function computeEMA(values, period){ if(values.length<period) return null; const k=2/(period+1); let ema=values.slice(0,period).reduce((a,b)=>a+b,0)/period; for(let i=period;i<values.length;i++){ema=values[i]*k+ema*(1-k);} return ema; }
function computeRSI(closes, period=14){ if(closes.length<period+1) return null; let gains=0,losses=0; for(let i=1;i<=period;i++){const diff=closes[i]-closes[i-1]; if(diff>=0) gains+=diff; else losses-=diff;} let avgGain=gains/period; let avgLoss=losses/period; for(let i=period+1;i<closes.length;i++){const diff=closes[i]-closes[i-1]; if(diff>=0){avgGain=(avgGain*(period-1)+diff)/period; avgLoss=(avgLoss*(period-1))/period;}else{avgGain=(avgGain*(period-1))/period; avgLoss=(avgLoss*(period-1)-diff)/period;}} if(avgLoss===0) return 100; const rs=avgGain/avgLoss; return 100-(100/(1+rs)); }
function computeBollingerBands(closes, period=20, mult=2){ if(closes.length<period) return null; const slice=closes.slice(-period); const mean=slice.reduce((a,b)=>a+b,0)/period; const variance=slice.reduce((a,b)=>a+Math.pow(b-mean,2),0)/period; const stdDev=Math.sqrt(variance); return {middle:mean, upper:mean+(mult*stdDev), lower:mean-(mult*stdDev)}; }
function computeBias(closes){ const ema9=computeEMA(closes,9); const ema21=computeEMA(closes,21); const ema50=computeEMA(closes,50); const rsi=computeRSI(closes,14); const bb=computeBollingerBands(closes,20,2); const lastPrice=closes[closes.length-1]; let bullish=0,bearish=0; if(ema9>ema21&&ema21>ema50) bullish+=2; if(ema9<ema21&&ema21<ema50) bearish+=2; if(rsi<40) bullish+=1; if(rsi>60) bearish+=1; if(lastPrice<bb.lower) bullish+=2; if(lastPrice>bb.upper) bearish+=2; if(bullish>bearish) return "LONG"; if(bearish>bullish) return "SHORT"; return ema9>ema21?"LONG":"SHORT"; }

async function fetchKlines(symbol, interval='15m', limit=100){
    const futUrl = `https://fapi.binance.com/fapi/v1/klines?symbol=${symbol.toUpperCase()}&interval=${interval}&limit=${limit}`;
    const options = {
        headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            'Accept': 'application/json'
        }
    };
    try {
        const res = await fetch(futUrl, options);
        if(!res.ok) throw new Error("Futures API failed");
        const data = await res.json();
        return data.map(row => ({ o: parseFloat(row[1]), h: parseFloat(row[2]), l: parseFloat(row[3]), c: parseFloat(row[4]) }));
    } catch(e) {
        const spotUrl = `https://api.binance.com/api/v3/klines?symbol=${symbol.toUpperCase()}&interval=${interval}&limit=${limit}`;
        const spotRes = await fetch(spotUrl, options);
        if(!spotRes.ok) throw new Error("Both APIs failed");
        const spotData = await spotRes.json();
        return spotData.map(row => ({ o: parseFloat(row[1]), h: parseFloat(row[2]), l: parseFloat(row[3]), c: parseFloat(row[4]) }));
    }
}

// --- BOT CORE LOGIC ---
function broadcastState() {
    io.emit('state', state);
}

async function analyzeMarketStructure() {
    if (!state.botActive) return null;
    try {
        state.structureText = "AI scanning 100 candles on 15m timeframe...";
        broadcastState();
        
        const klines = await fetchKlines(state.currentPair.symbol, '15m', 100);
        if (klines.length < 50) { state.structureText = "Insufficient data"; broadcastState(); return null; }

        const closes = klines.map(k => k.c);
        const bias = computeBias(closes);
        const ema9 = computeEMA(closes, 9);
        const ema21 = computeEMA(closes, 21);
        const rsi = computeRSI(closes, 14);
        const bb = computeBollingerBands(closes, 20, 2);

        if (bias === "LONG") {
            state.structureText = `🟢 AI STRATEGY (15m): Bullish confluence. Trend: ${ema9 > ema21 ? 'Up' : 'Down'}. RSI: ${rsi.toFixed(2)}. Executing LONG.`;
        } else {
            state.structureText = `🔴 AI STRATEGY (15m): Bearish confluence. Trend: ${ema9 < ema21 ? 'Down' : 'Up'}. RSI: ${rsi.toFixed(2)}. Executing SHORT.`;
        }
        state.thinkBiasSide = bias;
        broadcastState();
        return bias;
    } catch (e) {
        state.structureText = "AI analysis failed. Retrying...";
        broadcastState();
        return null;
    }
}

function enterThinkingMode() {
    if (state.position || !state.botActive) return;
    state.thinking = true;
    state.thinkSecondsLeft = THINK_SECONDS;
    broadcastState();

    if (thinkTimer) clearInterval(thinkTimer);
    thinkTimer = setInterval(() => {
        if (!state.thinking || !state.botActive) { clearInterval(thinkTimer); return; }
        state.thinkSecondsLeft--;
        broadcastState();
        if (state.thinkSecondsLeft <= 0) {
            clearInterval(thinkTimer);
            finishThinking();
        }
    }, 1000);
    
    analyzeMarketStructure();
}

function finishThinking() {
    state.thinking = false;
    if (state.lastPrice === 0) { enterThinkingMode(); return; }
    const side = state.thinkBiasSide || "LONG";
    openPosition(state.lastPrice, side);
    broadcastState();
}

function openPosition(entryPrice, side) {
    const stakedAmount = FIXED_STAKE;
    const notional = stakedAmount * LEVERAGE;
    const roundedEntry = round2(entryPrice);
    const qty = notional / roundedEntry;
    const tpDollars = stakedAmount * WIN_PCT;
    const slDollars = stakedAmount * LOSS_PCT;
    const tpDelta = tpDollars / qty;
    const slDelta = slDollars / qty;
    const tpPrice = round2(side === "LONG" ? roundedEntry + tpDelta : roundedEntry - tpDelta);
    const slPrice = round2(side === "LONG" ? roundedEntry - slDelta : roundedEntry + slDelta);

    state.position = { side, entry: roundedEntry, qty, tpPrice, slPrice, initialSlPrice: slPrice, stakedAmount, tpDollars, slDollars, maxPnl: 0 };
    broadcastState();
}

function checkExit(price) {
    if (!state.position) return;
    const { side, tpPrice, slPrice } = state.position;
    if (side === "LONG") {
        if (price >= tpPrice) settleTrade(true, price);
        else if (price <= slPrice) settleTrade(false, price);
    } else {
        if (price <= tpPrice) settleTrade(true, price);
        else if (price >= slPrice) settleTrade(false, price);
    }
}

function settleTrade(won, exitPrice) {
    if (!state.position) return;
    const closed = state.position;
    if (won) { state.balance += closed.tpDollars; state.wins++; } 
    else { state.balance -= closed.slDollars; state.losses++; }
    state.tradesCount++;

    const amt = won ? closed.tpDollars : closed.slDollars;
    state.tradeHistoryArr.push({ side: closed.side, entry: closed.entry, exit: exitPrice, tp: closed.tpPrice, sl: closed.initialSlPrice, won: won, amt: amt });
    if(state.tradeHistoryArr.length > 50) state.tradeHistoryArr.shift(); // Keep last 50 trades

    state.position = null;
    broadcastState();
    enterThinkingMode();
}

function handleTick(rawPrice) {
    const roundedPrice = round2(rawPrice);
    state.prevTickPrice = state.lastPrice;
    state.lastPrice = roundedPrice;

    if (!state.botActive) { broadcastState(); return; }

    if (state.position) {
        const { side, entry, qty, stakedAmount, tpDollars, slDollars } = state.position;
        let pnl = side === "LONG" ? (roundedPrice - entry) * qty : (entry - roundedPrice) * qty;
        
        if (state.trailingActive) {
            if (pnl > state.position.maxPnl) state.position.maxPnl = pnl;
            if (state.position.maxPnl >= 4 && state.position.slDollars !== 2) {
                state.position.slDollars = 2;
                state.position.slPrice = round2(side === "LONG" ? entry + (2/qty) : entry - (2/qty));
            } else if (state.position.maxPnl >= 2 && state.position.slPrice !== entry && state.position.slDollars !== 2) {
                state.position.slPrice = entry;
            }
        }
        checkExit(roundedPrice);
    } else {
        if (!state.thinking) enterThinkingMode();
    }
    broadcastState();
}

function connectStream(symbol) {
    if (ws) { try { ws.close(); } catch(e){} ws = null; }
    const url = `wss://fstream.binance.com/ws/${symbol}@trade`;
    ws = new WebSocket(url);
    ws.onmessage = (evt) => {
        try {
            const data = JSON.parse(evt.data);
            const price = parseFloat(data.p);
            if (!isNaN(price) && price > 0) handleTick(price);
        } catch (e) {}
    };
    ws.onclose = () => { if (state.botActive) setTimeout(() => connectStream(symbol), 2000); };
}

// --- SOCKET.IO CONNECTIONS (DASHBOARD CONTROLS) ---
io.on('connection', (socket) => {
    console.log('Dashboard connected');
    socket.emit('state', state); // Send current state to the dashboard

    socket.on('toggle_bot', () => {
        state.botActive = !state.botActive;
        if (state.botActive) {
            connectStream(state.currentPair.symbol);
            if (!state.position) enterThinkingMode();
        } else {
            if (ws) { try { ws.close(); } catch(e){} ws = null; }
            state.thinking = false;
            state.structureText = "Bot is stopped.";
        }
        broadcastState();
    });

    socket.on('toggle_trailing', () => {
        state.trailingActive = !state.trailingActive;
        broadcastState();
    });

    socket.on('switch_pair', (symbol) => {
        let pair = null;
        for (const group of Object.values(PAIRS)) {
            const hit = group.find(p => p.symbol === symbol);
            if (hit) { pair = hit; break; }
        }
        if (pair) {
            state.currentPair = pair;
            state.position = null;
            state.lastPrice = 0;
            if (thinkTimer) clearInterval(thinkTimer);
            if (state.botActive) {
                connectStream(pair.symbol);
                enterThinkingMode();
            }
            broadcastState();
        }
    });

    socket.on('reset_all', () => {
        state.balance = STARTING_BALANCE;
        state.tradesCount = 0; state.wins = 0; state.losses = 0;
        state.tradeHistoryArr = [];
        state.position = null;
        broadcastState();
        if (state.botActive) enterThinkingMode();
    });
});

// --- START SERVER ---
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`Bot server running on port ${PORT}`);
    connectStream(state.currentPair.symbol);
    enterThinkingMode();
});
