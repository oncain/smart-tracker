/**
 * Smart Money Live Tracker — Backend
 * -----------------------------------
 * Mantık:
 *  - SENİN sabit kasan (örn $1000) üzerinden çalışır.
 *  - Trader bir pozisyon açtığında, onun "kullandığı kasa %"si (margin / margin_balance)
 *    senin sabit kasana oranlanır => "Benim Giriş Tutarım".
 *  - Pozisyonun PnL'i, trader'ın değil SENİN giriş tutarın üzerinden,
 *    entry fiyatı ile canlı mark price farkından + kaldıraçtan hesaplanır.
 *  - Mark price'lar Binance Futures'ın HERKESE AÇIK mark price stream'inden canlı gelir.
 *
 * Not: Bu, herkese açık fiyat verisi + senin tarafında simülasyon yapar.
 *      Trader'ın gizli/özel verisine erişmez.
 */

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");

// ---- Ayarlar ----
const PORT = process.env.PORT || 3000;
const DEFAULT_FIXED_CAPITAL = 1000.0; // Benim Sabit Kasam ($)

// ---- Durum (in-memory) ----
const state = {
  fixedCapital: DEFAULT_FIXED_CAPITAL,   // Benim Sabit Kasam
  traderMarginBalance: 185845.26,        // Trader Marj Bakiyesi (profilden manuel/otomatik girilir)
  positions: [],                         // açık pozisyonlar
  history: [],                           // kapanmış pozisyonlar (geçmiş)
  prices: {},                            // { BTCUSDT: 64000.12, ... } canlı mark price
  seq: 1,
};

// Yardımcı: bir sembol için canlı fiyatı al
function getMarkPrice(symbol, fallback) {
  const p = state.prices[symbol];
  return (typeof p === "number" && p > 0) ? p : fallback;
}

/**
 * Tek bir pozisyon için SENİN PnL'ini hesaplar.
 * pos: {
 *   symbol, side('LONG'|'SHORT'), leverage, marginType('Cross'|'Isolated'),
 *   entryPrice, liqPrice,
 *   traderMarginPct  -> trader'ın kasasının % kaçını kullandığı (0.019 = %1.9)
 * }
 */
function computePosition(pos) {
  const mark = getMarkPrice(pos.symbol, pos.entryPrice);

  // Benim bu pozisyona ayırdığım giriş tutarım (margin):
  const myMargin = state.fixedCapital * pos.traderMarginPct; // örn 1000 * 0.019 = 18.90

  // Notional (kaldıraçlı pozisyon büyüklüğü) — benim tarafımda:
  const myNotional = myMargin * pos.leverage;

  // Fiyat değişim oranı
  const dir = pos.side === "LONG" ? 1 : -1;
  const priceChangePct = (mark - pos.entryPrice) / pos.entryPrice; // ham %
  const pnlPct = priceChangePct * dir;                              // yöne göre

  // Benim net PnL'im ($): notional * fiyat değişimi
  const myPnlUsd = myNotional * pnlPct;

  // ROI (margin üzerinden, yani kaldıraçlı getiri %)
  const myRoiPct = (myMargin > 0) ? (myPnlUsd / myMargin) * 100 : 0;

  return {
    ...pos,
    markPrice: mark,
    myMargin: round2(myMargin),
    myNotional: round2(myNotional),
    myPnlUsd: round2(myPnlUsd),
    myRoiPct: round2(myRoiPct),
  };
}

function round2(n) { return Math.round((n + Number.EPSILON) * 100) / 100; }

// Tüm açık pozisyonların hesaplanmış halini + özet kartları üretir
function buildSnapshot() {
  const computed = state.positions.map(computePosition);

  const usedMargin = computed.reduce((s, p) => s + p.myMargin, 0);
  const usedPct = state.fixedCapital > 0 ? (usedMargin / state.fixedCapital) * 100 : 0;
  const totalPnl = computed.reduce((s, p) => s + p.myPnlUsd, 0);

  return {
    type: "snapshot",
    ts: Date.now(),
    fixedCapital: round2(state.fixedCapital),
    usedMargin: round2(usedMargin),
    usedPct: round2(usedPct),
    traderMarginBalance: round2(state.traderMarginBalance),
    openCount: computed.length,
    totalPnl: round2(totalPnl),
    equity: round2(state.fixedCapital + totalPnl),
    positions: computed,
    history: state.history,
  };
}

// ---- Express + WS sunucu ----
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

function broadcast() {
  const snap = JSON.stringify(buildSnapshot());
  wss.clients.forEach((c) => {
    if (c.readyState === WebSocket.OPEN) c.send(snap);
  });
}

wss.on("connection", (ws) => {
  ws.send(JSON.stringify(buildSnapshot()));
});

// ---- REST API (test butonları için) ----

// Sabit kasayı / trader marj bakiyesini güncelle
app.post("/api/config", (req, res) => {
  const { fixedCapital, traderMarginBalance } = req.body || {};
  if (typeof fixedCapital === "number" && fixedCapital > 0) state.fixedCapital = fixedCapital;
  if (typeof traderMarginBalance === "number" && traderMarginBalance >= 0) state.traderMarginBalance = traderMarginBalance;
  broadcast();
  res.json({ ok: true });
});

// Yeni pozisyon aç
app.post("/api/positions", (req, res) => {
  const b = req.body || {};
  const pos = {
    id: state.seq++,
    symbol: (b.symbol || "BTCUSDT").toUpperCase(),
    side: (b.side === "SHORT" ? "SHORT" : "LONG"),
    leverage: Number(b.leverage) || 10,
    marginType: b.marginType === "Isolated" ? "Isolated" : "Cross",
    entryPrice: Number(b.entryPrice) || getMarkPrice((b.symbol||"BTCUSDT").toUpperCase(), 0) || 0,
    liqPrice: Number(b.liqPrice) || 0,
    // trader'ın kasasının yüzde kaçı (0.019 = %1.9). UI'da % olarak girilir, burada /100.
    traderMarginPct: (Number(b.traderMarginPct) || 1.9) / 100,
    openedAt: Date.now(),
  };
  // entry girilmemişse canlı fiyatı entry yap
  if (!pos.entryPrice) pos.entryPrice = getMarkPrice(pos.symbol, 0);
  state.positions.push(pos);
  broadcast();
  res.json({ ok: true, position: pos });
});

// Pozisyon güncelle (örn entry, kaldıraç, oran)
app.patch("/api/positions/:id", (req, res) => {
  const id = Number(req.params.id);
  const pos = state.positions.find((p) => p.id === id);
  if (!pos) return res.status(404).json({ ok: false });
  const b = req.body || {};
  if (b.entryPrice != null) pos.entryPrice = Number(b.entryPrice);
  if (b.leverage != null) pos.leverage = Number(b.leverage);
  if (b.liqPrice != null) pos.liqPrice = Number(b.liqPrice);
  if (b.traderMarginPct != null) pos.traderMarginPct = Number(b.traderMarginPct) / 100;
  if (b.side != null) pos.side = b.side === "SHORT" ? "SHORT" : "LONG";
  broadcast();
  res.json({ ok: true, position: pos });
});

// Pozisyon kapat -> geçmişe taşı
app.delete("/api/positions/:id", (req, res) => {
  const id = Number(req.params.id);
  const idx = state.positions.findIndex((p) => p.id === id);
  if (idx === -1) return res.status(404).json({ ok: false });
  const closed = computePosition(state.positions[idx]);
  closed.closedAt = Date.now();
  state.history.unshift(closed);
  state.positions.splice(idx, 1);
  broadcast();
  res.json({ ok: true });
});

// Hepsini sıfırla
app.post("/api/reset", (req, res) => {
  state.positions = [];
  state.history = [];
  state.seq = 1;
  broadcast();
  res.json({ ok: true });
});

// ---- Binance herkese açık MARK PRICE stream ----
// !markPrice@arr  -> tüm sembollerin mark price'ı (1sn). Auth gerektirmez, herkese açık.
function connectBinance() {
  const url = "wss://fstream.binance.com/ws/!markPrice@arr@1s";
  let ws;
  try {
    ws = new WebSocket(url);
  } catch (e) {
    console.error("Binance WS kurulamadı:", e.message);
    setTimeout(connectBinance, 5000);
    return;
  }

  ws.on("open", () => console.log("[Binance] mark price stream bağlandı"));
  ws.on("message", (raw) => {
    try {
      const arr = JSON.parse(raw);
      if (Array.isArray(arr)) {
        for (const t of arr) {
          // t.s = symbol, t.p = mark price
          if (t.s && t.p) state.prices[t.s] = parseFloat(t.p);
        }
      }
    } catch (_) {}
  });
  ws.on("close", () => {
    console.log("[Binance] bağlantı kapandı, yeniden bağlanılıyor...");
    setTimeout(connectBinance, 3000);
  });
  ws.on("error", (e) => {
    console.error("[Binance] hata:", e.message);
    try { ws.close(); } catch (_) {}
  });
}
connectBinance();

// Fiyatlar canlı değişince frontend'e yay (saniyede ~1)
setInterval(broadcast, 1000);

server.listen(PORT, () => {
  console.log(`Smart Money Live Tracker -> http://localhost:${PORT}`);
});
