/**
 * FormBot index.js (FULL COPY/PASTE REWRITE)
 *
 * Features:
 * ✅ Persistent panel message (panel.json)
 * ✅ Persistent user timezone offsets (timezones.json) as offsetMinutes
 * ✅ Reservation input in USER LOCAL -> converted to UTC string: "HH:mm dd/MM/yyyy" or "—"
 * ✅ Done button is reversible (toggles via Apps Script action: "toggle_done")
 * ✅ Remind button:
 *    - Only shows if reservation exists (not "—" / not empty)
 *    - Clicking "⏰ Remind" arms reminder AT the reservation UTC time
 *    - Button on the original request toggles to "🛑 Cancel Remind"
 *    - Cancel clears timer + clears Remind At in the sheet via Apps Script action: "clear_remind"
 * ✅ Reminder message:
 *    - Shows: "[Title] for [Username]" (from the original request embed)
 *    - NO Remind button on the reminder message
 *    - Includes ✅ Done + 🛑 Cancel reminder
 * ✅ Clicking Done cancels any reminder and clears Remind At
 *
 * Requires your Apps Script to support:
 * - { action:"toggle_done_and_clear_remind", rowSerial:"123" } -> { success:true, done:true/false, reminder:false }
 * - { action:"toggle_done", rowSerial:"123" } -> { success:true, done:true/false }
 * - { action:"remind", rowSerial:"123" }      -> { success:true }
 * - { action:"clear_remind", rowSerial:"123"} -> { success:true }
 */

const path = require("path");
require("dotenv").config({ path: path.join(__dirname, ".env"), quiet: true });
const fetch = require("node-fetch");
const { DateTime } = require("luxon");
const fs = require("fs");

const {
  Client,
  GatewayIntentBits,
  SlashCommandBuilder,
  REST,
  Routes,
  EmbedBuilder,
  ActionRowBuilder,
  ButtonBuilder,
  ButtonStyle,
  StringSelectMenuBuilder,
  ModalBuilder,
  TextInputBuilder,
  TextInputStyle,
  MessageFlags,
} = require("discord.js");

// =====================
// ENV
// =====================
const {
  DISCORD_TOKEN,
  CLIENT_ID,
  GUILD_ID,
  GUILD_IDS,
  APPS_SCRIPT_URL,
  SHEETDB_URL,
  PANEL_CHANNEL_ID,
  PANEL_CHANNEL_IDS,
  COMMAND_CHANNEL_IDS,
  GUARDIAN_ID,
  FORM_CHANNEL_ID,
  PING_CHANNEL_ID,
  PING_CHANNEL_BY_GUILD: PING_CHANNEL_BY_GUILD_ENV,
  REMINDER_CHANNEL_ID,
  BLOCK_BOOLEAN,
  HOURLY_RESTART,
  HOURLY_RESTART_MINUTES,
  BUTTON_LOGGER,
  PERF_LOGGER,
  PERF_SLOW_MS,
} = process.env;

function parseCsvIds(input) {
  return Array.from(
    new Set(
      String(input || "")
    .split(",")
    .map((s) => s.trim())
        .filter(Boolean)
    )
  );
}

function parseGuildChannelMap(input) {
  const pairs = [];
  for (const entry of String(input || "").split(",")) {
    const chunk = entry.trim();
    if (!chunk) continue;
    const parts = chunk.split(":").map((s) => s.trim()).filter(Boolean);
    if (parts.length !== 2) continue;
    pairs.push([parts[0], parts[1]]);
  }
  return pairs;
}

const DEFAULT_PING_CHANNEL_BY_GUILD = {
  "1423795703934877970": "1474066936328355851",
  "1422549840990044212": "1446589241273356519",
};

const PING_CHANNEL_BY_GUILD = new Map(Object.entries(DEFAULT_PING_CHANNEL_BY_GUILD));
for (const [guildId, channelId] of parseGuildChannelMap(PING_CHANNEL_BY_GUILD_ENV)) {
  PING_CHANNEL_BY_GUILD.set(guildId, channelId);
}

const TARGET_GUILD_IDS = Array.from(
  new Set([...parseCsvIds(GUILD_ID), ...parseCsvIds(GUILD_IDS)])
);
const TARGET_PANEL_CHANNEL_IDS = Array.from(
  new Set([...parseCsvIds(PANEL_CHANNEL_ID), ...parseCsvIds(PANEL_CHANNEL_IDS)])
);
const TARGET_PING_CHANNEL_IDS = Array.from(
  new Set([PING_CHANNEL_ID, ...Array.from(PING_CHANNEL_BY_GUILD.values())].filter(Boolean))
);
const TARGET_COMMAND_CHANNEL_IDS = parseCsvIds(COMMAND_CHANNEL_IDS);
const COMMAND_CHANNEL_SET = new Set(TARGET_COMMAND_CHANNEL_IDS);
const BLOCKS_ENABLED = String(BLOCK_BOOLEAN ?? "1").trim() === "1";
const HOURLY_RESTART_ENABLED = String(HOURLY_RESTART ?? "0").trim() === "1";
const HOURLY_RESTART_INTERVAL_MINUTES = Math.max(1, Number(HOURLY_RESTART_MINUTES ?? "60") || 60);
const HOURLY_RESTART_MS = HOURLY_RESTART_INTERVAL_MINUTES * 60 * 1000;
const BUTTON_LOGGER_ENABLED = String(BUTTON_LOGGER ?? "1").trim() === "1";

const missingEnvKeys = [];
if (!DISCORD_TOKEN) missingEnvKeys.push("DISCORD_TOKEN");
if (!CLIENT_ID) missingEnvKeys.push("CLIENT_ID");
if (TARGET_GUILD_IDS.length === 0) missingEnvKeys.push("GUILD_ID/GUILD_IDS");
if (!APPS_SCRIPT_URL) missingEnvKeys.push("APPS_SCRIPT_URL");
if (TARGET_PANEL_CHANNEL_IDS.length === 0) missingEnvKeys.push("PANEL_CHANNEL_ID/PANEL_CHANNEL_IDS");
if (!FORM_CHANNEL_ID) missingEnvKeys.push("FORM_CHANNEL_ID");
if (!PING_CHANNEL_ID) missingEnvKeys.push("PING_CHANNEL_ID");
if (!REMINDER_CHANNEL_ID) missingEnvKeys.push("REMINDER_CHANNEL_ID");
if (missingEnvKeys.length > 0) {
  throw new Error(`Missing required .env values: ${missingEnvKeys.join(", ")}`);
}

// =====================
// FILE STORES
// =====================
const PANEL_STORE_PATH = path.join(__dirname, "panel.json");
const TZ_STORE_PATH = path.join(__dirname, "timezones.json");
const PREFILL_STORE_PATH = path.join(__dirname, "prefill.json");
const TIMERS_STORE_PATH = path.join(__dirname, "timers-messages.json");
const RESERVATIONS_STORE_PATH = path.join(__dirname, "reservations-messages.json");
const RESERVATION_OWNER_STORE_PATH = path.join(__dirname, "reservation-owners.json");
const RESERVATION_MESSAGE_STORE_PATH = path.join(__dirname, "reservation-messages.json");
const AUDIT_LOG_PATH = path.join(__dirname, "audit.log");
const BUTTON_LOG_PATH = path.join(__dirname, "button-logs.ndjson");
const PERF_LOG_PATH = path.join(__dirname, "perf.log");
const TIMERS_CACHE_MAX_AGE_MS = 60_000;
const FILE_WRITE_DEBOUNCE_MS = 250;
const LOG_WRITE_DEBOUNCE_MS = 250;
const PERF_LOGGER_ENABLED = String(PERF_LOGGER ?? "1").trim() === "1";
const PERF_SLOW_THRESHOLD_MS = Math.max(0, Number(PERF_SLOW_MS ?? "200") || 0);
const CONTENT_VERIFY_INTERVAL_MS = 5 * 60_000;
const BUTTON_INTERACTION_TTL_MS = 2 * 60_000;
const PERF_METRIC_MAX_SAMPLES = 200;
const APPS_SCRIPT_TIMEOUT_MS = 12_000;
const APPS_SCRIPT_MAX_REDIRECTS = 2;
const TEST_DISCORD_SUFFIX_EMOJI = "<:cornershaking:1474243561506734121>";

function readJsonSafe(filepath, fallback = {}) {
  try {
    const raw = fs.readFileSync(filepath, "utf8");
    // Allow simple JSON-with-comments files (handy for manual admin notes).
    const withoutBlock = raw.replace(/\/\*[\s\S]*?\*\//g, "");
    const withoutLine = withoutBlock.replace(/^\s*\/\/.*$/gm, "");
    return JSON.parse(withoutLine);
  } catch {
    return fallback;
  }
}
const bufferedJsonWrites = new Map(); // filepath -> { data, timer, inFlight, dirty }
const bufferedLogWrites = new Map(); // filepath -> { chunks, timer, inFlight }
const processedButtonInteractionIds = new Map(); // interactionId -> expiresAtMs
const perfMetrics = new Map(); // key -> { samples:number[], totalCount:number, errorCount:number, lastMs:number, lastAt:number }

function flushJsonWrite(filepath) {
  const state = bufferedJsonWrites.get(filepath);
  if (!state || !state.dirty || state.inFlight) return;
  state.inFlight = true;
  state.dirty = false;
  const payload = JSON.stringify(state.data, null, 2);

  fs.promises.writeFile(filepath, payload, "utf8")
    .catch(() => {})
    .finally(() => {
      state.inFlight = false;
      if (state.dirty && !state.timer) {
        state.timer = setTimeout(() => {
          state.timer = null;
          flushJsonWrite(filepath);
        }, FILE_WRITE_DEBOUNCE_MS);
      }
    });
}

function scheduleJsonWrite(filepath, data, debounceMs = FILE_WRITE_DEBOUNCE_MS) {
  const state = bufferedJsonWrites.get(filepath) || { data: null, timer: null, inFlight: false, dirty: false };
  state.data = data;
  state.dirty = true;
  if (state.timer) clearTimeout(state.timer);
  state.timer = setTimeout(() => {
    state.timer = null;
    flushJsonWrite(filepath);
  }, debounceMs);
  bufferedJsonWrites.set(filepath, state);
}

function writeJsonSafe(filepath, data) {
  scheduleJsonWrite(filepath, data);
}

function flushLogWrite(filepath) {
  const state = bufferedLogWrites.get(filepath);
  if (!state || state.inFlight || state.chunks.length === 0) return;
  state.inFlight = true;
  const payload = state.chunks.join("");
  state.chunks = [];

  fs.promises.appendFile(filepath, payload, "utf8")
    .catch(() => {})
    .finally(() => {
      state.inFlight = false;
      if (state.chunks.length > 0 && !state.timer) {
        state.timer = setTimeout(() => {
          state.timer = null;
          flushLogWrite(filepath);
        }, LOG_WRITE_DEBOUNCE_MS);
      }
    });
}

function appendLineBuffered(filepath, line, debounceMs = LOG_WRITE_DEBOUNCE_MS) {
  const state = bufferedLogWrites.get(filepath) || { chunks: [], timer: null, inFlight: false };
  state.chunks.push(line);
  if (!state.timer) {
    state.timer = setTimeout(() => {
      state.timer = null;
      flushLogWrite(filepath);
    }, debounceMs);
  }
  bufferedLogWrites.set(filepath, state);
}

function flushAllBufferedWritesSync() {
  for (const [filepath, state] of bufferedJsonWrites.entries()) {
    if (!state?.dirty) continue;
    try {
      fs.writeFileSync(filepath, JSON.stringify(state.data, null, 2), "utf8");
      state.dirty = false;
    } catch {}
  }
  for (const [filepath, state] of bufferedLogWrites.entries()) {
    if (!state?.chunks?.length) continue;
    try {
      fs.appendFileSync(filepath, state.chunks.join(""), "utf8");
      state.chunks = [];
    } catch {}
  }
}

process.on("exit", flushAllBufferedWritesSync);

function perfLog(event, data = {}) {
  if (!PERF_LOGGER_ENABLED) return;
  try {
    appendLineBuffered(PERF_LOG_PATH, `${JSON.stringify({ ts: new Date().toISOString(), event, ...data })}\n`);
  } catch {}
}

function perfMetricKey(event, data = {}) {
  if (event === "apps_script_call") return `apps:${String(data.action || "unknown")}`;
  if (event === "discord_message_edit") return `discord_edit:${String(data.scope || "unknown")}`;
  if (event === "done_interaction_ack") return `done_ack:${String(data.ackType || "unknown")}`;
  if (event === "timers_tick") return "timers_tick";
  if (event === "reservations_tick") return "reservations_tick";
  if (event === "sheetdb_snapshot_fetch") return "sheetdb_snapshot_fetch";
  return String(event);
}

function recordPerfSample(event, durationMs, data = {}) {
  const key = perfMetricKey(event, data);
  const current = perfMetrics.get(key) || {
    samples: [],
    totalCount: 0,
    errorCount: 0,
    lastMs: 0,
    lastAt: 0,
  };
  current.samples.push(durationMs);
  if (current.samples.length > PERF_METRIC_MAX_SAMPLES) current.samples.shift();
  current.totalCount += 1;
  if (data.error) current.errorCount += 1;
  current.lastMs = durationMs;
  current.lastAt = Date.now();
  perfMetrics.set(key, current);
}

function percentileFromSorted(sortedValues, p) {
  if (!sortedValues.length) return 0;
  const idx = Math.max(0, Math.min(sortedValues.length - 1, Math.ceil((p / 100) * sortedValues.length) - 1));
  return sortedValues[idx];
}

function formatMs(ms) {
  if (!Number.isFinite(ms)) return "n/a";
  return `${ms.toFixed(1)}ms`;
}

function buildPerfSummaryLines(limit = 12) {
  if (perfMetrics.size === 0) {
    return ["No performance samples yet. Use the bot for a minute and retry."];
  }

  const now = Date.now();
  const rows = [];
  for (const [key, state] of perfMetrics.entries()) {
    const samples = Array.isArray(state.samples) ? state.samples.slice() : [];
    if (samples.length === 0) continue;
    samples.sort((a, b) => a - b);
    const sum = samples.reduce((acc, v) => acc + v, 0);
    const avg = sum / samples.length;
    const p50 = percentileFromSorted(samples, 50);
    const p95 = percentileFromSorted(samples, 95);
    const errorRate = state.totalCount > 0 ? (state.errorCount / state.totalCount) * 100 : 0;
    rows.push({
      key,
      samples: samples.length,
      totalCount: state.totalCount,
      avg,
      p50,
      p95,
      errorRate,
      lastAgoSec: Math.max(0, Math.floor((now - Number(state.lastAt || 0)) / 1000)),
    });
  }

  rows.sort((a, b) => b.p95 - a.p95 || b.avg - a.avg);
  const lines = [
    `Recent perf metrics (window <= ${PERF_METRIC_MAX_SAMPLES} samples per metric)`,
    `Logger: ${PERF_LOGGER_ENABLED ? "on" : "off"} | slow-log threshold: ${PERF_SLOW_THRESHOLD_MS}ms`,
  ];
  for (const row of rows.slice(0, limit)) {
    lines.push(
      `${row.key} | p50 ${formatMs(row.p50)} | p95 ${formatMs(row.p95)} | avg ${formatMs(row.avg)} | n ${row.samples}/${row.totalCount} | err ${row.errorRate.toFixed(1)}% | last ${row.lastAgoSec}s`
    );
  }
  if (rows.length > limit) {
    lines.push(`...and ${rows.length - limit} more metrics`);
  }
  return lines;
}

function perfDuration(event, startedAtMs, data = {}, force = false) {
  const durationMs = Date.now() - startedAtMs;
  recordPerfSample(event, durationMs, data);
  if (force || durationMs >= PERF_SLOW_THRESHOLD_MS) {
    perfLog(event, { durationMs, ...data });
  }
  return durationMs;
}

function pruneProcessedButtonInteractions(nowMs = Date.now()) {
  for (const [interactionId, expiresAt] of processedButtonInteractionIds.entries()) {
    if (expiresAt <= nowMs) processedButtonInteractionIds.delete(interactionId);
  }
}

function isDuplicateButtonInteraction(interactionId) {
  if (!interactionId) return false;
  const now = Date.now();
  if (processedButtonInteractionIds.size >= 1024) {
    pruneProcessedButtonInteractions(now);
  }
  const expiresAt = processedButtonInteractionIds.get(interactionId) || 0;
  if (expiresAt > now) return true;
  processedButtonInteractionIds.set(interactionId, now + BUTTON_INTERACTION_TTL_MS);
  return false;
}

function auditLog(event, data = {}) {
  try {
    const line = JSON.stringify({ ts: new Date().toISOString(), event, ...data });
    appendLineBuffered(AUDIT_LOG_PATH, `${line}\n`);
  } catch {}
}

function appendButtonLog(entry) {
  if (!BUTTON_LOGGER_ENABLED) return;
  try {
    appendLineBuffered(BUTTON_LOG_PATH, `${JSON.stringify(entry)}\n`);
  } catch {}
}

// ---- Panel store
let panelStoreCache = readJsonSafe(PANEL_STORE_PATH, {});
function readPanelStore() {
  return panelStoreCache && typeof panelStoreCache === "object" ? panelStoreCache : {};
}
function writePanelStore(data) {
  panelStoreCache = data && typeof data === "object" ? data : {};
  writeJsonSafe(PANEL_STORE_PATH, panelStoreCache);
}
function clearPanelMessageId() {
  writePanelStore({ byChannel: {} });
}
function setPanelMessageRef(channelId, messageId) {
  const store = readPanelStore();
  const byChannel = store.byChannel && typeof store.byChannel === "object" ? store.byChannel : {};
  byChannel[channelId] = messageId;
  writePanelStore({ byChannel });
}
function getPanelMessageRef(channelId) {
  const store = readPanelStore();
  if (store.byChannel && typeof store.byChannel === "object") {
    const messageId = store.byChannel[channelId];
    return { channelId, messageId };
  }
  // Backward compatibility with old single-panel format
  if (store.channelId && store.messageId && String(store.channelId) === String(channelId)) {
    return { channelId: store.channelId, messageId: store.messageId };
  }
  return { channelId, messageId: null };
}

// ---- Timezone store (discordId -> { zone | offsetMinutes, name })
const tzStore = readJsonSafe(TZ_STORE_PATH, {});
function normalizeTzStoreValue(v) {
  if (v && typeof v === "object") {
    const zone = typeof v.zone === "string" ? v.zone : null;
    const offsetMinutes = Number.isFinite(Number(v.offsetMinutes)) ? Number(v.offsetMinutes) : null;
    const name = typeof v.name === "string" ? v.name : undefined;
    if (zone) return { zone, name };
    if (offsetMinutes !== null) return { offsetMinutes, name };
  }
  if (typeof v === "string") {
    if (/^-?\d+(\.\d+)?$/.test(v)) return { offsetMinutes: Number(v) };
    return { zone: v };
  }
  if (Number.isFinite(Number(v))) return { offsetMinutes: Number(v) };
  return {};
}
const userTimezones = new Map(Object.entries(tzStore).map(([k, v]) => [k, normalizeTzStoreValue(v)]));

// ---- Prefill store (discordId -> { username, coordinates })
const prefillStore = readJsonSafe(PREFILL_STORE_PATH, {});
const userPrefill = new Map(
  Object.entries(prefillStore).map(([k, v]) => [k, { username: v.username || "", coordinates: v.coordinates || "" }])
);
const reservationOwnerStore = readJsonSafe(RESERVATION_OWNER_STORE_PATH, {});
const reservationOwners = new Map(Object.entries(reservationOwnerStore).map(([serial, userId]) => [String(serial), String(userId)]));
const reservationMessageStore = readJsonSafe(RESERVATION_MESSAGE_STORE_PATH, {});
const reservationMessages = new Map(Object.entries(reservationMessageStore).map(([serial, ref]) => [String(serial), ref || {}]));

function persistTimezones() {
  writeJsonSafe(TZ_STORE_PATH, Object.fromEntries(userTimezones));
}

function getUserTimezone(userId) {
  if (!userTimezones.has(userId)) return null;
  const v = userTimezones.get(userId);
  if (v && typeof v === "object") {
    if (typeof v.zone === "string") return { type: "iana", zone: v.zone };
    if (Number.isFinite(Number(v.offsetMinutes))) return { type: "offset", offsetMinutes: Number(v.offsetMinutes) };
  }
  return null;
}
function setUserOffsetMinutes(userId, offsetMinutes, name) {
  userTimezones.set(userId, { offsetMinutes, name });
  persistTimezones();
}
function setUserTimezoneZone(userId, zone, name) {
  userTimezones.set(userId, { zone, name });
  persistTimezones();
}
function clearUserTimezone(userId) {
  userTimezones.delete(userId);
  persistTimezones();
}

function persistPrefill() {
  writeJsonSafe(PREFILL_STORE_PATH, Object.fromEntries(userPrefill));
}

function setUserPrefill(userId, username, coordinates) {
  userPrefill.set(userId, { username, coordinates });
  persistPrefill();
}

function getUserPrefill(userId) {
  return userPrefill.get(userId) || { username: "", coordinates: "" };
}

function persistReservationOwners() {
  writeJsonSafe(RESERVATION_OWNER_STORE_PATH, Object.fromEntries(reservationOwners));
}

function setReservationOwner(serial, userId) {
  reservationOwners.set(String(serial), String(userId));
  persistReservationOwners();
}

function getReservationOwner(serial) {
  return reservationOwners.get(String(serial)) || null;
}

function persistReservationMessages() {
  writeJsonSafe(RESERVATION_MESSAGE_STORE_PATH, Object.fromEntries(reservationMessages));
}

function setReservationRequestMessage(serial, channelId, messageId, originGuildId = null) {
  const key = String(serial);
  const prev = reservationMessages.get(key) || {};
  const next = {
    ...prev,
    requestChannelId: String(channelId),
    requestMessageId: String(messageId),
  };
  if (originGuildId) next.originGuildId = String(originGuildId);
  reservationMessages.set(key, next);
  persistReservationMessages();
}

function setReservationReminderMessage(serial, channelId, messageId) {
  const key = String(serial);
  const prev = reservationMessages.get(key) || {};
  reservationMessages.set(key, {
    ...prev,
    reminderChannelId: String(channelId),
    reminderMessageId: String(messageId),
  });
  persistReservationMessages();
}

function clearReservationMessage(serial) {
  reservationMessages.delete(String(serial));
  persistReservationMessages();
}

function getReservationOriginGuild(serial) {
  const ref = reservationMessages.get(String(serial)) || {};
  if (!ref.originGuildId) return null;
  return String(ref.originGuildId);
}

function resolvePingChannelId(originGuildId, currentGuildId) {
  if (originGuildId) {
    const configured = PING_CHANNEL_BY_GUILD.get(String(originGuildId));
    if (configured) return configured;
  }
  if (currentGuildId) {
    const configured = PING_CHANNEL_BY_GUILD.get(String(currentGuildId));
    if (configured) return configured;
  }
  return PING_CHANNEL_ID;
}

function hasUserRecord(userId) {
  const p = userPrefill.get(userId);
  if (!p) return false;
  return Boolean(String(p.username || "").trim() || String(p.coordinates || "").trim());
}

// =====================
// TIME HELPERS (USER LOCAL CALENDAR via OFFSET or IANA)
// =====================
const DAY_MS = 24 * 60 * 60 * 1000;
const TW_ANCHOR_UTC_MS = Date.UTC(2026, 1, 3, 0, 0, 0, 0); // Tue Feb 3, 2026
const TW_INTERVAL_DAYS = 14;
const VAULT_ANCHOR_UTC_MS = Date.UTC(2026, 0, 31, 0, 0, 0, 0); // Sat Jan 31, 2026
const VAULT_INTERVAL_DAYS = 14;
const weekdays = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];

function pad(n) {
  return String(n).padStart(2, "0");
}

function userNow(tz) {
  if (!tz) return new Date();
  if (tz.type === "iana") return DateTime.now().setZone(tz.zone).toJSDate();
  return new Date(Date.now() + tz.offsetMinutes * 60000);
}
function userYMD(tz) {
  const d = userNow(tz);
  return { y: d.getUTCFullYear(), m: d.getUTCMonth() + 1, d: d.getUTCDate() };
}
function userNextWeekdayYMD(dayName, tz) {
  const targetIndex = weekdays.findIndex((w) => w.startsWith(dayName.toLowerCase()));
  if (targetIndex === -1) return null;

  const base = userNow(tz);
  const todayIndex = base.getUTCDay();
  let diff = (targetIndex - todayIndex + 7) % 7;
  if (diff === 0) diff = 7;

  const next = new Date(base.getTime() + diff * DAY_MS);
  return { y: next.getUTCFullYear(), m: next.getUTCMonth() + 1, d: next.getUTCDate() };
}

// Convert USER-LOCAL wall-clock -> UTC ms
function userLocalToUtcMs(y, m, d, hh, mm, tz) {
  if (!tz) return Date.UTC(y, m - 1, d, hh, mm, 0, 0);
  if (tz.type === "iana") {
    return DateTime.fromObject(
      { year: y, month: m, day: d, hour: hh, minute: mm },
      { zone: tz.zone }
    )
      .toUTC()
      .toMillis();
  }
  return Date.UTC(y, m - 1, d, hh, mm, 0, 0) - tz.offsetMinutes * 60000;
}

function formatUTCDateTime(dUtc) {
  return `${pad(dUtc.getUTCHours())}:${pad(dUtc.getUTCMinutes())} ${pad(dUtc.getUTCDate())}/${pad(
    dUtc.getUTCMonth() + 1
  )}/${dUtc.getUTCFullYear()}`;
}

function isTempleWarsBlockedAtUtcMs(utcMs) {
  if (!BLOCKS_ENABLED) return false;
  if (!Number.isFinite(utcMs)) return false;
  const d = new Date(utcMs);
  if (d.getUTCDay() !== 2) return false; // Tuesday

  const dayStartMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0, 0);
  const daysFromAnchor = Math.floor((dayStartMs - TW_ANCHOR_UTC_MS) / DAY_MS);
  const cycleDay = ((daysFromAnchor % TW_INTERVAL_DAYS) + TW_INTERVAL_DAYS) % TW_INTERVAL_DAYS;
  if (cycleDay !== 0) return false; // not the bi-weekly TW Tuesday

  const minutesOfDay = d.getUTCHours() * 60 + d.getUTCMinutes();
  return minutesOfDay >= 0 && minutesOfDay <= (23 * 60 + 59); // 00:00..23:59 UTC
}

function isVaultBlockedAtUtcMsForTitle(utcMs, title) {
  if (!BLOCKS_ENABLED) return false;
  if (String(title || "").toLowerCase() !== "general") return false;
  if (!Number.isFinite(utcMs)) return false;

  const d = new Date(utcMs);
  if (d.getUTCDay() !== 6) return false; // Saturday

  const dayStartMs = Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(), 0, 0, 0, 0);
  const daysFromAnchor = Math.floor((dayStartMs - VAULT_ANCHOR_UTC_MS) / DAY_MS);
  const cycleDay = ((daysFromAnchor % VAULT_INTERVAL_DAYS) + VAULT_INTERVAL_DAYS) % VAULT_INTERVAL_DAYS;
  if (cycleDay !== 0) return false; // not the bi-weekly Vault Saturday

  const minutesOfDay = d.getUTCHours() * 60 + d.getUTCMinutes();
  const inWindowOne = minutesOfDay >= 30 && minutesOfDay <= 180; // 00:30..03:00 UTC
  const inWindowTwo = minutesOfDay >= (17 * 60 + 30) && minutesOfDay <= (20 * 60); // 17:30..20:00 UTC
  return inWindowOne || inWindowTwo;
}

// =====================
// PARSERS
// =====================
function normalizeCoords(input) {
  if (!input) return "—";
  const digits = input.replace(/\D/g, "");
  if (digits.length === 6) return `${digits.slice(0, 3)}:${digits.slice(3)}`;
  const parts = input.replace(/\D+/g, ":").split(":").filter(Boolean);
  return parts.length >= 2 ? `${parts[0]}:${parts[1]}` : input;
}

function parseTime(input) {
  if (!input) return null;
  let t = input.toLowerCase().trim();
  t = t.replace(/\butc\b/g, "").trim();
  if (t === "now" || t === "asap") return null;
  if (t === "reset" || t === "on reset" || t === "at reset") return "00:00";

  t = t
    .replace(/\s+/g, "")
    .replace(/(\d{1,2})[.,;:-](\d{2})/g, "$1:$2")
    .replace(/\./g, "");

  // support 1330 -> 13:30
  const compact = t.match(/^(\d{1,2})(\d{2})(am|pm)?$/);
  if (compact) {
    t = `${compact[1]}:${compact[2]}${compact[3] || ""}`;
  }

  const m = t.match(/^(\d{1,2})(?::(\d{2}))?(am|pm)?$/);
  if (!m) return null;

  let h = parseInt(m[1], 10);
  let min = parseInt(m[2] ?? "0", 10);
  const ap = m[3];

  if (ap === "pm" && h < 12) h += 12;
  if (ap === "am" && h === 12) h = 0;

  if (h < 0 || h > 23 || min < 0 || min > 59) return null;
  return `${pad(h)}:${pad(min)}`;
}

function parseTimeParts(input) {
  const t = parseTime(input);
  if (!t) return null;
  const [hh, mm] = t.split(":").map(Number);
  return { hh, mm, hhmm: t };
}

function parseDateParts(input, tz) {
  if (!input) return null;
  let raw = input.toLowerCase().trim();

  raw = raw
    .replace(/(\d{1,2})[.\-](\d{1,2})[.\-](\d{2,4})/g, "$1/$2/$3")
    .replace(/(\d{1,2})[.\-](\d{1,2})$/g, "$1/$2");

  raw = raw.replace(/^(\d{1,2})(st|nd|rd|th)$/i, "$1");

  const monthMap = {
    jan: 1,
    january: 1,
    feb: 2,
    february: 2,
    mar: 3,
    march: 3,
    apr: 4,
    april: 4,
    may: 5,
    jun: 6,
    june: 6,
    jul: 7,
    july: 7,
    aug: 8,
    august: 8,
    sep: 9,
    sept: 9,
    september: 9,
    oct: 10,
    october: 10,
    nov: 11,
    november: 11,
    dec: 12,
    december: 12,
  };

  // Formats like: "4 feb", "4th feb", "04feb", "4 february"
  const dayMonth = raw.match(/^(\d{1,2})(?:st|nd|rd|th)?\s*([a-z]{3,9})$/i);
  if (dayMonth) {
    const dd = Number(dayMonth[1]);
    const MM = monthMap[String(dayMonth[2] || "").toLowerCase()];
    if (!MM) return null;
    let yyyy;
    if (tz?.type === "iana") {
      yyyy = DateTime.now().setZone(tz.zone).year;
    } else {
      yyyy = userNow(tz).getUTCFullYear();
    }
    const maxDays = new Date(Date.UTC(yyyy, MM, 0)).getUTCDate();
    if (dd < 1 || dd > maxDays) return null;
    return { y: yyyy, m: MM, d: dd };
  }

  if (raw === "today") {
    if (tz?.type === "iana") {
      const dt = DateTime.now().setZone(tz.zone);
      return { y: dt.year, m: dt.month, d: dt.day };
    }
    return userYMD(tz);
  }

  if (raw === "tomorrow") {
    if (tz?.type === "iana") {
      const dt = DateTime.now().setZone(tz.zone).plus({ days: 1 });
      return { y: dt.year, m: dt.month, d: dt.day };
    }
    const u = userNow(tz);
    const t = new Date(u.getTime() + DAY_MS);
    return { y: t.getUTCFullYear(), m: t.getUTCMonth() + 1, d: t.getUTCDate() };
  }

  const weekdayAliases = {
    sun: "sunday",
    sunday: "sunday",
    mon: "monday",
    mo: "monday",
    monday: "monday",
    tue: "tuesday",
    tu: "tuesday",
    tues: "tuesday",
    tuesday: "tuesday",
    wed: "wednesday",
    we: "wednesday",
    weds: "wednesday",
    wednesday: "wednesday",
    thu: "thursday",
    th: "thursday",
    thur: "thursday",
    thurs: "thursday",
    thursday: "thursday",
    fri: "friday",
    fr: "friday",
    friday: "friday",
    sat: "saturday",
    sa: "saturday",
    saturday: "saturday",
    su: "sunday",
  };
  const weekdayName = weekdayAliases[raw];
  if (weekdayName) {
    if (tz?.type === "iana") {
      const targetIndex = weekdays.findIndex((w) => w === weekdayName);
      if (targetIndex === -1) return null;
      let dt = DateTime.now().setZone(tz.zone).startOf("day");
      for (let i = 0; i < 14; i++) {
        if (dt.weekday % 7 === targetIndex) {
          return { y: dt.year, m: dt.month, d: dt.day };
        }
        dt = dt.plus({ days: 1 });
      }
      return null;
    }
    return userNextWeekdayYMD(weekdayName, tz);
  }

  // Day-only: "14" means 14th of current month in USER local calendar
  const dayOnly = raw.match(/^(\d{1,2})$/);
  if (dayOnly) {
    const dd = parseInt(dayOnly[1], 10);
    if (tz?.type === "iana") {
      const now = DateTime.now().setZone(tz.zone);
      const yyyy = now.year;
      const MM = now.month;
      const maxDays = DateTime.utc(yyyy, MM).daysInMonth;
      if (dd < 1 || dd > maxDays) return null;
      return { y: yyyy, m: MM, d: dd };
    }
    const now = userNow(tz);
    const yyyy = now.getUTCFullYear();
    const MM = now.getUTCMonth() + 1;
    const maxDays = new Date(Date.UTC(yyyy, MM, 0)).getUTCDate();
    if (dd < 1 || dd > maxDays) return null;
    return { y: yyyy, m: MM, d: dd };
  }

  // dd/MM or MM/dd with optional year
  const m = raw.match(/^(\d{1,2})\/(\d{1,2})(?:\/(\d{2,4}))?$/);
  if (m) {
    const p1 = parseInt(m[1], 10);
    const p2 = parseInt(m[2], 10);
    const yyRaw = m[3];

    let yyyy;
    if (tz?.type === "iana") {
      const now = DateTime.now().setZone(tz.zone);
      yyyy = now.year;
    } else {
      const now = userNow(tz);
      yyyy = now.getUTCFullYear();
    }
    if (yyRaw) yyyy = yyRaw.length === 2 ? 2000 + parseInt(yyRaw, 10) : parseInt(yyRaw, 10);

    let dd;
    let MM;
    const p1IsDay = p1 >= 1 && p1 <= 31;
    const p2IsDay = p2 >= 1 && p2 <= 31;
    const p1IsMonth = p1 >= 1 && p1 <= 12;
    const p2IsMonth = p2 >= 1 && p2 <= 12;

    if (!p1IsDay || !p2IsDay) return null;

    if (p1 > 12 && p2IsMonth) {
      dd = p1;
      MM = p2;
    } else if (p2 > 12 && p1IsMonth) {
      MM = p1;
      dd = p2;
    } else {
      const isAmericas = tz?.type === "iana" && tz.zone.startsWith("America/");
      if (isAmericas) {
        MM = p1;
        dd = p2;
      } else {
        dd = p1;
        MM = p2;
      }
    }

    if (MM < 1 || MM > 12) return null;
    const maxDays = new Date(Date.UTC(yyyy, MM, 0)).getUTCDate();
    if (dd < 1 || dd > maxDays) return null;

    return { y: yyyy, m: MM, d: dd };
  }

  return null;
}

/**
 * Reservation rules:
 * - now/asap => "—"
 * - none => "—"
 * - date only => local 00:00 on that date -> UTC
 * - time only => local today at time, but if passed in USER local -> tomorrow
 * - both => local date+time -> UTC
 */
function buildReservation(rawTime, rawDate, tz) {
  const rt = (rawTime || "").trim().toLowerCase();
  if (rt === "now" || rt === "asap") return "—";
  const timeExplicitUtc = /\butc\b/i.test(String(rawTime || ""));
  const effectiveTz = timeExplicitUtc ? { type: "offset", offsetMinutes: 0 } : tz;

  const timeP = parseTimeParts(rawTime);
  const dateP = parseDateParts(rawDate, effectiveTz);

  if (!timeP && !dateP) return "—";

  if (effectiveTz?.type === "iana") {
    const zone = effectiveTz.zone;
    if (!timeP && dateP) {
      const dt = DateTime.fromObject(
        { year: dateP.y, month: dateP.m, day: dateP.d, hour: 0, minute: 0 },
        { zone }
      );
      return formatUTCDateTime(new Date(dt.toUTC().toMillis()));
    }

    if (timeP && !dateP) {
      const nowUser = DateTime.now().setZone(zone);
      let dt = nowUser.set({ hour: timeP.hh, minute: timeP.mm, second: 0, millisecond: 0 });
      if (dt <= nowUser) {
        dt = dt.plus({ days: 1 });
      }
      return formatUTCDateTime(new Date(dt.toUTC().toMillis()));
    }

    const dt = DateTime.fromObject(
      { year: dateP.y, month: dateP.m, day: dateP.d, hour: timeP.hh, minute: timeP.mm },
      { zone }
    );
    return formatUTCDateTime(new Date(dt.toUTC().toMillis()));
  }

  // date only
  if (!timeP && dateP) {
    const utcMs = userLocalToUtcMs(dateP.y, dateP.m, dateP.d, 0, 0, effectiveTz);
    return formatUTCDateTime(new Date(utcMs));
  }

  // time only (compare in USER LOCAL)
  if (timeP && !dateP) {
    const nowUser = userNow(effectiveTz);
    const today = userYMD(effectiveTz);

    let utcMs = userLocalToUtcMs(today.y, today.m, today.d, timeP.hh, timeP.mm, effectiveTz);

    const reqUser = new Date(nowUser.getTime());
    reqUser.setUTCFullYear(today.y, today.m - 1, today.d);
    reqUser.setUTCHours(timeP.hh, timeP.mm, 0, 0);

    if (reqUser.getTime() <= nowUser.getTime()) {
      utcMs += DAY_MS;
    }

    return formatUTCDateTime(new Date(utcMs));
  }

  // date + time
  const utcMs = userLocalToUtcMs(dateP.y, dateP.m, dateP.d, timeP.hh, timeP.mm, effectiveTz);
  return formatUTCDateTime(new Date(utcMs));
}

// =====================
// TITLES
// =====================
const TITLES = [
  { label: "Governor", description: "Recruitment speed +10%", value: "Governor" },
  { label: "Architect", description: "Construction speed +10%", value: "Architect" },
  { label: "Prefect", description: "Research speed +10%", value: "Prefect" },
  { label: "General", description: "Bender attack +5%", value: "General" },
];

// userId -> selected title (for modal submit)
const pendingTitleByUser = new Map(); // userId -> { value,label,description,ts }

// =====================
// REMINDERS (in-memory)
// =====================
const reminderTimers = new Map(); // rowSerial -> Timeout
const reminderMeta = new Map(); // rowSerial -> { title, username, channelId, sourceUrl }
const MAX_TIMEOUT_MS = 2_147_483_647; // ~24.8 days
const timersMessageByChannel = new Map(); // channelId -> { messageId, intervalId }
const reservationsMessageByChannel = new Map(); // channelId -> { messageId, intervalId }
const interactionCooldowns = new Map(); // key -> expiresAtMs
const doneToggleInFlight = new Set(); // rowSerial keys currently toggling done/not done
const doneStateOverrides = new Map(); // rowSerial -> { done, expiresAt }
const LIVE_MESSAGE_REFRESH_MS = 15_000;
let startupStickyDelayUntil = 0;
let lastTimersText = null;
let lastTimersTextAt = 0;
let timersLastSuccessAt = 0;
let timersLastFailureAt = 0;
let appsScriptLastOkAt = 0;
let appsScriptLastError = "";
let timersSnapshot = null;
let timersSnapshotAt = 0;
let runtimeClient = null;
let orphanReservationCleanupInFlight = false;
let reservationStateSyncInFlight = false;
const TIMERS_REFRESH_MS = 15_000;
const SNAPSHOT_FETCH_TIMEOUT_MS = 8_000;
const SNAPSHOT_RECONCILE_INTERVAL_MS = 60_000;
const SNAPSHOT_REFRESH_SLOW_MS = 10_000;
const SNAPSHOT_REFRESH_VERY_SLOW_MS = 20_000;
const SNAPSHOT_REFRESH_SLOW_DELAY_MS = 30_000;
const SNAPSHOT_REFRESH_MAX_DELAY_MS = 60_000;
const DONE_STATE_OVERRIDE_TTL_MS = 30_000;
let timersNextFetchAttemptAt = 0;
let timersFailureStreak = 0;
let timersSnapshotRefreshPromise = null;
let timersSnapshotBackgroundIntervalId = null;
let timersSnapshotBackgroundInFlight = false;
let lastSnapshotRefreshDurationMs = 0;
let lastSnapshotReconcileAt = 0;
let snapshotReconcileInFlight = false;
let pendingSnapshotReconcile = null;
const timersStore = readJsonSafe(TIMERS_STORE_PATH, {});
const reservationsStore = readJsonSafe(RESERVATIONS_STORE_PATH, {});

for (const [channelId, messageId] of Object.entries(timersStore)) {
  if (channelId && messageId) {
    timersMessageByChannel.set(channelId, { messageId, intervalId: null, lastRenderedContent: null, lastVerifiedAt: 0 });
  }
}
for (const [channelId, messageId] of Object.entries(reservationsStore)) {
  if (channelId && messageId) {
    reservationsMessageByChannel.set(channelId, { messageId, intervalId: null, lastRenderedContent: null, lastVerifiedAt: 0 });
  }
}

function persistTimersStore() {
  const data = Object.fromEntries(
    Array.from(timersMessageByChannel.entries()).map(([channelId, entry]) => [channelId, entry?.messageId])
  );
  writeJsonSafe(TIMERS_STORE_PATH, data);
}

function persistReservationsStore() {
  const data = Object.fromEntries(
    Array.from(reservationsMessageByChannel.entries()).map(([channelId, entry]) => [channelId, entry?.messageId])
  );
  writeJsonSafe(RESERVATIONS_STORE_PATH, data);
}

function setTimersBackoff(isRateLimited = false) {
  timersLastFailureAt = Date.now();
  timersFailureStreak += 1;
  const backoffMs = isRateLimited
    ? Math.min(10 * 60_000, 60_000 * Math.pow(2, Math.max(0, timersFailureStreak - 1)))
    : Math.min(60_000, 5_000 * Math.pow(2, Math.max(0, timersFailureStreak - 1)));
  timersNextFetchAttemptAt = Date.now() + backoffMs;
}

function getAdaptiveSnapshotRefreshDelayMs() {
  if (timersFailureStreak >= 3) return SNAPSHOT_REFRESH_MAX_DELAY_MS;
  if (timersFailureStreak > 0) return SNAPSHOT_REFRESH_SLOW_DELAY_MS;
  if (lastSnapshotRefreshDurationMs >= SNAPSHOT_REFRESH_VERY_SLOW_MS) return SNAPSHOT_REFRESH_MAX_DELAY_MS;
  if (lastSnapshotRefreshDurationMs >= SNAPSHOT_REFRESH_SLOW_MS) return SNAPSHOT_REFRESH_SLOW_DELAY_MS;
  return TIMERS_REFRESH_MS;
}

function tryStartSnapshotReconcile() {
  if (snapshotReconcileInFlight) return;
  if (!pendingSnapshotReconcile) return;
  if (Date.now() - lastSnapshotReconcileAt < SNAPSHOT_RECONCILE_INTERVAL_MS) return;

  const payload = pendingSnapshotReconcile;
  pendingSnapshotReconcile = null;
  snapshotReconcileInFlight = true;
  lastSnapshotReconcileAt = Date.now();

  (async () => {
    const startedAt = Date.now();
    try {
      await reconcileDeletedReservations(payload.activeSerials);
      await reconcileReservationState(payload.rowStates);
      perfDuration("snapshot_reconcile", startedAt, {
        activeSerialCount: Array.isArray(payload.activeSerials) ? payload.activeSerials.length : 0,
        rowStateCount: Array.isArray(payload.rowStates) ? payload.rowStates.length : 0,
      });
    } catch (e) {
      perfDuration("snapshot_reconcile", startedAt, { error: true });
      console.error("snapshot reconcile error:", e);
    } finally {
      snapshotReconcileInFlight = false;
      tryStartSnapshotReconcile();
    }
  })();
}

function scheduleSnapshotReconcile(activeSerials, rowStates) {
  pendingSnapshotReconcile = {
    activeSerials: Array.isArray(activeSerials) ? activeSerials : [],
    rowStates: Array.isArray(rowStates) ? rowStates : [],
  };
  tryStartSnapshotReconcile();
}

async function getTextBasedChannel(client, channelId) {
  const key = String(channelId);
  const cached = client.channels.cache.get(key);
  if (cached?.isTextBased()) return cached;
  const fetched = await client.channels.fetch(key);
  return fetched?.isTextBased() ? fetched : null;
}

async function getChannelMessage(channel, messageId) {
  const key = String(messageId);
  const cached = channel.messages?.cache?.get(key);
  if (cached) return cached;
  return channel.messages.fetch(key);
}

function shouldSkipRenderedUpdate(entry, nextContent) {
  if (!entry) return false;
  if (entry.lastRenderedContent !== nextContent) return false;
  const lastVerifiedAt = Number(entry.lastVerifiedAt || 0);
  return Date.now() - lastVerifiedAt < CONTENT_VERIFY_INTERVAL_MS;
}

function markRenderedUpdate(entry, content) {
  if (!entry) return;
  entry.lastRenderedContent = content;
  entry.lastVerifiedAt = Date.now();
}

function startTimersInterval(client, channelId, messageId) {
  let inFlight = false;
  const tick = async () => {
    if (inFlight) return;
    const tickStartedAt = Date.now();
    let hadSnapshot = false;
    let skipReason = null;
    inFlight = true;
    try {
      const updated = await fetchTimersText({ cacheOnly: true });
      hadSnapshot = !!updated;
      if (!updated) return;

      const entry = timersMessageByChannel.get(channelId);
      if (entry?.messageId === messageId && shouldSkipRenderedUpdate(entry, updated)) {
        skipReason = "content_unchanged";
        return;
      }

      const ch = await getTextBasedChannel(client, channelId);
      if (!ch?.isTextBased()) return;
      const msg = await getChannelMessage(ch, messageId);
      const editStartedAt = Date.now();
      await msg.edit(updated);
      perfDuration("discord_message_edit", editStartedAt, { scope: "timers", channelId, messageId });
      if (entry?.messageId === messageId) {
        markRenderedUpdate(entry, updated);
      }
    } catch (e) {
      const rawCode = e && typeof e === "object"
        ? (e.code ?? e.rawError?.code ?? null)
        : null;
      const code = Number(rawCode);
      // Message missing: stop tracking instead of auto-recreating.
      if (code === 10008) {
        const entry = timersMessageByChannel.get(channelId);
        if (entry?.messageId === messageId) {
          timersMessageByChannel.delete(channelId);
          persistTimersStore();
        }
        console.log(`⚠️ Tracked /timers message missing in ${channelId}; auto-recreate disabled.`);
        clearInterval(intervalId);
        return;
      }
      // Channel missing: drop tracking entry.
      if (code === 10003) {
        const entry = timersMessageByChannel.get(channelId);
        if (entry?.messageId === messageId) {
          timersMessageByChannel.delete(channelId);
          persistTimersStore();
        }
        clearInterval(intervalId);
        return;
      }
      console.error("timers update failed:", e);
    } finally {
      inFlight = false;
      perfDuration("timers_tick", tickStartedAt, {
        channelId,
        messageId,
        hadSnapshot,
        skipped: !!skipReason,
        skipReason,
      });
    }
  };

  const intervalId = setInterval(tick, LIVE_MESSAGE_REFRESH_MS);
  // run once immediately to validate the stored message id
  tick();

  return intervalId;
}

function parseReservationUTC(resStr) {
  if (!resStr || typeof resStr !== "string") return null;
  const m = resStr.trim().match(/^(\d{2}):(\d{2})\s+(\d{2})\/(\d{2})\/(\d{4})$/);
  if (!m) return null;

  const hh = Number(m[1]);
  const mm = Number(m[2]);
  const dd = Number(m[3]);
  const MM = Number(m[4]);
  const yyyy = Number(m[5]);

  const utcMs = Date.UTC(yyyy, MM - 1, dd, hh, mm, 0, 0);
  if (!Number.isFinite(utcMs)) return null;
  return new Date(utcMs);
}

async function findExistingReservationsMessage(channel, client) {
  if (!channel?.isTextBased()) return null;
  const msgs = await channel.messages.fetch({ limit: 50 });
  return (
    msgs.find(
      (m) =>
        m?.author?.id === client?.user?.id &&
        typeof m.content === "string" &&
        m.content.includes("Next reservation time (UTC)")
    ) || null
  );
}

function isPanelMessage(msg, client) {
  if (!msg || msg.author?.id !== client?.user?.id) return false;
  const hasPanelTitle = Array.isArray(msg.embeds) && msg.embeds.some((e) => e?.title === "Title Requests");
  if (!hasPanelTitle) return false;
  return Array.isArray(msg.components) && msg.components.some((row) =>
    Array.isArray(row?.components) && row.components.some((component) => component?.customId === "select_title")
  );
}

async function findPanelMessages(channel, client) {
  if (!channel?.isTextBased()) return [];
  const msgs = await channel.messages.fetch({ limit: 50 });
  return msgs.filter((m) => isPanelMessage(m, client)).map((m) => m);
}

function startReservationsInterval(client, channelId, messageId) {
  let inFlight = false;
  let lastBottomCheckAt = 0;
  const BOTTOM_CHECK_MS = 3000;
  const tick = async () => {
    if (inFlight) return;
    const tickStartedAt = Date.now();
    let updated = null;
    let hadSnapshot = false;
    let skipReason = null;
    inFlight = true;
    try {
      updated = await fetchReservationsText({ cacheOnly: true });
      hadSnapshot = !!updated;
      if (!updated) return;

      const ch = await getTextBasedChannel(client, channelId);
      if (!ch?.isTextBased()) return;
      let shouldRepostAtBottom = false;
      if (Date.now() >= startupStickyDelayUntil && Date.now() - lastBottomCheckAt >= BOTTOM_CHECK_MS) {
        lastBottomCheckAt = Date.now();
        try {
          const latest = await ch.messages.fetch({ limit: 1 });
          const latestMsg = latest.first();
          shouldRepostAtBottom = !!latestMsg && latestMsg.id !== messageId;
        } catch {}
      }
      if (shouldRepostAtBottom) {
        // Always send a fresh message at bottom first to avoid delete->recreate gaps.
        const replacement = await ch.send(updated);
        try {
          const old = await ch.messages.fetch(messageId);
          await old.delete().catch(() => {});
        } catch {}
        const newIntervalId = startReservationsInterval(client, channelId, replacement.id);
        reservationsMessageByChannel.set(channelId, {
          messageId: replacement.id,
          intervalId: newIntervalId,
          lastRenderedContent: updated,
          lastVerifiedAt: Date.now(),
        });
        persistReservationsStore();
        clearInterval(intervalId);
        return;
      }
      const entry = reservationsMessageByChannel.get(channelId);
      if (entry?.messageId === messageId && shouldSkipRenderedUpdate(entry, updated)) {
        skipReason = "content_unchanged";
        return;
      }

      const msg = await getChannelMessage(ch, messageId);
      const editStartedAt = Date.now();
      await msg.edit(updated);
      perfDuration("discord_message_edit", editStartedAt, { scope: "reservations", channelId, messageId });
      if (entry?.messageId === messageId) {
        markRenderedUpdate(entry, updated);
      }
    } catch (e) {
      const rawCode = e && typeof e === "object"
        ? (e.code ?? e.rawError?.code ?? null)
        : null;
      const code = Number(rawCode);
      if (code === 10008) {
        try {
          const ch = await getTextBasedChannel(client, channelId);
          if (ch?.isTextBased()) {
            let replacement = await findExistingReservationsMessage(ch, client);
            if (replacement) {
              await replacement.edit(updated).catch(() => {});
            } else {
              replacement = await ch.send(updated);
            }
            const newIntervalId = startReservationsInterval(client, channelId, replacement.id);
            reservationsMessageByChannel.set(channelId, {
              messageId: replacement.id,
              intervalId: newIntervalId,
              lastRenderedContent: updated,
              lastVerifiedAt: Date.now(),
            });
            persistReservationsStore();
          }
        } catch (recreateErr) {
          console.error("reservations message recreate failed:", recreateErr);
          const entry = reservationsMessageByChannel.get(channelId);
          if (entry?.messageId === messageId) {
            reservationsMessageByChannel.delete(channelId);
            persistReservationsStore();
          }
        }
        clearInterval(intervalId);
        return;
      }
      if (code === 10003) {
        const entry = reservationsMessageByChannel.get(channelId);
        if (entry?.messageId === messageId) {
          reservationsMessageByChannel.delete(channelId);
          persistReservationsStore();
        }
        clearInterval(intervalId);
        return;
      }
      console.error("reservations update failed:", e);
    } finally {
      inFlight = false;
      perfDuration("reservations_tick", tickStartedAt, {
        channelId,
        messageId,
        hadSnapshot,
        skipped: !!skipReason,
        skipReason,
      });
    }
  };

  const intervalId = setInterval(tick, LIVE_MESSAGE_REFRESH_MS);
  tick();
  return intervalId;
}

function getRowValue(row, keys) {
  for (const key of keys) {
    if (Object.prototype.hasOwnProperty.call(row, key)) return row[key];
  }
  return "";
}

function parseBool(value) {
  if (value === true) return true;
  const s = String(value || "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes";
}

function parseDoneTimeMs(value) {
  if (value === null || value === undefined || value === "") return null;
  if (value instanceof Date && Number.isFinite(value.getTime())) return value.getTime();

  const raw = String(value).trim();
  if (!raw) return null;

  const dmy = raw.match(/^(\d{2})\/(\d{2})\/(\d{4})\s+(\d{2}):(\d{2})$/);
  if (dmy) {
    const dd = Number(dmy[1]);
    const MM = Number(dmy[2]);
    const yyyy = Number(dmy[3]);
    const hh = Number(dmy[4]);
    const mm = Number(dmy[5]);
    return Date.UTC(yyyy, MM - 1, dd, hh, mm, 0, 0);
  }

  const parsedMs = Date.parse(raw);
  if (Number.isFinite(parsedMs)) return parsedMs;
  return null;
}

async function fetchTimersSnapshotFromSheetDb() {
  const startedAt = Date.now();
  if (!SHEETDB_URL) {
    throw new Error("SHEETDB_URL not configured");
  }

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), SNAPSHOT_FETCH_TIMEOUT_MS);
  let res;
  try {
    res = await fetch(SHEETDB_URL, {
      method: "GET",
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
  } catch (e) {
    if (e?.name === "AbortError") {
      throw new Error(`SheetDB request timed out after ${SNAPSHOT_FETCH_TIMEOUT_MS}ms`);
    }
    throw e;
  } finally {
    clearTimeout(timeoutId);
  }
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`SheetDB HTTP ${res.status}: ${text.slice(0, 200)}`);
  }

  let rows;
  try {
    rows = JSON.parse(text);
  } catch {
    throw new Error("SheetDB returned non-JSON response");
  }
  if (!Array.isArray(rows)) {
    throw new Error("SheetDB response is not an array");
  }

  const titles = ["Governor", "Architect", "Prefect", "General"];
  const nowMs = Date.now();
  const activeSerials = [];
  const rowStates = [];
  const latestDoneByTitle = new Map();
  const nextReservationByTitle = new Map();
  const reservationStartsByTitle = new Map(titles.map((t) => [t, []]));

  for (const row of rows) {
    const serialRaw = getRowValue(row, ["Row ID", "ROW_ID", "row_id", "serial"]);
    const serial = Number(serialRaw);
    if (Number.isFinite(serial)) activeSerials.push(serial);

    const title = String(getRowValue(row, ["Title", "TITLE", "title"]) || "").trim();
    const done = parseBool(getRowValue(row, ["Done", "DONE", "done"]));
    const reservationRaw = String(getRowValue(row, ["Reservation (UTC)", "RESERVATION (UTC)", "reservationUtc", "reservation_utc"]) || "").trim();
    const reminder = parseBool(getRowValue(row, ["Reminder", "REMINDER", "reminder"]));
    const username = String(getRowValue(row, ["Username", "USERNAME", "username"]) || "").trim();
    const coordinates = String(getRowValue(row, ["Coordinates", "COORDINATES", "coordinates"]) || "").trim();

    if (Number.isFinite(serial)) {
      rowStates.push({
        serial,
        title,
        reservationUtc: reservationRaw || "—",
        reminder,
        done,
        username,
        coordinates,
      });
    }

    if (!titles.includes(title)) continue;

    if (done) {
      const doneMs = parseDoneTimeMs(getRowValue(row, ["Done Time", "DONE_TIME", "done_time", "doneTime"]));
      if (Number.isFinite(doneMs)) {
        const prev = latestDoneByTitle.get(title);
        if (!Number.isFinite(prev) || doneMs > prev) latestDoneByTitle.set(title, doneMs);
      }
    }

    if (!reservationRaw || reservationRaw === "—") continue;

    const resUtc = parseReservationUTC(reservationRaw);
    if (!resUtc) continue;
    const resMs = resUtc.getTime();
    if (!done && resMs >= nowMs - 3600_000) {
      reservationStartsByTitle.get(title).push(resMs);
    }
    if (done || resMs <= nowMs) continue;

    const prev = nextReservationByTitle.get(title);
    if (!prev || resMs < prev.resMs) {
      nextReservationByTitle.set(title, { resMs, reservationUtc: formatUTCDateTime(resUtc) });
    }
  }

  const timers = titles
    .filter((title) => Number.isFinite(latestDoneByTitle.get(title)))
    .map((title) => ({
      title,
      elapsedSeconds: Math.max(0, Math.floor((nowMs - latestDoneByTitle.get(title)) / 1000)),
    }));

  const nextReservations = titles
    .filter((title) => nextReservationByTitle.has(title))
    .map((title) => {
      const next = nextReservationByTitle.get(title);
      return {
        title,
        secondsUntil: Math.max(0, Math.floor((next.resMs - nowMs) / 1000)),
        reservationUtc: next.reservationUtc,
      };
    });

  const activeCooldowns = titles
    .map((title) => {
      const doneMs = latestDoneByTitle.get(title);
      if (!Number.isFinite(doneMs)) return null;
      const cooldownUntilMs = doneMs + 3600_000;
      if (cooldownUntilMs <= nowMs) return null;
      return {
        title,
        secondsUntil: Math.max(0, Math.floor((cooldownUntilMs - nowMs) / 1000)),
      };
    })
    .filter(Boolean);

  const nextPossibleReservations = titles.map((title) => {
    const starts = (reservationStartsByTitle.get(title) || []).slice().sort((a, b) => a - b);
    // minute granularity for stable output
    let candidateMs = Math.ceil(nowMs / 60000) * 60000;
    const doneMs = latestDoneByTitle.get(title);
    if (Number.isFinite(doneMs)) {
      const cooldownUntilMs = doneMs + 3600_000;
      if (cooldownUntilMs > candidateMs) candidateMs = cooldownUntilMs;
    }

    for (const startMs of starts) {
      if (candidateMs + 3600_000 <= startMs) break;
      const reservationEnd = startMs + 3600_000;
      if (candidateMs < reservationEnd) candidateMs = reservationEnd;
    }

    return {
      title,
      secondsUntil: Math.max(0, Math.floor((candidateMs - nowMs) / 1000)),
      reservationUtc: formatUTCDateTime(new Date(candidateMs)),
    };
  });

  perfDuration("sheetdb_snapshot_fetch", startedAt, { status: res.status, rowCount: rows.length });
  return { success: true, timers, nextReservations, nextPossibleReservations, activeCooldowns, activeSerials, rowStates };
}

function formatReservationForUserTimezone(reservationUtcStr, tz) {
  const utcDate = parseReservationUTC(reservationUtcStr);
  if (!utcDate) return `${reservationUtcStr} (UTC)`;

  if (tz?.type === "iana") {
    const local = DateTime.fromJSDate(utcDate, { zone: "utc" }).setZone(tz.zone);
    const dateFmt = getDateExampleFormat(tz) === "MM/dd/yy" ? "MM/dd/yyyy" : "dd/MM/yyyy";
    const short = local.offsetNameShort || tz.zone;
    return `${local.toFormat(`HH:mm ${dateFmt}`)} (${short})`;
  }

  if (tz?.type === "offset" && Number.isFinite(tz.offsetMinutes)) {
    const localMs = utcDate.getTime() + tz.offsetMinutes * 60_000;
    const localDate = new Date(localMs);
    const dateFmt = getDateExampleFormat(tz) === "MM/dd/yy" ? "MM/dd/yyyy" : "dd/MM/yyyy";
    const datePart =
      dateFmt === "MM/dd/yyyy"
        ? `${pad(localDate.getUTCMonth() + 1)}/${pad(localDate.getUTCDate())}/${localDate.getUTCFullYear()}`
        : `${pad(localDate.getUTCDate())}/${pad(localDate.getUTCMonth() + 1)}/${localDate.getUTCFullYear()}`;
    return `${pad(localDate.getUTCHours())}:${pad(localDate.getUTCMinutes())} ${datePart} (${formatUtcOffsetLabel(
      tz.offsetMinutes
    )})`;
  }

  return `${reservationUtcStr} (UTC)`;
}

function renderTimersTextFromSnapshot() {
  if (!timersSnapshot) return null;
  const ageSeconds = Math.max(0, Math.floor((Date.now() - timersSnapshotAt) / 1000));
  const byTitle = new Map((timersSnapshot.timers || []).map((t) => [String(t.title), Number(t.elapsedSeconds) + ageSeconds]));
  const nextByTitle = new Map((timersSnapshot.nextReservations || []).map((t) => [
    String(t.title),
    { secondsUntil: Number(t.secondsUntil) - ageSeconds, reservationUtc: String(t.reservationUtc || "") },
  ]));
  const activeCooldownByTitle = new Map((timersSnapshot.activeCooldowns || []).map((t) => [
    String(t.title),
    Number(t.secondsUntil) - ageSeconds,
  ]));
  const nextPossibleByTitle = new Map((timersSnapshot.nextPossibleReservations || []).map((t) => [
    String(t.title),
    { secondsUntil: Number(t.secondsUntil) - ageSeconds, reservationUtc: String(t.reservationUtc || "") },
  ]));
  const titles = ["Governor", "Architect", "Prefect", "General"];
  const formatRemaining = (title) => {
    const possible = nextPossibleByTitle.get(title);
    if (possible && Number.isFinite(possible.secondsUntil)) {
      if (possible.secondsUntil <= 59) return "Available";
      const mmTotal = Math.floor(possible.secondsUntil / 60);
      const hh = Math.floor(mmTotal / 60);
      const mm = mmTotal % 60;
      return hh > 0 ? `${hh}h ${mm}m` : `${mm}m`;
    }

    // Fallback for old Apps Script snapshot shape (elapsed-only).
    const elapsedSeconds = byTitle.get(title);
    if (!Number.isFinite(elapsedSeconds)) return "Available";
    const remaining = 3600 - elapsedSeconds;
    if (remaining <= 59) return "Available";
    const mm = Math.floor(remaining / 60);
    return `${mm}m`;
  };
  const formatNextPossible = (title, next) => {
    const hasUpcomingReservation = nextByTitle.has(title);
    const cooldownSeconds = Number(activeCooldownByTitle.get(title));
    const hasActiveCooldown = Number.isFinite(cooldownSeconds) && cooldownSeconds > 0;
    if (!hasUpcomingReservation && !hasActiveCooldown) return "Any time/date";
    if (!next || !Number.isFinite(next.secondsUntil) || !next.reservationUtc) return "Unknown";
    const res = next.reservationUtc;
    const match = res.match(/^(\d{2}):(\d{2})\s+(\d{2})\/(\d{2})\/(\d{4})$/);
    if (!match) return res;
    const timePart = `${match[1]}:${match[2]}`;
    const datePart = `${match[3]}/${match[4]}/${match[5]}`;
    const todayUtc = new Date().toISOString().slice(0, 10).split("-").reverse().join("/");
    return datePart === todayUtc ? timePart : `${timePart} ${datePart}`;
  };

  const lines = [
    "\u200b",
    "Next reservation time possible (UTC)",
    ...titles.map((t) => `-# ${t}: ${formatNextPossible(t, nextPossibleByTitle.get(t))}`),
    "",
    "Time left until title is available",
    ...titles.map((t) => `-# ${t}: ${formatRemaining(t)}`),
  ];
  const text = lines.join("\n");
  lastTimersText = text;
  lastTimersTextAt = Date.now();
  timersLastSuccessAt = lastTimersTextAt;
  return text;
}

async function fetchTimersText(opts = {}) {
  const cacheOnly = !!opts.cacheOnly;
  const now = Date.now();
  if (cacheOnly) {
    const rendered = renderTimersTextFromSnapshot();
    if (rendered) return rendered;
    if (lastTimersText && now - lastTimersTextAt <= TIMERS_CACHE_MAX_AGE_MS) return lastTimersText;
    return null;
  }
  if (now < timersNextFetchAttemptAt) {
    if (lastTimersText && now - lastTimersTextAt <= TIMERS_CACHE_MAX_AGE_MS) return lastTimersText;
    return null;
  }
  if (!timersSnapshot || now - timersSnapshotAt >= TIMERS_REFRESH_MS) {
    if (!timersSnapshotRefreshPromise) {
      timersSnapshotRefreshPromise = (async () => {
        let json;
        let source = SHEETDB_URL ? "sheetdb" : "apps_script";
        const fetchStartedAt = Date.now();
        try {
          if (SHEETDB_URL) {
            json = await fetchTimersSnapshotFromSheetDb();
          } else {
            json = await postToAppsScript({ action: "list_timers" });
          }
        } catch (e) {
          const msg = String(e?.message || e || "");
          const isRateLimited = /HTTP 429|Request limit exceeded/i.test(msg);
          if (Date.now() - timersLastFailureAt > 10_000) {
            console.error("list_timers fetch error:", e);
          }
          if (SHEETDB_URL && !isRateLimited) {
            source = "apps_script_fallback";
            try {
              json = await postToAppsScript({ action: "list_timers" });
            } catch {}
          }
          if (!json) {
            perfDuration("snapshot_data_fetch", fetchStartedAt, { source, error: true, rateLimited: isRateLimited });
            setTimersBackoff(isRateLimited);
            return false;
          }
        }

        const fetchOk = !!json?.success;
        perfDuration("snapshot_data_fetch", fetchStartedAt, { source, ok: fetchOk });

        if (!json?.success) {
          if (Date.now() - timersLastFailureAt > 10_000) {
            console.error("list_timers failed:", json);
          }
          const isRateLimited = /429|limit/i.test(JSON.stringify(json || ""));
          setTimersBackoff(isRateLimited);
          return false;
        }

        const postProcessStartedAt = Date.now();
        timersFailureStreak = 0;
        timersNextFetchAttemptAt = 0;
        timersSnapshot = json;
        timersSnapshotAt = Date.now();
        scheduleSnapshotReconcile(json.activeSerials, json.rowStates);
        perfDuration("snapshot_postprocess", postProcessStartedAt, {
          activeSerialCount: Array.isArray(json.activeSerials) ? json.activeSerials.length : 0,
          rowStateCount: Array.isArray(json.rowStates) ? json.rowStates.length : 0,
        });
        return true;
      })().finally(() => {
        timersSnapshotRefreshPromise = null;
      });
    }

    const refreshed = await timersSnapshotRefreshPromise;
    if (!refreshed && (!lastTimersText || Date.now() - lastTimersTextAt > TIMERS_CACHE_MAX_AGE_MS)) {
      return null;
    }
    if (!refreshed) return lastTimersText;
  }
  return renderTimersTextFromSnapshot();
}

function renderReservationsTextFromSnapshot() {
  if (!timersSnapshot) return null;
  const ageSeconds = Math.max(0, Math.floor((Date.now() - timersSnapshotAt) / 1000));
  const nextByTitle = new Map((timersSnapshot.nextReservations || []).map((t) => [
    String(t.title),
    { secondsUntil: Number(t.secondsUntil) - ageSeconds, reservationUtc: String(t.reservationUtc || "") },
  ]));
  const titles = ["Governor", "Architect", "Prefect", "General"];
  const formatUntil = (next) => {
    if (!next || !Number.isFinite(next.secondsUntil)) return "No reservation";
    if (next.secondsUntil <= 0) return "No reservation";
    const mmTotal = Math.floor(next.secondsUntil / 60);
    const hh = Math.floor(mmTotal / 60);
    const mm = mmTotal % 60;
    const remainingStr = hh > 0 ? `${hh}h ${mm}m` : `${mm}m`;
    const res = next.reservationUtc;
    const match = res.match(/^(\d{2}):(\d{2})\s+(\d{2})\/(\d{2})\/(\d{4})$/);
    if (!match) return `Next reservation in ${mm}m`;
    const timePart = `${match[1]}:${match[2]}`;
    const datePart = `${match[3]}/${match[4]}/${match[5]}`;
    const todayUtc = new Date().toISOString().slice(0, 10).split("-").reverse().join("/");
    if (datePart === todayUtc) {
      return `Next reservation at ${timePart}, in ${remainingStr}`;
    }
    return `Next reservation at ${timePart} ${datePart}, in ${remainingStr}`;
  };

  return [
    "\u200b",
    "Next reservation time (UTC)",
    ...titles.map((t) => `-# ${t}: ${formatUntil(nextByTitle.get(t))}`),
  ].join("\n");
}

async function fetchReservationsText(opts = {}) {
  const cacheOnly = !!opts.cacheOnly;
  const now = Date.now();
  if (cacheOnly) return renderReservationsTextFromSnapshot();
  if (now < timersNextFetchAttemptAt) return null;
  if (!timersSnapshot || now - timersSnapshotAt >= TIMERS_REFRESH_MS) {
    await fetchTimersText();
    if (!timersSnapshot) return null;
  }
  return renderReservationsTextFromSnapshot();
}

async function refreshTimersSnapshotInBackground() {
  if (timersSnapshotBackgroundInFlight) return;
  timersSnapshotBackgroundInFlight = true;
  const startedAt = Date.now();
  try {
    const text = await fetchTimersText();
    lastSnapshotRefreshDurationMs = perfDuration("snapshot_refresh_loop", startedAt, { ok: !!text });
  } catch (e) {
    lastSnapshotRefreshDurationMs = perfDuration("snapshot_refresh_loop", startedAt, { ok: false, error: true });
    throw e;
  } finally {
    timersSnapshotBackgroundInFlight = false;
  }
}

function scheduleNextSnapshotRefreshTick() {
  const delayMs = getAdaptiveSnapshotRefreshDelayMs();
  if (timersSnapshotBackgroundIntervalId) {
    clearTimeout(timersSnapshotBackgroundIntervalId);
  }
  timersSnapshotBackgroundIntervalId = setTimeout(() => {
    refreshTimersSnapshotInBackground()
      .catch((e) => {
        console.error("snapshot refresh loop error:", e);
      })
      .finally(() => {
        scheduleNextSnapshotRefreshTick();
      });
  }, delayMs);
}

function startSnapshotRefreshLoop() {
  if (timersSnapshotBackgroundIntervalId) {
    clearTimeout(timersSnapshotBackgroundIntervalId);
  }
  refreshTimersSnapshotInBackground().catch((e) => {
    console.error("snapshot warmup failed:", e);
  }).finally(() => {
    scheduleNextSnapshotRefreshTick();
  });
}

async function updateTimersMessage(client, channelId) {
  const entry = timersMessageByChannel.get(channelId);
  if (!entry?.messageId) return;

  const text = await fetchTimersText({ cacheOnly: true });
  if (!text) return;
  if (shouldSkipRenderedUpdate(entry, text)) return;

  try {
    const ch = await getTextBasedChannel(client, channelId);
    if (!ch?.isTextBased()) return;
    const msg = await getChannelMessage(ch, entry.messageId);
    const editStartedAt = Date.now();
    await msg.edit(text);
    perfDuration("discord_message_edit", editStartedAt, { scope: "timers_immediate", channelId, messageId: entry.messageId });
    markRenderedUpdate(entry, text);
  } catch (e) {
    console.error("timers immediate update failed:", e);
  }
}

async function updateAllTimersMessages(client) {
  if (timersMessageByChannel.size === 0) return;
  const text = await fetchTimersText({ cacheOnly: true });
  if (!text) return;

  await Promise.all(
    Array.from(timersMessageByChannel.entries()).map(async ([channelId, entry]) => {
      if (!entry?.messageId) return;
      if (shouldSkipRenderedUpdate(entry, text)) return;
      try {
        const ch = await getTextBasedChannel(client, channelId);
        if (!ch?.isTextBased()) return;
        const msg = await getChannelMessage(ch, entry.messageId);
        const editStartedAt = Date.now();
        await msg.edit(text);
        perfDuration("discord_message_edit", editStartedAt, { scope: "timers_bulk", channelId, messageId: entry.messageId });
        markRenderedUpdate(entry, text);
      } catch (e) {
        const code = getDiscordErrorCode(e);
        if (code === 10008 || code === 10003) {
          if (entry?.intervalId) clearInterval(entry.intervalId);
          timersMessageByChannel.delete(channelId);
          persistTimersStore();
          return;
        }
        console.error("timers immediate update failed:", e);
      }
    })
  );
}

async function updateAllReservationsMessages(client) {
  if (reservationsMessageByChannel.size === 0) return;
  const text = await fetchReservationsText({ cacheOnly: true });
  if (!text) return;

  await Promise.all(
    Array.from(reservationsMessageByChannel.entries()).map(async ([channelId, entry]) => {
      if (!entry?.messageId) return;
      if (shouldSkipRenderedUpdate(entry, text)) return;
      try {
        const ch = await getTextBasedChannel(client, channelId);
        if (!ch?.isTextBased()) return;
        const msg = await getChannelMessage(ch, entry.messageId);
        const editStartedAt = Date.now();
        await msg.edit(text);
        perfDuration("discord_message_edit", editStartedAt, { scope: "reservations_bulk", channelId, messageId: entry.messageId });
        markRenderedUpdate(entry, text);
      } catch (e) {
        const code = getDiscordErrorCode(e);
        if (code === 10008 || code === 10003) {
          if (entry?.intervalId) clearInterval(entry.intervalId);
          reservationsMessageByChannel.delete(channelId);
          persistReservationsStore();
          return;
        }
        console.error("reservations immediate update failed:", e);
      }
    })
  );
}

function cancelReminder(rowSerial, keepMetaIfFired = false) {
  const key = String(rowSerial);
  const t = reminderTimers.get(key);
  if (t) clearTimeout(t);
  reminderTimers.delete(key);
  const meta = reminderMeta.get(key);
  if (keepMetaIfFired && meta?.fired) return;
  reminderMeta.delete(key);
}

async function deleteMessageRef(client, channelId, messageId) {
  if (!client || !channelId || !messageId) return;
  try {
    const ch = await client.channels.fetch(String(channelId));
    if (!ch?.isTextBased()) return;
    const msg = await ch.messages.fetch(String(messageId));
    await msg.delete().catch(() => {});
  } catch {}
}

async function reconcileDeletedReservations(activeSerials) {
  if (orphanReservationCleanupInFlight) return;
  if (!Array.isArray(activeSerials)) return;
  if (!runtimeClient) return;

  orphanReservationCleanupInFlight = true;
  try {
    const activeSet = new Set(activeSerials.map((s) => String(s)).filter(Boolean));
    const missing = Array.from(reservationMessages.keys()).filter((serial) => !activeSet.has(serial));
    if (missing.length === 0) return;

    let ownersChanged = false;
    for (const serial of missing) {
      const ref = reservationMessages.get(serial) || {};
      cancelReminder(serial);
      if (reservationOwners.delete(serial)) ownersChanged = true;

      await deleteMessageRef(runtimeClient, ref.requestChannelId, ref.requestMessageId);
      await deleteMessageRef(runtimeClient, ref.reminderChannelId, ref.reminderMessageId);

      reservationMessages.delete(serial);
      auditLog("manual_row_delete_sync", { rowSerial: serial });
    }

    if (ownersChanged) persistReservationOwners();
    persistReservationMessages();
    updateAllTimersMessages(runtimeClient);
    updateAllReservationsMessages(runtimeClient);
  } finally {
    orphanReservationCleanupInFlight = false;
  }
}

function upsertEmbedField(fields, name, value, inline = true) {
  const idx = fields.findIndex((f) => String(f.name || "").toLowerCase().includes(String(name).toLowerCase()));
  if (idx >= 0) {
    fields[idx] = { ...fields[idx], value: String(value), inline: fields[idx].inline ?? inline };
  } else {
    fields.push({ name, value: String(value), inline });
  }
}

function parseCurrentCustomIds(message) {
  const ids = [];
  for (const row of message?.components || []) {
    for (const component of row?.components || []) {
      if (component?.customId) ids.push(String(component.customId));
    }
  }
  return ids;
}

function getDoneStateOverride(serial) {
  const key = String(serial);
  const override = doneStateOverrides.get(key);
  if (!override) return null;
  if ((override.expiresAt || 0) <= Date.now()) {
    doneStateOverrides.delete(key);
    return null;
  }
  return override;
}

function setDoneStateOverride(serial, done) {
  doneStateOverrides.set(String(serial), {
    done: !!done,
    expiresAt: Date.now() + DONE_STATE_OVERRIDE_TTL_MS,
  });
}

function getEffectiveDoneState(serial, sheetDone) {
  const sheetDoneBool = !!sheetDone;
  const override = getDoneStateOverride(serial);
  if (!override) return sheetDoneBool;
  if (override.done === sheetDoneBool) {
    doneStateOverrides.delete(String(serial));
    return sheetDoneBool;
  }
  return override.done;
}

async function reconcileReservationState(rowStates) {
  if (reservationStateSyncInFlight) return;
  if (!Array.isArray(rowStates) || !runtimeClient) return;

  reservationStateSyncInFlight = true;
  try {
    for (const rowState of rowStates) {
      const serial = String(rowState?.serial || "");
      if (!serial) continue;
      const ref = reservationMessages.get(serial);
      if (!ref?.requestChannelId || !ref?.requestMessageId) continue;

      let requestMsg = null;
      try {
        const requestChannel = await runtimeClient.channels.fetch(ref.requestChannelId);
        if (requestChannel?.isTextBased()) {
          requestMsg = await requestChannel.messages.fetch(ref.requestMessageId);
        }
      } catch {}
      if (!requestMsg) continue;

      const reservationStr = String(rowState.reservationUtc || "—").trim() || "—";
      const completed = getEffectiveDoneState(serial, rowState.done);
      const reminderEnabled = !!rowState.reminder && reservationStr !== "—" && !completed;
      const currentIds = parseCurrentCustomIds(requestMsg);
      const showPing = currentIds.some((id) => id.startsWith("ping_"));
      const removeConfirm = currentIds.some((id) => id.startsWith("remove_confirm_"));
      const pingUserId = getDiscordUserIdFromEmbed(requestMsg) || getReservationOwner(serial);

      const existingEmbed = requestMsg.embeds?.[0] || null;
      const embed = existingEmbed ? EmbedBuilder.from(existingEmbed) : new EmbedBuilder().setTitle("📋 New Title Request");
      const fields = (existingEmbed?.fields || []).map((f) => ({ name: f.name, value: f.value, inline: f.inline }));
      if (String(rowState.username || "").trim()) {
        upsertEmbedField(fields, "Username", String(rowState.username).trim(), true);
      }
      if (String(rowState.coordinates || "").trim()) {
        upsertEmbedField(fields, "Coordinates", String(rowState.coordinates).trim(), true);
      }
      if (String(rowState.title || "").trim()) {
        upsertEmbedField(fields, "Title", String(rowState.title).trim(), true);
      }
      upsertEmbedField(fields, "Reservation", reservationStr, true);
      embed.setFields(fields);
      if (completed) {
        embed.setColor(0x777777).setFooter({ text: "Completed" });
      } else {
        embed.setColor(0x00ff00).setFooter(null);
      }

      const actionRow = buildRequestActionRow(
        serial,
        reservationStr,
        reminderEnabled ? "cancel" : "arm",
        completed,
        pingUserId,
        showPing,
        removeConfirm
      );

      try {
        await requestMsg.edit({ embeds: [embed], components: [actionRow] });
      } catch {}

      if (!reminderEnabled) {
        cancelReminder(serial);
        continue;
      }

      const reservationUtc = parseReservationUTC(reservationStr);
      if (!reservationUtc || reservationUtc.getTime() <= Date.now()) {
        cancelReminder(serial);
        continue;
      }

      const existingMeta = reminderMeta.get(serial) || {};
      if (reminderTimers.has(serial) && existingMeta.reservationStr === reservationStr) {
        continue;
      }

      const title = String(rowState.title || getTitleFromEmbed(requestMsg) || existingMeta.title || "").trim();
      const username = String(rowState.username || getUsernameFromEmbed(requestMsg) || existingMeta.username || "").trim();
      const coordinates = String(rowState.coordinates || getCoordinatesFromEmbed(requestMsg) || existingMeta.coordinates || "").trim();
      const discordMention = pingUserId ? `<@${pingUserId}>` : (existingMeta.discordMention || null);

      scheduleReminder({
        client: runtimeClient,
        rowSerial: serial,
        reservationUtc,
        channelId: existingMeta.channelId || FORM_CHANNEL_ID,
        sourceMessageUrl: requestMsg.url,
        title,
        username,
        coordinates,
        discordMention,
        reservationStr,
      });
    }
  } finally {
    reservationStateSyncInFlight = false;
  }
}

function getFieldFromEmbed(message, fieldNameLowerIncludes) {
  const emb = message.embeds?.[0];
  if (!emb) return null;
  const f = emb.fields?.find((x) => typeof x.name === "string" && x.name.toLowerCase().includes(fieldNameLowerIncludes));
  return f?.value ?? null;
}

function getReservationFromEmbed(message) {
  return getFieldFromEmbed(message, "reservation");
}
function getUsernameFromEmbed(message) {
  const field = getFieldFromEmbed(message, "username");
  if (field) return field;
  const emb = message.embeds?.[0];
  const desc = emb?.description || "";
  const m = String(desc).match(/\*\*(.+?)\*\*\s+for\s+\*\*(.+?)\*\*/i);
  if (m) return m[2];
  return null;
}
function getTitleFromEmbed(message) {
  // In your embed you used field name "Title" and value = titleShort
  const field = getFieldFromEmbed(message, "title");
  if (field) return field;
  const emb = message.embeds?.[0];
  const desc = emb?.description || "";
  const m = String(desc).match(/\*\*(.+?)\*\*\s+for\s+\*\*(.+?)\*\*/i);
  if (m) return m[1];
  return null;
}
function getCoordinatesFromEmbed(message) {
  return getFieldFromEmbed(message, "coordinate");
}
function getDiscordUserFromEmbed(message) {
  return getFieldFromEmbed(message, "discord");
}
function getDiscordUserIdFromEmbed(message) {
  const mention = getDiscordUserFromEmbed(message);
  const match = String(mention || "").match(/<@!?(\d+)>/);
  return match ? match[1] : null;
}
function resolvePingUserId(message, rowSerial, explicitUserId = null) {
  if (explicitUserId) return String(explicitUserId);
  const fromEmbed = getDiscordUserIdFromEmbed(message);
  if (fromEmbed) return String(fromEmbed);
  const fromOwner = rowSerial ? getReservationOwner(rowSerial) : null;
  return fromOwner ? String(fromOwner) : null;
}
function getCompletedFromEmbed(message) {
  const emb = message.embeds?.[0];
  const footerText = emb?.footer?.text || "";
  return String(footerText).toLowerCase().includes("completed");
}
function isReminderMessage(message) {
  const emb = message.embeds?.[0];
  const title = emb?.title || "";
  return String(title).toLowerCase().includes("reservation reminder");
}
function extractDiscordMessageLink(content) {
  const match = String(content || "").match(/https?:\/\/(?:canary\.|ptb\.)?discord\.com\/channels\/(\d+)\/(\d+)\/(\d+)/);
  if (!match) return null;
  return { channelId: match[2], messageId: match[3] };
}
function extractDiscordMessageLinkFromUrl(url) {
  const match = String(url || "").match(/https?:\/\/(?:canary\.|ptb\.)?discord\.com\/channels\/(\d+)\/(\d+)\/(\d+)/);
  if (!match) return null;
  return { channelId: match[2], messageId: match[3] };
}
async function updateOriginalRequestFromReminder(client, reminderMessage, rowSerial, completedOverride, showPingOverride) {
  const link = extractDiscordMessageLink(reminderMessage?.content);
  if (!link) return false;

  const ch = await client.channels.fetch(link.channelId);
  if (!ch?.isTextBased()) return false;

  const msg = await ch.messages.fetch(link.messageId);
  const reservationStr = getReservationFromEmbed(msg);
  const completed = typeof completedOverride === "boolean" ? completedOverride : getCompletedFromEmbed(msg);
  const pingUserId = resolvePingUserId(msg, rowSerial);
  const base = msg.embeds?.[0]
    ? EmbedBuilder.from(msg.embeds[0])
    : new EmbedBuilder().setTitle("📋 Title Request");
  if (completed) {
    base.setColor(0x777777).setFooter({ text: "Completed" });
  } else {
    base.setColor(0x00ff00).setFooter(null);
  }

  const showPing = typeof showPingOverride === "boolean" ? showPingOverride : true;
  const updatedRow = buildRequestActionRow(rowSerial, reservationStr, "arm", completed, pingUserId, showPing);
  await msg.edit({ embeds: [base], components: [updatedRow] });
  return true;
}

async function updateReminderMessagePingVisibility(client, rowSerial, showPing, completedOverride) {
  const meta = reminderMeta.get(String(rowSerial));
  if (!meta?.reminderMessageId || !meta?.reminderChannelId) return false;
  const ch = await client.channels.fetch(meta.reminderChannelId);
  if (!ch?.isTextBased()) return false;
  const msg = await ch.messages.fetch(meta.reminderMessageId);
  const completed = typeof completedOverride === "boolean" ? completedOverride : getCompletedFromEmbed(msg);
  const pingUserId = meta.discordMention ? String(meta.discordMention).match(/<@!?(\d+)>/)?.[1] : null;
  const row = buildReminderActionRow(rowSerial, completed, pingUserId, showPing);
  await msg.edit({ components: [row] });
  return true;
}

async function syncLinkedDoneState(client, sourceMessage, rowSerial, completed, showPing = true) {
  if (isReminderMessage(sourceMessage)) {
    return updateOriginalRequestFromReminder(client, sourceMessage, rowSerial, completed, showPing);
  }
  return updateReminderMessagePingVisibility(client, rowSerial, showPing, completed);
}

async function postReminder({ client, channelId, rowSerial, title, username, coordinates, discordMention, reservationStr, sourceMessageUrl }) {
  const targetChannelId = REMINDER_CHANNEL_ID || channelId;
  const ch = await client.channels.fetch(targetChannelId);
  if (!ch?.isTextBased()) return;

  const jump = sourceMessageUrl ? `\n${sourceMessageUrl}` : "";

  const embed = new EmbedBuilder()
    .setTitle("🔔 Reservation reminder")
    .setDescription(`**${title || "Title"}** for **${username || "Username"}**\n**Coords:** ${coordinates || "—"}`)
    .setColor(0xffcc00);

  const pingUserId = discordMention ? String(discordMention).match(/<@!?(\d+)>/)?.[1] : null;

  // Reminder message: Done + Ping (Ping only when completed)
  const row = buildReminderActionRow(rowSerial, false, pingUserId, true);

  const guardianMention = GUARDIAN_ID ? `<@&${GUARDIAN_ID}>` : "@guardian";

  const sent = await ch.send({
    content: `${guardianMention} Row #${rowSerial}${jump}`,
    embeds: [embed],
    components: [row],
  });
  setReservationReminderMessage(rowSerial, sent.channelId, sent.id);

  const key = String(rowSerial);
  const meta = reminderMeta.get(key) || {};
  reminderMeta.set(key, {
    ...meta,
    fired: true,
    firedAt: Date.now(),
    reminderMessageId: sent.id,
    reminderChannelId: sent.channelId,
  });

  if (sourceMessageUrl) {
    const ref = extractDiscordMessageLinkFromUrl(sourceMessageUrl);
    if (ref) {
      try {
        const sourceChannel = await client.channels.fetch(ref.channelId);
        if (sourceChannel?.isTextBased()) {
          const sourceMsg = await sourceChannel.messages.fetch(ref.messageId);
          const completed = getCompletedFromEmbed(sourceMsg);
          const reservation = getReservationFromEmbed(sourceMsg);
          const pingUserId = resolvePingUserId(sourceMsg, rowSerial);
          const updatedRow = buildRequestActionRow(rowSerial, reservation, "arm", completed, pingUserId, true);
          await sourceMsg.edit({ components: [updatedRow] });
        }
      } catch (e) {
        console.error("Failed to update original message after reminder:", e);
      }
    }
  }
}

function scheduleReminder({ client, rowSerial, reservationUtc, channelId, sourceMessageUrl, title, username, coordinates, discordMention, reservationStr }) {
  const key = String(rowSerial);
  const remindAtMs = reservationUtc.getTime();

  // overwrite existing
  cancelReminder(key);

  // keep meta so the reminder can print correctly
  reminderMeta.set(key, { title, username, coordinates, discordMention, channelId, sourceUrl: sourceMessageUrl, reservationStr });

  const arm = () => {
    const now = Date.now();
    const remaining = remindAtMs - now;

    if (remaining <= 0) {
      reminderTimers.delete(key);
      const meta = reminderMeta.get(key) || {};
      return postReminder({
        client,
        channelId: meta.channelId || channelId,
        rowSerial: key,
        title: meta.title || title,
        username: meta.username || username,
        coordinates: meta.coordinates ?? coordinates,
        discordMention: meta.discordMention || discordMention,
        reservationStr: meta.reservationStr || reservationStr,
        sourceMessageUrl: meta.sourceUrl || sourceMessageUrl,
      });
    }

    const nextDelay = Math.min(remaining, MAX_TIMEOUT_MS);
    const t = setTimeout(arm, nextDelay);
    reminderTimers.set(key, t);
  };

  arm();
}

// =====================
// UI BUILDERS
// =====================
function buildPanelPayload() {
  const embed = new EmbedBuilder()
    .setTitle("Title Requests")
    .setDescription("Select your requested title:")
    .setColor(0x2b2d31);

  return { embeds: [embed], components: buildPanelComponents() };
}

function buildPanelComponents() {
  return [buildTitleSelectRow(), buildPanelActionsRow()];
}

function buildTitleSelectRow() {
  const select = new StringSelectMenuBuilder()
    .setCustomId("select_title")
    .setPlaceholder("Choose a title…")
    .addOptions(
      TITLES.map((t) => ({
        label: t.label,
        description: t.description,
        value: t.value,
      }))
    );

  return new ActionRowBuilder().addComponents(select);
}

function buildPanelActionsRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("check_reservations")
      .setLabel("Check Reservations")
      .setStyle(ButtonStyle.Primary)
  );
}

function buildReservationsRefreshRow() {
  return new ActionRowBuilder().addComponents(
    new ButtonBuilder()
      .setCustomId("check_reservations_refresh")
      .setLabel("Refresh")
      .setStyle(ButtonStyle.Secondary)
  );
}

const TZ_NAMES = {
  "-12": "AoE",
  "-11": "SST",
  "-10": "HST",
  "-9": "AKST",
  "-8": "PST",
  "-7": "MST",
  "-6": "CST",
  "-5": "EST",
  "-4": "AST",
  "-3": "ART",
  "-2": "GST",
  "-1": "AZOT",
  "0": "UTC",
  "1": "CET",
  "2": "EET",
  "3": "MSK",
  "4": "GST",
  "5": "PKT",
  "6": "BST",
  "7": "ICT",
  "8": "CST",
  "9": "JST",
  "10": "AEST",
  "11": "SBT",
  "12": "NZST",
};

function formatUtcOffsetLabel(offsetMinutes) {
  if (!Number.isFinite(offsetMinutes)) return "UTC";
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const abs = Math.abs(offsetMinutes);
  const hh = Math.floor(abs / 60);
  const mm = String(abs % 60).padStart(2, "0");
  return mm === "00" ? `UTC${sign}${hh}` : `UTC${sign}${hh}:${mm}`;
}

function getTimezoneLabel(tz) {
  if (!tz) return "UTC";
  if (tz.type === "iana") {
    const dt = DateTime.now().setZone(tz.zone);
    const offsetLabel = formatUtcOffsetLabel(dt.offset);
    if (tz.zone === "Europe/London") return `GMT - ${offsetLabel}`;
    const name = String(dt.offsetNameShort || tz.zone)
      .replace(/\bGMT\b/gi, "UTC")
      .replace(/^UTC\s*([+-]\d+(?::\d+)?)$/i, "UTC$1");
    if (/^UTC(?:[+-]\d+(?::\d+)?)?$/i.test(name)) return offsetLabel;
    return `${name} - ${offsetLabel}`;
  }
  const hours = Math.round(tz.offsetMinutes / 60);
  const tzName = TZ_NAMES[String(hours)];
  const offsetLabel = formatUtcOffsetLabel(tz.offsetMinutes);
  if (tzName) return `${tzName} - ${offsetLabel}`;
  return offsetLabel;
}

function getDateExampleFormat(tz) {
  if (tz?.type === "iana" && tz.zone.startsWith("America/")) {
    return "MM/dd/yy";
  }
  return "dd/MM/yy";
}

const IANA_TIMEZONE_ZONES = [
  "Europe/London",
  "Europe/Paris",
  "Europe/Athens",
  "Europe/Moscow",
  "America/New_York",
  "America/Chicago",
  "America/Denver",
  "America/Los_Angeles",
  "America/Anchorage",
  "America/Halifax",
  "America/Sao_Paulo",
  "Pacific/Honolulu",
  "Asia/Dubai",
  "Asia/Kolkata",
  "Asia/Bangkok",
  "Asia/Singapore",
  "Asia/Seoul",
  "Asia/Tokyo",
  "Australia/Perth",
  "Australia/Sydney",
  "Pacific/Auckland",
];

function formatOffsetShort(offsetMinutes) {
  if (!Number.isFinite(offsetMinutes)) return "+0";
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const abs = Math.abs(offsetMinutes);
  const hh = Math.floor(abs / 60);
  const mm = abs % 60;
  return mm === 0 ? `${sign}${hh}` : `${sign}${hh}:${String(mm).padStart(2, "0")}`;
}

function buildIanaOption(zone) {
  const dt = DateTime.now().setZone(zone);
  const utcLabel = formatUtcOffsetLabel(dt.offset);
  const customZoneAbbr = {
    "Europe/London": "GMT",
    "Europe/Paris": "CET",
    "Europe/Athens": "EET",
    "Europe/Moscow": "MSK",
    "America/New_York": "EST",
    "America/Chicago": "CST",
    "America/Denver": "MST",
    "America/Los_Angeles": "PST",
    "America/Anchorage": "AKST",
    "America/Halifax": "AST",
    "America/Sao_Paulo": "BRT",
    "Pacific/Honolulu": "HST",
    "Asia/Dubai": "GST",
    "Asia/Kolkata": "IST",
    "Asia/Bangkok": "ICT",
    "Asia/Singapore": "SGT",
    "Asia/Seoul": "KST",
    "Asia/Tokyo": "JST",
    "Australia/Perth": "AWST",
    "Australia/Sydney": "AEST",
    "Pacific/Auckland": "NZST",
  };
  const abbr = customZoneAbbr[zone];
  if (abbr) {
    return { label: `${zone} (${abbr}, ${utcLabel})`, value: zone };
  }

  const rawName = String(dt.offsetNameShort || zone);
  const name = rawName.replace(/\bGMT\b/gi, "UTC").replace(/^UTC\s*([+-]\d+(?::\d+)?)$/i, "UTC$1");
  if (/^UTC(?:[+-]\d+(?::\d+)?)?$/i.test(name)) {
    return { label: `${zone} (${utcLabel})`, value: zone };
  }
  return { label: `${zone} (${name} - ${utcLabel})`, value: zone };
}

function getTimezoneMenuLabel(zone) {
  return buildIanaOption(zone).label;
}

function buildTimezoneSelectRow(customId = "select_timezone_iana") {
  const select = new StringSelectMenuBuilder()
    .setCustomId(customId)
    .setPlaceholder("Choose your timezone (region)…")
    .addOptions(IANA_TIMEZONE_ZONES.map(buildIanaOption));

  return new ActionRowBuilder().addComponents(select);
}

function buildRequestModal(userId, tz, pickedTitleLabel, pickedTitleDescription) {
  const descAbbrevMap = {
    "Recruitment speed +10%": "recruit. +10%",
    "Construction speed +10%": "const. +10%",
    "Research speed +10%": "research +10%",
    "Bender attack +5%": "Attack +5%",
  };
  const pickedAbbrev = descAbbrevMap[pickedTitleDescription] || "";
  let modalTitle = pickedTitleLabel
    ? `Request for ${pickedTitleLabel}${pickedAbbrev ? ` (${pickedAbbrev})` : ""}`
    : "Title Request";
  if (modalTitle.length > 45) modalTitle = `${modalTitle.slice(0, 42)}...`;
  const modal = new ModalBuilder().setCustomId("title_request_modal").setTitle(modalTitle);
  const prefill = userId ? getUserPrefill(userId) : { username: "", coordinates: "" };
  const tzLabel = getTimezoneLabel(tz);
  const dateExample = getDateExampleFormat(tz);

  const username = new TextInputBuilder()
    .setCustomId("username")
    .setLabel("Game Username")
    .setStyle(TextInputStyle.Short)
    .setRequired(true);
  if (prefill.username) username.setValue(prefill.username);

  const coords = new TextInputBuilder()
    .setCustomId("coordinates")
    .setLabel("Coords (e.g. 123:456)")
    .setStyle(TextInputStyle.Short)
    .setRequired(true)
    .setPlaceholder("123:456");
  if (prefill.coordinates) coords.setValue(prefill.coordinates);

  const resTime = new TextInputBuilder()
    .setCustomId("reservation_time")
    .setLabel(`Reservation Time (${tzLabel})`)
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setPlaceholder("now - 14:30 - 1pm");

  const resDate = new TextInputBuilder()
    .setCustomId("reservation_date")
    .setLabel(`Reservation Date (${tzLabel})`)
    .setStyle(TextInputStyle.Short)
    .setRequired(false)
    .setPlaceholder(`${dateExample}`);

  const comments = new TextInputBuilder()
    .setCustomId("comments")
    .setLabel("Comments")
    .setStyle(TextInputStyle.Paragraph)
    .setRequired(false)
    .setPlaceholder("Anything the guardian should know…");

  modal.addComponents(
    new ActionRowBuilder().addComponents(username),
    new ActionRowBuilder().addComponents(coords),
    new ActionRowBuilder().addComponents(resTime),
    new ActionRowBuilder().addComponents(resDate),
    new ActionRowBuilder().addComponents(comments)
  );

  return modal;
}

function buildDoneButton(rowSerial, completed, pingUserId) {
  const suffix = pingUserId ? `_${pingUserId}` : "";
  return new ButtonBuilder()
    .setCustomId(`done_${rowSerial}${suffix}`)
    .setLabel(completed ? "Not Done" : "Mark Done")
    .setStyle(completed ? ButtonStyle.Secondary : ButtonStyle.Success);
}

function buildReminderActionRow(rowSerial, completed, pingUserId, showPing = true) {
  const buttons = [buildDoneButton(rowSerial, !!completed, pingUserId)];
  if (completed && showPing) {
    buttons.push(
      new ButtonBuilder()
        .setCustomId(`ping_${rowSerial}${pingUserId ? `_${pingUserId}` : ""}`)
        .setLabel("Ping")
        .setStyle(ButtonStyle.Primary)
    );
  }
  return new ActionRowBuilder().addComponents(buttons);
}

function buildRemoveButton(rowSerial, confirm = false, userId = null) {
  const confirmId = userId ? `remove_confirm_${rowSerial}_${userId}` : `remove_confirm_${rowSerial}`;
  return new ButtonBuilder()
    .setCustomId(confirm ? confirmId : `remove_${rowSerial}`)
    .setLabel(confirm ? "Sure?" : "REMOVE")
    .setStyle(ButtonStyle.Danger);
}

function updateRemoveButtonComponents(components, rowSerial, confirm, userId = null) {
  const rows = [];
  const removeJson = buildRemoveButton(rowSerial, confirm, userId).toJSON();
  for (const row of components || []) {
    const rowJson = typeof row?.toJSON === "function" ? row.toJSON() : row;
    if (!rowJson?.components) {
      rows.push(rowJson);
      continue;
    }
    const updatedComponents = rowJson.components.map((component) => {
      const id = String(component?.custom_id || component?.customId || "");
      if (id.startsWith("remove_") || id.startsWith("remove_confirm_")) {
        return removeJson;
      }
      return component;
    });
    rows.push({ ...rowJson, components: updatedComponents });
  }
  return rows;
}

function hasRemoveConfirm(components, rowSerial, userId = null) {
  const target = userId ? `remove_confirm_${rowSerial}_${userId}` : `remove_confirm_${rowSerial}`;
  for (const row of components || []) {
    for (const component of row?.components || []) {
      if (String(component?.customId || "") === target) return true;
    }
  }
  return false;
}

// Action row builder for request message:
// - Mark Done / Not Done always
// - Remind/Cancel Remind only if reservation exists and remindMode says which label to show
function buildRequestActionRow(
  rowSerial,
  reservationStr,
  remindMode /* "arm" | "cancel" */,
  completed,
  pingUserId,
  showPing = true,
  removeConfirm = false
) {
  const buttons = [];

  buttons.push(
    buildDoneButton(rowSerial, !!completed, pingUserId)
  );

  const hasReservation =
    typeof reservationStr === "string" &&
    reservationStr.trim() !== "" &&
    reservationStr.trim() !== "—";
  const reservationUtc = hasReservation ? parseReservationUTC(reservationStr) : null;
  const reservationInFuture = reservationUtc ? reservationUtc.getTime() > Date.now() : false;

  if (hasReservation && reservationInFuture && !completed) {
    if (remindMode === "cancel") {
      buttons.push(
        new ButtonBuilder()
          .setCustomId(`remind_cancel_${rowSerial}`)
          .setLabel("🛑 Cancel Remind")
          .setStyle(ButtonStyle.Danger)
      );
    } else {
      buttons.push(
        new ButtonBuilder()
          .setCustomId(`remind_${rowSerial}`)
          .setLabel("⏰ Remind")
          .setStyle(ButtonStyle.Primary)
      );
    }
  }

  if (completed && showPing) {
    buttons.push(
      new ButtonBuilder()
        .setCustomId(`ping_${rowSerial}${pingUserId ? `_${pingUserId}` : ""}`)
        .setLabel("Ping")
        .setStyle(ButtonStyle.Primary)
    );
  }

  buttons.push(buildRemoveButton(rowSerial, removeConfirm));

  return new ActionRowBuilder().addComponents(buttons);
}

// =====================
// PANEL ENSURE
// =====================
async function ensurePanel(client, { allowCreate = true, cleanupExtra = false } = {}) {
  const payload = buildPanelPayload();
  for (const panelChannelId of TARGET_PANEL_CHANNEL_IDS) {
    const panelChannel = await client.channels.fetch(panelChannelId);
    if (!panelChannel?.isTextBased()) {
      throw new Error(`Panel channel is not a text channel: ${panelChannelId}`);
    }

    const { channelId, messageId } = getPanelMessageRef(panelChannelId);
    if (channelId && messageId) {
      try {
        const ch = await client.channels.fetch(channelId);
        if (ch?.isTextBased()) {
          const msg = await ch.messages.fetch(messageId);
          await msg.edit(payload);
          if (cleanupExtra) {
            try {
              const panelMessages = await findPanelMessages(panelChannel, client);
              for (const extra of panelMessages) {
                if (extra.id === msg.id) continue;
                await extra.delete().catch(() => {});
              }
            } catch {}
          }
          console.log(`✅ Panel message updated (${panelChannelId})`);
          continue;
        }
      } catch {
        if (!allowCreate) {
          console.log(`⚠️ Panel message missing/uneditable (${panelChannelId}); auto-create disabled.`);
          continue;
        }
        // fall through and recreate
      }
    }

    if (!allowCreate) {
      console.log(`⚠️ No stored panel message for channel (${panelChannelId}); auto-create disabled.`);
      continue;
    }

    const existingPanels = await findPanelMessages(panelChannel, client).catch(() => []);
    if (existingPanels.length > 0) {
      const msg = existingPanels[0];
      await msg.edit(payload).catch(() => {});
      setPanelMessageRef(panelChannel.id, msg.id);
      if (cleanupExtra) {
        for (const extra of existingPanels) {
          if (extra.id === msg.id) continue;
          await extra.delete().catch(() => {});
        }
      }
      console.log(`✅ Panel message relinked (${panelChannelId})`);
      continue;
    }

    const msg = await panelChannel.send(payload);
    setPanelMessageRef(panelChannel.id, msg.id);
    console.log(`✅ Panel message created and stored (${panelChannelId})`);
  }
}

async function runStartupChecks(client) {
  console.log("🔎 Startup checks:");

  for (const guildId of TARGET_GUILD_IDS) {
    try {
      await client.guilds.fetch(guildId);
      console.log(`  ✅ Guild reachable: ${guildId}`);
    } catch {
      console.log(`  ❌ Guild unreachable: ${guildId}`);
    }
  }

  const channelChecks = [
    { name: "Panel", ids: TARGET_PANEL_CHANNEL_IDS },
    { name: "Form", ids: [FORM_CHANNEL_ID] },
    { name: "Ping", ids: TARGET_PING_CHANNEL_IDS },
    { name: "Reminder", ids: [REMINDER_CHANNEL_ID] },
  ];
  for (const check of channelChecks) {
    for (const id of check.ids) {
      try {
        const ch = await client.channels.fetch(id);
        const ok = !!ch?.isTextBased();
        console.log(`  ${ok ? "✅" : "❌"} ${check.name} channel ${id}${ok ? "" : " (not text-based)"}`);
      } catch {
        console.log(`  ❌ ${check.name} channel unreachable: ${id}`);
      }
    }
  }

  if (SHEETDB_URL) {
    try {
      const probe = await fetchTimersSnapshotFromSheetDb();
      console.log(`  ${probe?.success ? "✅" : "⚠️"} SheetDB timers probe${probe?.success ? "" : " returned failure"}`);
    } catch {
      console.log("  ⚠️ SheetDB timers probe failed");
    }
  } else {
    try {
      const probe = await postToAppsScript({ action: "list_timers" }, 0, { silent: true });
      console.log(`  ${probe?.success ? "✅" : "⚠️"} Apps Script probe${probe?.success ? "" : " returned failure"}`);
    } catch {
      console.log("  ⚠️ Apps Script probe failed (transient or deployment issue)");
    }
  }
}

function scheduleHourlyRestart() {
  if (!HOURLY_RESTART_ENABLED) return;
  setTimeout(async () => {
    try {
      console.log("♻️ Hourly restart triggered. Shutting down...");
      await client.destroy();
    } catch {}
    process.exit(0);
  }, HOURLY_RESTART_MS);
}

function isOnCooldown(userId, actionKey, ms) {
  const key = `${userId}:${actionKey}`;
  const now = Date.now();
  const expiresAt = interactionCooldowns.get(key) || 0;
  if (expiresAt > now) return true;
  interactionCooldowns.set(key, now + ms);
  return false;
}

// =====================
// APPS SCRIPT POST
// =====================
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function getDiscordErrorCode(err) {
  if (!err || typeof err !== "object") return null;
  const raw = err.code ?? (err.rawError && err.rawError.code);
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function isUnknownInteractionError(err) {
  return getDiscordErrorCode(err) === 10062;
}

function isAlreadyAcknowledgedInteractionError(err) {
  return getDiscordErrorCode(err) === 40060;
}

function isRetryableNetworkError(err) {
  if (!err) return false;
  if (String(err?.name || "") === "AbortError") return true;
  const code = String(err?.code || err?.errno || "").toUpperCase();
  return (
    code === "ETIMEDOUT" ||
    code === "ECONNRESET" ||
    code === "EAI_AGAIN" ||
    code === "ECONNREFUSED" ||
    code === "UND_ERR_CONNECT_TIMEOUT"
  );
}

async function fetchWithTimeout(url, options = {}, timeoutMs = APPS_SCRIPT_TIMEOUT_MS) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

async function postJsonWithManualRedirect(url, payload, redirectsLeft = APPS_SCRIPT_MAX_REDIRECTS) {
  const res = await fetchWithTimeout(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: payload,
    redirect: "manual",
  });

  if ([301, 302, 303, 307, 308].includes(res.status) && redirectsLeft > 0) {
    const location = res.headers.get("location");
    if (location) {
      const nextUrl = new URL(location, url).toString();
      // Apps Script /exec often redirects to a one-time googleusercontent URL
      // where the final response should be fetched with GET.
      if ([301, 302, 303].includes(res.status)) {
        return fetchWithTimeout(nextUrl, {
          method: "GET",
          headers: { Accept: "application/json,text/plain,*/*" },
          redirect: "follow",
        });
      }
      return postJsonWithManualRedirect(nextUrl, payload, redirectsLeft - 1);
    }
  }
  return res;
}

async function postToAppsScript(bodyObj, attempt = 0, opts = {}) {
  const startedAt = Date.now();
  const action = (bodyObj && typeof bodyObj === "object")
    ? (bodyObj.action || (bodyObj.namedValues ? "submit_named_values" : "submit"))
    : "unknown";
  const payload = JSON.stringify(bodyObj);
  try {
    const res = await postJsonWithManualRedirect(APPS_SCRIPT_URL, payload);
    const text = await res.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch (e) {
      // Google Apps Script sometimes returns an HTML "Moved Temporarily" page with a one-time googleusercontent URL.
      const movedHrefMatch = text.match(/<A HREF="([^"]+)">here<\/A>/i);
      if (attempt < 2 && movedHrefMatch && movedHrefMatch[1]) {
        const movedUrl = movedHrefMatch[1].replace(/&amp;/g, "&");
        try {
          const movedRes = await fetchWithTimeout(movedUrl, {
            method: "GET",
            headers: { Accept: "application/json,text/plain,*/*" },
            redirect: "follow",
          });
          const movedText = await movedRes.text();
          const movedJson = JSON.parse(movedText);
          appsScriptLastOkAt = Date.now();
          appsScriptLastError = "";
          perfDuration("apps_script_call", startedAt, {
            action,
            attempt,
            status: movedRes.status,
            moved: true,
          });
          return movedJson;
        } catch {}
      }

      let errorMsg = "";
      const m = text.match(/<div[^>]*class="errorMessage"[^>]*>([^<]*)<\/div>/i);
      if (m && m[1]) {
        errorMsg = m[1].trim();
      }
      const snippet = errorMsg || text.slice(0, 1200);
      const isRetryableHtml =
        /FAILED_PRECONDITION/i.test(snippet) ||
        /Exceeded maximum execution time/i.test(snippet) ||
        /server error occurred/i.test(snippet) ||
        /Moved Temporarily/i.test(snippet) ||
        /Script function not found:\s*doGet/i.test(snippet) ||
        /Page not found/i.test(snippet) ||
        res.status >= 500;
      const willRetry = attempt < 4 && isRetryableHtml;
      if (!opts.silent && !willRetry) {
        console.error("Apps Script non-JSON response:", res.status, snippet);
      }
      appsScriptLastError = `HTTP ${res.status}: ${snippet.slice(0, 180)}`;
      if (willRetry) {
        const waitMs = 500 * Math.pow(2, attempt);
        await sleep(waitMs);
        const retried = await postToAppsScript(bodyObj, attempt + 1, opts);
        perfDuration("apps_script_call", startedAt, {
          action,
          attempt,
          status: res.status,
          retried: true,
        });
        return retried;
      }
      throw new Error("Apps Script returned non-JSON response");
    }
    if (!res.ok) {
      if (!opts.silent) {
        console.error("Apps Script HTTP error:", res.status, json);
      }
      appsScriptLastError = `HTTP ${res.status}: ${JSON.stringify(json).slice(0, 180)}`;
      if (attempt < 4 && res.status >= 500) {
        const waitMs = 500 * Math.pow(2, attempt);
        await sleep(waitMs);
        const retried = await postToAppsScript(bodyObj, attempt + 1, opts);
        perfDuration("apps_script_call", startedAt, {
          action,
          attempt,
          status: res.status,
          retried: true,
        });
        return retried;
      }
    }
    appsScriptLastOkAt = Date.now();
    appsScriptLastError = "";
    perfDuration("apps_script_call", startedAt, {
      action,
      attempt,
      status: res.status,
    });
      return json;
    } catch (err) {
    if (attempt < 4 && isRetryableNetworkError(err)) {
      const waitMs = 500 * Math.pow(2, attempt);
      await sleep(waitMs);
      const retried = await postToAppsScript(bodyObj, attempt + 1, opts);
      perfDuration("apps_script_call", startedAt, {
        action,
        attempt,
        retried: true,
        networkRetry: true,
      });
      return retried;
    }
    perfDuration(
      "apps_script_call",
      startedAt,
      {
        action,
        attempt,
        error: true,
        message: String(err?.message || err || "").slice(0, 180),
      },
      true
    );
    throw err;
  }
}

async function toggleDoneAndClearRemind(rowSerial) {
  try {
    const combined = await postToAppsScript({ action: "toggle_done_and_clear_remind", rowSerial });
    if (combined?.success) return combined;
    const msg = String(combined?.message || "").toLowerCase();
    const unsupported = msg.includes("missing action") || msg.includes("unknown action") || msg.includes("unsupported");
    if (!unsupported) return combined;
  } catch {}

  try {
    await postToAppsScript({ action: "clear_remind", rowSerial });
  } catch {}

  return postToAppsScript({ action: "toggle_done", rowSerial });
}

// =====================
// SLASH COMMANDS
// =====================
const panelResetCommand = new SlashCommandBuilder()
  .setName("panel-reset")
  .setDescription("Reset the title request panel (clears stored message id and re-posts panel).");

const timezoneCommand = new SlashCommandBuilder()
  .setName("timezone")
  .setDescription("Timezone utilities")
  .addSubcommand((s) =>
    s
      .setName("clear")
      .setDescription("Clear your saved timezone so you can pick it again.")
  );

const spreadsheetCommand = new SlashCommandBuilder()
  .setName("spreadsheet")
  .setDescription("Get the shared spreadsheet link.");

const timersCommand = new SlashCommandBuilder()
  .setName("timers")
  .setDescription("Show time left until each title is available.");

const reservationsCommand = new SlashCommandBuilder()
  .setName("reservations")
  .setDescription("Show the next reservation time for each title.");

const statusCommand = new SlashCommandBuilder()
  .setName("status")
  .setDescription("Show bot health and integration status.");

const perfCommand = new SlashCommandBuilder()
  .setName("perf")
  .setDescription("Show recent runtime performance metrics.");

const rest = new REST({ version: "10" }).setToken(DISCORD_TOKEN);
(async () => {
  const body = [
    timezoneCommand.toJSON(),
    panelResetCommand.toJSON(),
    spreadsheetCommand.toJSON(),
    timersCommand.toJSON(),
    reservationsCommand.toJSON(),
    statusCommand.toJSON(),
    perfCommand.toJSON(),
  ];
  await Promise.all(
    TARGET_GUILD_IDS.map(async (guildId) => {
      await rest.put(Routes.applicationGuildCommands(CLIENT_ID, guildId), { body });
      console.log(`✅ Slash commands registered for guild ${guildId}`);
    })
  );
  console.log("✅ Slash commands registered");
})();

// =====================
// CLIENT
// =====================
const client = new Client({ intents: [GatewayIntentBits.Guilds] });

client.once("clientReady", async () => {
  runtimeClient = client;
  startupStickyDelayUntil = Date.now() + 2 * 60_000;
  console.log(`🤖 Logged in as ${client.user.tag}`);
  scheduleHourlyRestart();
  await runStartupChecks(client);
  try {
    await ensurePanel(client, { allowCreate: false });
  } catch (e) {
    console.error("❌ ensurePanel failed:", e);
  }
  startSnapshotRefreshLoop();

  for (const [channelId, entry] of timersMessageByChannel.entries()) {
    if (!entry?.messageId) continue;
    if (entry.intervalId) clearInterval(entry.intervalId);
    const intervalId = startTimersInterval(client, channelId, entry.messageId);
    timersMessageByChannel.set(channelId, {
      messageId: entry.messageId,
      intervalId,
      lastRenderedContent: entry.lastRenderedContent ?? null,
      lastVerifiedAt: entry.lastVerifiedAt ?? 0,
    });
  }
  if (timersMessageByChannel.size > 0) {
    persistTimersStore();
  }

  for (const [channelId, entry] of reservationsMessageByChannel.entries()) {
    if (!entry?.messageId) continue;
    try {
      const ch = await getTextBasedChannel(client, channelId);
      if (ch?.isTextBased()) {
        try {
          await getChannelMessage(ch, entry.messageId);
        } catch (e) {
          if (getDiscordErrorCode(e) === 10008) {
            const existing = await findExistingReservationsMessage(ch, client);
            if (existing?.id) {
              reservationsMessageByChannel.set(channelId, {
                messageId: existing.id,
                intervalId: entry.intervalId || null,
                lastRenderedContent: entry.lastRenderedContent ?? null,
                lastVerifiedAt: entry.lastVerifiedAt ?? 0,
              });
            }
          }
        }
      }
    } catch {}
    if (entry.intervalId) clearInterval(entry.intervalId);
    const current = reservationsMessageByChannel.get(channelId) || entry;
    const intervalId = startReservationsInterval(client, channelId, current.messageId);
    reservationsMessageByChannel.set(channelId, {
      messageId: current.messageId,
      intervalId,
      lastRenderedContent: current.lastRenderedContent ?? null,
      lastVerifiedAt: current.lastVerifiedAt ?? 0,
    });
  }
  if (reservationsMessageByChannel.size > 0) {
    persistReservationsStore();
  }
});

// =====================
// INTERACTIONS
// =====================
client.on("interactionCreate", async (interaction) => {
  try {
    if (interaction.isButton()) {
      if (isDuplicateButtonInteraction(interaction.id)) {
        perfLog("button_duplicate_ignored", {
          interactionId: interaction.id || null,
          buttonId: interaction.customId || null,
          userId: interaction.user?.id || null,
        });
        return;
      }
      appendButtonLog({
        ts: new Date().toISOString(),
        interactionId: interaction.id || null,
        userId: interaction.user?.id || null,
        username: interaction.user?.tag || interaction.user?.username || null,
        buttonId: interaction.customId || null,
        messageId: interaction.message?.id || null,
        channelId: interaction.channelId || null,
        guildId: interaction.guildId || null,
      });
      const id = interaction.customId || "";
      const action =
        id.startsWith("done_") ? "done" :
        id.startsWith("remind_") ? "remind" :
        id.startsWith("ping_") ? "ping" :
        (id.startsWith("remove_") || id.startsWith("remove_confirm_")) ? "remove" :
        null;
      const isRemoveConfirmClick = id.startsWith("remove_confirm_");
      if (action && !(action === "remove" && isRemoveConfirmClick) && isOnCooldown(interaction.user.id, action, 1200)) {
        return interaction.reply({ flags: MessageFlags.Ephemeral, content: "⏳ Please wait a moment and try again." });
      }
    }
    if (interaction.isModalSubmit() && interaction.customId === "title_request_modal") {
      if (isOnCooldown(interaction.user.id, "submit_form", 2500)) {
        return interaction.reply({ flags: MessageFlags.Ephemeral, content: "⏳ Please wait a moment before submitting again." });
      }
    }

    if (interaction.isChatInputCommand() && COMMAND_CHANNEL_SET.size > 0 && !COMMAND_CHANNEL_SET.has(interaction.channelId)) {
      const target = TARGET_COMMAND_CHANNEL_IDS.map((id) => `<#${id}>`).join(", ");
      return interaction.reply({
        flags: MessageFlags.Ephemeral,
        content: `❌ Commands are only allowed in: ${target}`,
      });
    }

    // /panel-reset
    if (interaction.isChatInputCommand() && interaction.commandName === "panel-reset") {
      if (!interaction.memberPermissions?.has("Administrator")) {
        return interaction.reply({ flags: MessageFlags.Ephemeral, content: "❌ You need Administrator to use this." });
      }

      try {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
      } catch (e) {
        if (isUnknownInteractionError(e)) return;
        throw e;
      }

      try {
        await ensurePanel(client, { cleanupExtra: true });
        return interaction.editReply("✅ Panel synced (duplicates cleaned where found).");
      } catch (e) {
        console.error("panel-reset ensurePanel failed:", e);
        return interaction.editReply("❌ Panel sync failed (check logs).");
      }
    }

    // /timezone
    if (interaction.isChatInputCommand() && interaction.commandName === "timezone") {
      const sub = interaction.options.getSubcommand();
      if (sub === "clear") {
        clearUserTimezone(interaction.user.id);
        pendingTitleByUser.delete(interaction.user.id);
        return interaction.reply({
          content: "✅ Your timezone has been cleared. Please select a new timezone:",
          components: [buildTimezoneSelectRow("select_timezone_iana_clear")],
          flags: MessageFlags.Ephemeral,
        });
      }
    }

    // /spreadsheet
    if (interaction.isChatInputCommand() && interaction.commandName === "spreadsheet") {
      return interaction.reply({
        content: "https://docs.google.com/spreadsheets/d/1P8ZeMLRpwzg3wjaElc6OoUfnw6eUy6G8MT0TfCjWQZA/edit?gid=1555567410#gid=1555567410",
        flags: MessageFlags.Ephemeral,
      });
    }

    // /status
    if (interaction.isChatInputCommand() && interaction.commandName === "status") {
      const fmt = (ms) => (ms ? `<t:${Math.floor(ms / 1000)}:R>` : "never");
      const lines = [
        `Apps Script: ${appsScriptLastError ? "⚠️ degraded" : "✅ healthy"}`,
        `Apps Script last success: ${fmt(appsScriptLastOkAt)}`,
        `Timers last success: ${fmt(timersLastSuccessAt)}`,
        `Timers last failure: ${fmt(timersLastFailureAt)}`,
        `Tracked /timers channels: ${timersMessageByChannel.size}`,
        `Tracked /reservations channels: ${reservationsMessageByChannel.size}`,
        `Panel channels configured: ${TARGET_PANEL_CHANNEL_IDS.length}`,
      ];
      if (appsScriptLastError) lines.push(`Last error: ${appsScriptLastError.slice(0, 140)}`);
      return interaction.reply({ flags: MessageFlags.Ephemeral, content: lines.join("\n") });
    }

    if (interaction.isChatInputCommand() && interaction.commandName === "perf") {
      const lines = buildPerfSummaryLines(14);
      const content = lines.join("\n").slice(0, 3900);
      return interaction.reply({ flags: MessageFlags.Ephemeral, content });
    }

    // /timers
    if (interaction.isChatInputCommand() && interaction.commandName === "timers") {
      await interaction.deferReply();
      const text = await fetchTimersText({ cacheOnly: true });
      if (!text) {
        refreshTimersSnapshotInBackground().catch(() => {});
        return interaction.editReply("⏳ Timers cache is warming up. Try again in a few seconds.");
      }

      await interaction.editReply(text);
      const msg = await interaction.fetchReply();
      const channelId = interaction.channelId;

      const existing = timersMessageByChannel.get(channelId);
      if (existing?.intervalId) clearInterval(existing.intervalId);

      const intervalId = startTimersInterval(client, channelId, msg.id);
      timersMessageByChannel.set(channelId, {
        messageId: msg.id,
        intervalId,
        lastRenderedContent: text,
        lastVerifiedAt: Date.now(),
      });
      persistTimersStore();
      return;
    }

    // /reservations
    if (interaction.isChatInputCommand() && interaction.commandName === "reservations") {
      await interaction.deferReply();
      const text = await fetchReservationsText({ cacheOnly: true });
      if (!text) {
        refreshTimersSnapshotInBackground().catch(() => {});
        return interaction.editReply("⏳ Reservations cache is warming up. Try again in a few seconds.");
      }

      await interaction.editReply(text);
      const msg = await interaction.fetchReply();
      const channelId = interaction.channelId;

      const existing = reservationsMessageByChannel.get(channelId);
      if (existing?.intervalId) clearInterval(existing.intervalId);

      const intervalId = startReservationsInterval(client, channelId, msg.id);
      reservationsMessageByChannel.set(channelId, {
        messageId: msg.id,
        intervalId,
        lastRenderedContent: text,
        lastVerifiedAt: Date.now(),
      });
      persistReservationsStore();
      return;
    }

    // timezone picker select (IANA)
    if (interaction.isStringSelectMenu() && interaction.customId === "select_timezone_iana") {
      const zone = interaction.values?.[0];
      if (!zone) {
        return interaction.update({ content: "❌ No timezone selected.", components: [] });
      }
      setUserTimezoneZone(interaction.user.id, zone, interaction.user.tag);
      const pending = pendingTitleByUser.get(interaction.user.id);
      if (pending && Date.now() - pending.ts <= 10 * 60 * 1000) {
        try {
          return await interaction.showModal(
            buildRequestModal(
              interaction.user.id,
              { type: "iana", zone },
              pending.label || pending.value,
              pending.description || ""
            )
          );
        } catch (e) {
          if (isUnknownInteractionError(e)) return;
          throw e;
        }
      }
      const tzLabel = getTimezoneMenuLabel(zone);
      return interaction.update({
        content: `✅ Timezone saved: ${tzLabel}. Select your requested title:`,
        components: buildPanelComponents(),
      });
    }

    // timezone picker for /timezone clear flow (save only; never auto-open form)
    if (interaction.isStringSelectMenu() && interaction.customId === "select_timezone_iana_clear") {
      const zone = interaction.values?.[0];
      if (!zone) {
        return interaction.update({ content: "❌ No timezone selected.", components: [] });
      }
      setUserTimezoneZone(interaction.user.id, zone, interaction.user.tag);
      const tzLabel = getTimezoneMenuLabel(zone);
      return interaction.update({
        content: `✅ Timezone updated: ${tzLabel}.`,
        components: [],
      });
    }

    // reservations panel
    if (
      interaction.isButton() &&
      (interaction.customId === "check_reservations" || interaction.customId === "check_reservations_refresh")
    ) {
      const isRefresh = interaction.customId === "check_reservations_refresh";
      if (isRefresh) {
        await interaction.deferUpdate();
      } else {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
      }
      const discordId = String(interaction.user.id);

      let js;
      try {
        js = await postToAppsScript({ action: "list_user_reservations" });
      } catch (e) {
        console.error("list_user_reservations fetch error:", e);
        return interaction.editReply("❌ Could not fetch reservations right now.");
      }
      if (!js?.success) {
        return interaction.editReply(`❌ Could not fetch reservations${js?.message ? `: ${js.message}` : "."}`);
      }

      const allItems = Array.isArray(js.reservations) ? js.reservations : [];
      const items = allItems.filter((r) => {
        const serial = r?.serial ?? r?.row;
        return getReservationOwner(serial) === discordId;
      });
      const embed = new EmbedBuilder()
        .setTitle("Your Outstanding Reservations")
        .setColor(0x5865f2);

      if (!items.length) {
        embed.setDescription(js.message || "No reservations found for this user");
        return interaction.editReply({ embeds: [embed], components: [buildReservationsRefreshRow()] });
      }

      const lines = items.slice(0, 25).map((r) => {
        const title = String(r.title || "Title");
        const reservationUtc = String(r.reservationUtc || "—");
        const tz = getUserTimezone(interaction.user.id);
        return `${title} — ${formatReservationForUserTimezone(reservationUtc, tz)}`;
      });

      embed.setDescription(lines.join("\n").slice(0, 4000));
      embed.setFooter({ text: `User: ${interaction.user.tag}` });
      return interaction.editReply({ embeds: [embed], components: [buildReservationsRefreshRow()] });
    }

    // panel button
    if (interaction.isButton() && interaction.customId === "open_title_request") {
      const tz = getUserTimezone(interaction.user.id);

      if (!tz) {
        return interaction.reply({
          content: "One-time setup: select your timezone so reservation times convert to UTC correctly.",
          components: [buildTimezoneSelectRow()],
          flags: MessageFlags.Ephemeral,
        });
      }

      return interaction.reply({
        content: "Select your requested title:",
        components: buildPanelComponents(),
      });
    }

    // select title -> modal
    if (interaction.isStringSelectMenu() && interaction.customId === "select_title") {
      const picked = interaction.values?.[0];
      const t = TITLES.find((x) => x.value === picked);
      const tz = getUserTimezone(interaction.user.id);
      if (!tz) {
        pendingTitleByUser.set(interaction.user.id, {
          value: picked,
          label: t?.label ?? picked,
          description: t?.description ?? "",
          ts: Date.now(),
        });
      await interaction.reply({
          content: "One-time setup: select your timezone so reservation times convert to UTC correctly.",
          components: [buildTimezoneSelectRow()],
          flags: MessageFlags.Ephemeral,
        });
        // Reset in background so the current interaction returns instantly.
        setTimeout(async () => {
          try {
            await interaction.message.edit({ components: buildPanelComponents() });
          } catch (e) {
            if (getDiscordErrorCode(e) !== 10008) {
              console.error("Failed to reset panel title menu:", e);
            }
          }
        }, 0);
        return;
      }

      pendingTitleByUser.set(interaction.user.id, {
        value: picked,
        label: t?.label ?? picked,
        description: t?.description ?? "",
        ts: Date.now(),
      });

      try {
        await interaction.showModal(
          buildRequestModal(interaction.user.id, tz, t?.label ?? picked, t?.description ?? "")
        );
      } catch (e) {
        if (isUnknownInteractionError(e)) return;
        throw e;
      }
      // Reset in background so modal appears with minimum latency.
      setTimeout(async () => {
        try {
          await interaction.message.edit({ components: buildPanelComponents() });
        } catch (e) {
          if (getDiscordErrorCode(e) !== 10008) {
            console.error("Failed to reset title select menu:", e);
          }
        }
      }, 0);
      return;
    }

    // modal submit -> submit -> post in FORM channel
    if (interaction.isModalSubmit() && interaction.customId === "title_request_modal") {
      try {
        await interaction.deferReply({ flags: MessageFlags.Ephemeral });
      } catch (e) {
        if (isUnknownInteractionError(e)) return;
        throw e;
      }

      const pending = pendingTitleByUser.get(interaction.user.id);
      if (!pending || Date.now() - pending.ts > 10 * 60 * 1000) {
        return interaction.editReply("❌ Your title selection expired. Please click the panel button and try again.");
      }

      const tz = getUserTimezone(interaction.user.id);
      if (!tz) {
        pendingTitleByUser.delete(interaction.user.id);
        return interaction.editReply("❌ Your timezone isn’t set. Click the panel button again and select your timezone.");
      }

      const username = interaction.fields.getTextInputValue("username");
      const coords = normalizeCoords(interaction.fields.getTextInputValue("coordinates"));
      setUserPrefill(interaction.user.id, username, coords);
      const comments = interaction.fields.getTextInputValue("comments") || "";

      const rawTime = interaction.fields.getTextInputValue("reservation_time") || "";
      const rawDate = interaction.fields.getTextInputValue("reservation_date") || "";
      const requestedNow = ["now", "asap"].includes(String(rawTime).trim().toLowerCase());

      const reservation = buildReservation(rawTime, rawDate, tz);
      const titleShort = pending.value;

      const isImmediateRequest = requestedNow || reservation === "—";
      if (isImmediateRequest && isTempleWarsBlockedAtUtcMs(Date.now())) {
        pendingTitleByUser.delete(interaction.user.id);
        return interaction.editReply("❌ Reservations are blocked during Temple Wars ❌");
      }
      if (isImmediateRequest && isVaultBlockedAtUtcMsForTitle(Date.now(), titleShort)) {
        pendingTitleByUser.delete(interaction.user.id);
        return interaction.editReply("❌ General reservations are blocked during Vault ❌");
      }
      const rawTitle = pending.description ? `${pending.label}  -  ${pending.description}` : pending.label;

      if (reservation && reservation !== "—") {
        const reservationUtc = parseReservationUTC(reservation);
        if (!reservationUtc || reservationUtc.getTime() <= Date.now()) {
          pendingTitleByUser.delete(interaction.user.id);
          return interaction.editReply("❌ Reservation cannot be a past time/date ❌");
        }
        if (isTempleWarsBlockedAtUtcMs(reservationUtc.getTime())) {
          pendingTitleByUser.delete(interaction.user.id);
          return interaction.editReply("❌ Reservations are blocked during Temple Wars ❌");
        }
        if (isVaultBlockedAtUtcMsForTitle(reservationUtc.getTime(), titleShort)) {
          pendingTitleByUser.delete(interaction.user.id);
          return interaction.editReply("❌ General reservations are blocked during Vault ❌");
        }

        try {
          const dup = await postToAppsScript({
            action: "check_duplicate",
            title: titleShort,
            reservation,
          });
          if (dup?.duplicate) {
            pendingTitleByUser.delete(interaction.user.id);
            return interaction.editReply("❌ This time is reserved for this title, try reserving for a later time ❌");
          }
        } catch (e) {
          console.error("check_duplicate fetch error:", e);
          return interaction.editReply("❌ Could not verify reservation time. Please try again ❌");
        }
      }

      // POST NEW SUBMISSION
      let jsonResponse;
      try {
        jsonResponse = await postToAppsScript({
          namedValues: {
            Username: username,
            Coordinates: coords,
            "Title Request": rawTitle,
            "Reservation Date and Time (UTC)": reservation,
            Comments: comments,
          },
        });
      } catch (e) {
        console.error("Apps Script fetch error:", e);
        pendingTitleByUser.delete(interaction.user.id);
        return interaction.editReply("❌ Failed to submit to Google Sheets (network error).");
      }

      if (!jsonResponse?.success) {
        console.error("Apps Script failed:", jsonResponse);
        pendingTitleByUser.delete(interaction.user.id);
        return interaction.editReply("❌ Failed to submit to Google Sheets.");
      }

      const rowSerial = String(jsonResponse.serial);
      setReservationOwner(rowSerial, interaction.user.id);
      const isTestUsername = String(username).trim() === "#TEST";
      const displayDiscordUsername = `${interaction.user.username}${isTestUsername ? ` ${TEST_DISCORD_SUFFIX_EMOJI}` : ""}`;

      const embed = new EmbedBuilder()
        .setTitle("📋 New Title Request")
        .addFields(
          { name: "👤 Discord", value: displayDiscordUsername, inline: true },
          { name: "🎮 Username", value: username, inline: true },
          { name: "📍 Coordinates", value: coords, inline: true },
          { name: "🏷️ Title", value: titleShort, inline: true },
          { name: "🕒 Reservation (UTC)", value: reservation, inline: true }
        )
        .setColor(0x00ff00);

      if (comments.trim()) {
        embed.addFields({ name: "📝 Comments", value: comments.slice(0, 1024), inline: false });
      }

      // Default: Remind is available (arm mode) only if reservation exists
      const actionRow = buildRequestActionRow(rowSerial, reservation, "arm", false, interaction.user.id, true);

      const formChannel = await client.channels.fetch(FORM_CHANNEL_ID);
      const guardianMention = GUARDIAN_ID ? `<@&${GUARDIAN_ID}>` : "";
      const contentParts = [];
      if (guardianMention && !isTestUsername) contentParts.push(guardianMention);
      const content = contentParts.length ? contentParts.join(" ") : undefined;

      const sentFormMessage = await formChannel.send({ content, embeds: [embed], components: [actionRow] });
      setReservationRequestMessage(rowSerial, sentFormMessage.channelId, sentFormMessage.id, interaction.guildId);
      auditLog("form_submit", {
        userId: interaction.user.id,
        rowSerial,
        title: titleShort,
        reservation,
        guildId: interaction.guildId,
        channelId: interaction.channelId,
      });

      pendingTitleByUser.delete(interaction.user.id);
      return interaction.editReply("✅ Title request submitted successfully!");
    }

    // 🗑️ Remove (double-confirm)
    if (
      interaction.isButton() &&
      (interaction.customId.startsWith("remove_") || interaction.customId.startsWith("remove_confirm_"))
    ) {
      const isConfirm = interaction.customId.startsWith("remove_confirm_");
      const parts = interaction.customId.split("_");
      const rowSerial = isConfirm ? parts[2] : parts[1];
      const confirmUserId = isConfirm && parts.length >= 4 ? parts[3] : null;
      if (isConfirm && confirmUserId && confirmUserId !== String(interaction.user.id)) {
        return interaction.reply({
          flags: MessageFlags.Ephemeral,
          content: "❌ Only the user who clicked REMOVE can confirm this.",
        });
      }

      if (!isConfirm) {
        try {
          await interaction.deferUpdate();
        } catch (e) {
          if (isUnknownInteractionError(e)) return;
          if (!isAlreadyAcknowledgedInteractionError(e)) throw e;
        }
        const updatedComponents = updateRemoveButtonComponents(
          interaction.message.components,
          rowSerial,
          true,
          interaction.user.id
        );
        if (updatedComponents.length) {
          try {
            await interaction.message.edit({ components: updatedComponents });
          } catch (e) {
            if (getDiscordErrorCode(e) !== 10008) throw e;
            return;
          }
          const messageId = interaction.message?.id;
          const channelId = interaction.channelId;
          const confirmIdUser = String(interaction.user.id);
          setTimeout(async () => {
            try {
              const ch = await client.channels.fetch(channelId);
              if (!ch?.isTextBased()) return;
              const msg = await ch.messages.fetch(messageId);
              if (!msg) return;
              if (!hasRemoveConfirm(msg.components, rowSerial, confirmIdUser)) return;
              const reverted = updateRemoveButtonComponents(msg.components, rowSerial, false);
              if (reverted.length) {
                await msg.edit({ components: reverted });
              }
            } catch {}
          }, 10_000);
        }
        return;
      }

      await interaction.deferReply({ flags: MessageFlags.Ephemeral });

      let js;
      try {
        js = await postToAppsScript({ action: "remove_row", rowSerial });
      } catch (e) {
        console.error("remove_row fetch error:", e);
        return interaction.editReply("❌ Could not remove reservation right now.");
      }
      if (!js?.success) {
        return interaction.editReply(`❌ Could not remove reservation${js?.message ? `: ${js.message}` : "."}`);
      }

      // Cancel any scheduled reminder and remove reminder message if present
      const meta = reminderMeta.get(String(rowSerial));
      cancelReminder(rowSerial);
      doneStateOverrides.delete(String(rowSerial));
      if (reservationOwners.has(String(rowSerial))) {
        reservationOwners.delete(String(rowSerial));
        persistReservationOwners();
      }
      clearReservationMessage(rowSerial);
      if (meta?.reminderMessageId && meta?.reminderChannelId) {
        try {
          const rch = await client.channels.fetch(meta.reminderChannelId);
          if (rch?.isTextBased()) {
            const rmsg = await rch.messages.fetch(meta.reminderMessageId);
            await rmsg.delete().catch(() => {});
          }
        } catch {}
      }

      await interaction.message.delete().catch(() => {});

      // Update timer panels immediately since availability/reservation graph changed.
      updateAllTimersMessages(client);
      updateAllReservationsMessages(client);
      auditLog("remove_row", {
        userId: interaction.user.id,
        rowSerial,
        guildId: interaction.guildId,
        channelId: interaction.channelId,
      });
      return interaction.editReply("✅ Reservation removed.");
    }

    // 🛑 Cancel Remind
    if (interaction.isButton() && interaction.customId.startsWith("remind_cancel_")) {
      const rowSerial = interaction.customId.split("_")[2];

      // Use deferUpdate so we can also edit message buttons
      await interaction.deferUpdate();

      const metaBeforeCancel = reminderMeta.get(String(rowSerial)) || {};
      cancelReminder(rowSerial);

      // Clear Remind At in sheet
      let clearJson = null;
      try {
        clearJson = await postToAppsScript({ action: "clear_remind", rowSerial });
      } catch (e) {
        console.error("clear_remind -> Apps Script error:", e);
      }
      if (clearJson && !clearJson.success) {
        console.error("clear_remind failed:", clearJson);
      }

      const completed = getCompletedFromEmbed(interaction.message);
      const reservationStr =
        getReservationFromEmbed(interaction.message) ||
        metaBeforeCancel.reservationStr ||
        "";
      if (isReminderMessage(interaction.message)) {
        try {
          await updateOriginalRequestFromReminder(client, interaction.message, rowSerial);
        } catch (e) {
          console.error("updateOriginalRequestFromReminder error:", e);
        }
        const updatedRow = new ActionRowBuilder().addComponents(buildDoneButton(rowSerial, completed));
        await interaction.message.edit({ components: [updatedRow] });
      } else {
        const pingUserId = resolvePingUserId(interaction.message, rowSerial);
        const updatedRow = buildRequestActionRow(rowSerial, reservationStr, "arm", completed, pingUserId, true);
        await interaction.message.edit({ components: [updatedRow] });
      }

      const cancelMessage = clearJson && clearJson.success
        ? "✅ Reminder cancelled."
        : "⚠️ Reminder cancelled, but I couldn't clear the sheet. Check Apps Script logs.";
      auditLog("remind_cancel", {
        userId: interaction.user.id,
        rowSerial,
        guildId: interaction.guildId,
        channelId: interaction.channelId,
        success: !!(clearJson && clearJson.success),
      });
      return interaction.followUp({
        flags: MessageFlags.Ephemeral,
        content: cancelMessage,
      });
    }

    // 🟣 Ping
    if (interaction.isButton() && interaction.customId.startsWith("ping_")) {
      const parts = interaction.customId.split("_");
      const rowSerial = parts.length >= 2 ? parts[1] : null;
      const pingUserId = resolvePingUserId(interaction.message, rowSerial, parts.length >= 3 ? parts[2] : null);
      if (!pingUserId) {
        return interaction.reply({ flags: MessageFlags.Ephemeral, content: "❌ I couldn't find the submitter to ping." });
      }
      const mention = `<@${pingUserId}>`;

      const completed = getCompletedFromEmbed(interaction.message);

      // Remove Ping button immediately to prevent spam
      try {
        const isReminder = isReminderMessage(interaction.message);
        const updatedRow = isReminder
          ? buildReminderActionRow(rowSerial, completed, pingUserId, false)
          : (() => {
              const reservationStr = getReservationFromEmbed(interaction.message);
              return buildRequestActionRow(rowSerial, reservationStr, "arm", completed, pingUserId, false);
            })();
        await interaction.update({ components: [updatedRow] });
      } catch (e) {
        console.error("ping button update error:", e);
        try {
          await interaction.deferUpdate();
        } catch {}
      }

      // Remove ping button on the linked message too
      try {
        if (isReminderMessage(interaction.message)) {
          await updateOriginalRequestFromReminder(client, interaction.message, rowSerial, completed, false);
        } else {
          await updateReminderMessagePingVisibility(client, rowSerial, false, completed);
        }
      } catch (e) {
        console.error("Failed to sync ping button visibility:", e);
      }

      const title = getTitleFromEmbed(interaction.message) || "Title";
      const username = getUsernameFromEmbed(interaction.message) || "Username";
      try {
        const originGuildId = rowSerial ? getReservationOriginGuild(rowSerial) : null;
        const targetPingChannelId = resolvePingChannelId(originGuildId, interaction.guildId);
        const pingChannel = await client.channels.fetch(targetPingChannelId);
        if (!pingChannel?.isTextBased()) {
          return interaction.followUp({ flags: MessageFlags.Ephemeral, content: "❌ The configured ping channel is not a text channel." });
        }

        await pingChannel.send(`${mention} ${title} is on ${username}! Please refresh your game.`);
        auditLog("ping_sent", {
          userId: interaction.user.id,
          rowSerial,
          targetUserId: pingUserId,
          title,
          username,
          guildId: interaction.guildId,
          channelId: interaction.channelId,
          originGuildId,
          targetPingChannelId,
        });
      } catch (e) {
        console.error("ping send error:", e);
        return interaction.followUp({ flags: MessageFlags.Ephemeral, content: "❌ Failed to send ping." });
      }

      return interaction.followUp({ flags: MessageFlags.Ephemeral, content: "✅ Ping sent." });
    }

    // ⏰ Remind (arm)
    if (interaction.isButton() && interaction.customId.startsWith("remind_")) {
      const rowSerial = interaction.customId.split("_")[1];
      await interaction.deferReply({ flags: MessageFlags.Ephemeral });

      const reservationStr = getReservationFromEmbed(interaction.message);
      if (!reservationStr || String(reservationStr).trim() === "" || reservationStr === "—") {
        return interaction.editReply("❌ This request has no reservation time to remind for.");
      }

      const reservationUtc = parseReservationUTC(reservationStr);
      if (!reservationUtc) {
        return interaction.editReply(`❌ I couldn't parse the reservation time: \`${reservationStr}\``);
      }

      // Store meta from the request embed
      const title = getTitleFromEmbed(interaction.message) || "Title";
      const username = getUsernameFromEmbed(interaction.message) || "Username";
      const coordinates = getCoordinatesFromEmbed(interaction.message) || "—";
      const ownerUserId = resolvePingUserId(interaction.message, rowSerial);
      const discordMention = ownerUserId ? `<@${ownerUserId}>` : null;

      // Optimistic UI: flip to "Cancel Remind" immediately
      const completed = getCompletedFromEmbed(interaction.message);
      const pingUserId = resolvePingUserId(interaction.message, rowSerial);
      const optimisticRow = buildRequestActionRow(rowSerial, reservationStr, "cancel", completed, pingUserId, true);
      const previousComponents = interaction.message.components;
      try {
        await interaction.message.edit({ components: [optimisticRow] });
      } catch (e) {
        console.error("Failed to optimistically update remind button:", e);
      }

      // Write remind time into sheet (Remind At = reservation)
      try {
        const js = await postToAppsScript({ action: "remind", rowSerial, remindAt: reservationStr });
        if (!js?.success) {
          if (previousComponents?.length) {
            await interaction.message.edit({ components: previousComponents });
          }
          return interaction.editReply("❌ Could not set reminder in the sheet (Apps Script returned failure).");
        }
      } catch (e) {
        console.error("remind -> Apps Script error:", e);
        if (previousComponents?.length) {
          await interaction.message.edit({ components: previousComponents });
        }
        return interaction.editReply("❌ Could not set reminder in the sheet (network error).");
      }

      // Schedule reminder timer in bot
      scheduleReminder({
        client,
        rowSerial,
        reservationUtc,
        channelId: interaction.channelId,
        sourceMessageUrl: interaction.message.url,
        title,
        username,
        coordinates,
        discordMention,
        reservationStr,
      });
      auditLog("remind_arm", {
        userId: interaction.user.id,
        rowSerial,
        reservationStr,
        guildId: interaction.guildId,
        channelId: interaction.channelId,
      });

      return interaction.editReply(`✅ Reminder armed for **${reservationStr}** (UTC).`);
    }

    // ✅ Done (toggle) — also cancels reminder + clears Remind At
    if (interaction.isButton() && interaction.customId.startsWith("done_")) {
      const parts = interaction.customId.split("_");
      const rowSerial = parts[1];
      const pingUserIdFromCustom = parts.length >= 3 ? parts[2] : null;
      const doneKey = String(rowSerial);
      if (doneToggleInFlight.has(doneKey)) {
        return interaction.reply({
          flags: MessageFlags.Ephemeral,
          content: "⏳ This request is already updating. Please try again in a moment.",
        }).catch(() => {});
      }
      doneToggleInFlight.add(doneKey);

      try {
        const previousEmbeds = interaction.message.embeds;
        const previousComponents = interaction.message.components;
        const wasCompleted = getCompletedFromEmbed(interaction.message);
        const optimisticCompleted = !wasCompleted;
        const isReminder = isReminderMessage(interaction.message);

        // Optimistic UI update
        const optimisticBase = interaction.message.embeds?.[0]
          ? EmbedBuilder.from(interaction.message.embeds[0])
          : new EmbedBuilder().setTitle("📋 Title Request");
        if (optimisticCompleted) {
          optimisticBase.setColor(0x777777).setFooter({ text: "Completed" });
        } else {
          optimisticBase.setColor(0x00ff00).setFooter(null);
        }
        const optimisticRow = isReminder
          ? buildReminderActionRow(rowSerial, optimisticCompleted, resolvePingUserId(interaction.message, rowSerial, pingUserIdFromCustom), true)
          : (() => {
              const optimisticReservation = getReservationFromEmbed(interaction.message);
              const pingUserId = resolvePingUserId(interaction.message, rowSerial, pingUserIdFromCustom);
              return buildRequestActionRow(rowSerial, optimisticReservation, "arm", optimisticCompleted, pingUserId, true);
            })();
        const ackStartedAt = Date.now();
        let interactionAcked = false;
        let optimisticApplied = false;
        try {
          await interaction.update({ embeds: [optimisticBase], components: [optimisticRow] });
          interactionAcked = true;
          optimisticApplied = true;
          perfDuration("done_interaction_ack", ackStartedAt, { rowSerial, ackType: "update" });
        } catch (e) {
          if (isUnknownInteractionError(e)) return;
          console.error("Failed to optimistically update done button:", e);
          try {
            await interaction.deferUpdate();
            interactionAcked = true;
            perfDuration("done_interaction_ack", ackStartedAt, { rowSerial, ackType: "deferUpdate" });
          } catch (deferErr) {
            if (isUnknownInteractionError(deferErr)) return;
          }
        }
        if (!interactionAcked) return;

        syncLinkedDoneState(client, interaction.message, rowSerial, optimisticCompleted, true)
          .catch((e) => console.error("Failed to optimistically sync linked message:", e));

        const reminderKey = String(rowSerial);
        const hadReminder = reminderTimers.has(reminderKey) || reminderMeta.has(reminderKey);
        const reminderMetaEntry = reminderMeta.get(reminderKey);
        const reminderFired = !!reminderMetaEntry?.fired;

        // Always cancel timer locally, then persist done+reminder state in sheet.
        cancelReminder(rowSerial, true);

        let json;
        try {
          json = await toggleDoneAndClearRemind(rowSerial);
        } catch (e) {
          console.error("toggle_done_and_clear_remind fetch error:", e);
          if (previousEmbeds?.length && previousComponents?.length) {
            await interaction.message.edit({ embeds: previousEmbeds, components: previousComponents });
          }
          await syncLinkedDoneState(client, interaction.message, rowSerial, wasCompleted, true)
            .catch((syncErr) => console.error("Failed to rollback linked message after done error:", syncErr));
          return;
        }

        if (!json?.success) {
          console.error("toggle_done_and_clear_remind failed:", json);
          if (previousEmbeds?.length && previousComponents?.length) {
            await interaction.message.edit({ embeds: previousEmbeds, components: previousComponents });
          }
          await syncLinkedDoneState(client, interaction.message, rowSerial, wasCompleted, true)
            .catch((syncErr) => console.error("Failed to rollback linked message after done failure:", syncErr));
          return;
        }

        const completed = !!json.done;
        setDoneStateOverride(rowSerial, completed);

        if (!optimisticApplied || completed !== optimisticCompleted) {
          const base = interaction.message.embeds?.[0]
            ? EmbedBuilder.from(interaction.message.embeds[0])
            : new EmbedBuilder().setTitle("📋 Title Request");

          if (completed) {
            base.setColor(0x777777).setFooter({ text: "Completed" });
          } else {
            base.setColor(0x00ff00).setFooter(null);
          }

          const row = isReminder
            ? buildReminderActionRow(rowSerial, completed, resolvePingUserId(interaction.message, rowSerial, pingUserIdFromCustom), true)
            : (() => {
                const reservationStr = getReservationFromEmbed(interaction.message);
                const pingUserId = resolvePingUserId(interaction.message, rowSerial, pingUserIdFromCustom);
                return buildRequestActionRow(rowSerial, reservationStr, "arm", completed, pingUserId, true);
              })();

          await interaction.message.edit({ embeds: [base], components: [row] });
        }

        // Update embed visuals
        if (completed && hadReminder && !reminderFired && !isReminder) {
          await interaction.followUp({
            flags: MessageFlags.Ephemeral,
            content: "✅ Reminder cancelled because this request was marked done.",
          });
        }

        try {
          await syncLinkedDoneState(client, interaction.message, rowSerial, completed, true);
        } catch (e) {
          console.error("Failed to sync linked message from done toggle:", e);
        }

        // Immediate timers refresh for all active timers messages
        updateAllTimersMessages(client);
        updateAllReservationsMessages(client);
        auditLog("done_toggle", {
          userId: interaction.user.id,
          rowSerial,
          completed,
          guildId: interaction.guildId,
          channelId: interaction.channelId,
        });
      } finally {
        doneToggleInFlight.delete(doneKey);
      }
    }
  } catch (err) {
    console.error("Unhandled interaction error:", err);
    try {
      if (interaction.isRepliable() && !interaction.replied && !interaction.deferred) {
        await interaction.reply({ content: "❌ Something went wrong.", flags: MessageFlags.Ephemeral });
      }
    } catch {}
  }
});

client.on("error", (err) => console.error("Discord client error:", err));
process.on("unhandledRejection", (reason) => console.error("Unhandled promise rejection:", reason));

client.login(DISCORD_TOKEN);

