// Home Base runtime: sign-in, shared data (Supabase), receipts, voice notes and push notifications.
// It gives the app the same small API it used inside Claude, so the app code barely changed.
import { createClient } from "https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2.45.4/+esm";
import { SUPABASE_URL, SUPABASE_ANON_KEY, VAPID_PUBLIC_KEY } from "./config.js";

const sb = createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
  auth: { persistSession: true, autoRefreshToken: true, storageKey: "hb-auth" },
});

if ("serviceWorker" in navigator) navigator.serviceWorker.register("./sw.js").catch(() => {});

/* ---------------- shared data: a tiny document store on one table ---------------- */
const rid = () => crypto.randomUUID().replace(/-/g, "").slice(0, 20);
const caches = {}; // col -> Map(id -> data)
const loading = {}; // col -> Promise
const listeners = {}; // col -> Set
const channels = {}; // col -> realtime channel

const toErr = (e) => {
  const msg = (e && e.message) || String(e);
  const denied = e && (e.code === "42501" || e.code === "P0002" || /row-level security|permission|not found/i.test(msg));
  return { code: denied ? "invalid_argument" : "unavailable", message: msg };
};
const clone = (v) => (v == null ? v : JSON.parse(JSON.stringify(v)));
const docSnap = (id, data) => ({ id, exists: data != null, data: () => (data == null ? undefined : clone(data)), metadata: { fromCache: false, hasPendingWrites: false } });
const cmp = (a, b) => (a == null ? 1 : b == null ? -1 : a < b ? -1 : a > b ? 1 : 0);
const ops = { "==": (a, b) => a === b, "!=": (a, b) => a !== b, ">=": (a, b) => a >= b, "<=": (a, b) => a <= b, ">": (a, b) => a > b, "<": (a, b) => a < b };

async function load(col, force) {
  if (!force && loading[col]) return loading[col];
  loading[col] = (async () => {
    const map = new Map();
    let from = 0;
    for (;;) {
      const { data, error } = await sb.from("docs").select("id, data").eq("col", col).range(from, from + 999);
      if (error) throw toErr(error);
      data.forEach((r) => map.set(r.id, r.data));
      if (data.length < 1000) break;
      from += 1000;
    }
    caches[col] = map;
  })();
  return loading[col];
}
function fire(col) {
  (listeners[col] || new Set()).forEach((l) => {
    try { l.deliver(); } catch (e) { /* keep others running */ }
  });
}
function watch(col) {
  if (channels[col]) return;
  channels[col] = sb
    .channel("docs-" + col)
    .on("postgres_changes", { event: "*", schema: "public", table: "docs", filter: "col=eq." + col }, (p) => {
      const map = caches[col] || (caches[col] = new Map());
      if (p.eventType === "DELETE") map.delete(p.old.id);
      else map.set(p.new.id, p.new.data);
      fire(col);
    })
    .subscribe();
}
document.addEventListener("visibilitychange", () => {
  if (document.visibilityState !== "visible") return;
  Object.keys(listeners).forEach((col) => { if (listeners[col].size) load(col, true).then(() => fire(col)).catch(() => {}); });
});

class Query {
  constructor(col, filters = [], order = null, lim = null) { this.col = col; this.path = col; this.filters = filters; this.order = order; this.lim = lim; }
  where(f, op, v) { return new Query(this.col, [...this.filters, [f, op, v]], this.order, this.lim); }
  orderBy(f, dir = "asc") { return new Query(this.col, this.filters, [f, dir], this.lim); }
  limit(n) { return new Query(this.col, this.filters, this.order, n); }
  _snap() {
    let rows = [...(caches[this.col] || new Map()).entries()].map(([id, data]) => ({ id, data }));
    for (const [f, op, v] of this.filters) rows = rows.filter((r) => ops[op] && ops[op](r.data[f], v));
    if (this.order) { const [f, dir] = this.order; rows.sort((a, b) => cmp(a.data[f], b.data[f]) * (dir === "desc" ? -1 : 1)); }
    else rows.sort((a, b) => cmp(a.id, b.id));
    if (this.lim) rows = rows.slice(0, this.lim);
    const docs = rows.map((r) => docSnap(r.id, r.data));
    return { docs, size: docs.length, empty: !docs.length, docChanges: () => [], metadata: { fromCache: false, hasPendingWrites: false } };
  }
  async get() { await load(this.col); return this._snap(); }
  onSnapshot(next, error) {
    const l = { deliver: () => next(this._snap()) };
    (listeners[this.col] || (listeners[this.col] = new Set())).add(l);
    watch(this.col);
    load(this.col).then(() => l.deliver()).catch((e) => error && error(toErr(e)));
    return () => listeners[this.col].delete(l);
  }
}
class Collection extends Query {
  doc(id) { return new DocRef(this.col, id || rid()); }
  async add(data) { const r = this.doc(); await r.set(data); return r; }
}
class DocRef {
  constructor(col, id) { this.col = col; this.id = id; this.path = col + "/" + id; }
  async get() {
    const { data, error } = await sb.from("docs").select("data").eq("col", this.col).eq("id", this.id).maybeSingle();
    if (error) throw toErr(error);
    return docSnap(this.id, data ? data.data : null);
  }
  async set(d) {
    const { error } = await sb.from("docs").upsert({ col: this.col, id: this.id, data: d, updated_at: new Date().toISOString() });
    if (error) throw toErr(error);
    (caches[this.col] || (caches[this.col] = new Map())).set(this.id, clone(d)); fire(this.col);
  }
  async update(p) {
    const { error } = await sb.rpc("docs_merge", { p_col: this.col, p_id: this.id, p_patch: p });
    if (error) throw toErr(error);
    const m = caches[this.col]; if (m && m.has(this.id)) { m.set(this.id, Object.assign({}, m.get(this.id), clone(p))); fire(this.col); }
  }
  async delete() {
    const { error } = await sb.from("docs").delete().eq("col", this.col).eq("id", this.id);
    if (error) throw toErr(error);
    caches[this.col] && caches[this.col].delete(this.id); fire(this.col);
  }
  onSnapshot(next, error) {
    const l = { deliver: () => next(docSnap(this.id, (caches[this.col] || new Map()).get(this.id))) };
    (listeners[this.col] || (listeners[this.col] = new Set())).add(l);
    watch(this.col);
    load(this.col).then(() => l.deliver()).catch((e) => error && error(toErr(e)));
    return () => listeners[this.col].delete(l);
  }
}
const db = {
  collection: (path) => new Collection(path),
  doc: (path) => { const i = path.lastIndexOf("/"); return new DocRef(path.slice(0, i), path.slice(i + 1)); },
};

/* ---------------- receipts ---------------- */
const signed = new Map();
window.hbReceiptUrl = (id) => {
  if (!id) return "";
  const hit = signed.get(id);
  if (hit && hit !== "pending") return hit;
  if (!hit) {
    signed.set(id, "pending");
    sb.storage.from("receipts").createSignedUrl(id, 60 * 60 * 6).then(({ data }) => {
      if (data && data.signedUrl) { signed.set(id, data.signedUrl); window.dispatchEvent(new Event("hb-refresh")); }
      else signed.delete(id);
    });
  }
  return "data:image/gif;base64,R0lGODlhAQABAAAAACw=";
};
window.hbReceiptUrlAsync = async (id) => {
  const { data } = await sb.storage.from("receipts").createSignedUrl(id, 600);
  return data ? data.signedUrl : "";
};
const assets = {
  async upload(blob, opts) {
    const name = rid() + ".jpg";
    const { error } = await sb.storage.from("receipts").upload(name, blob, { contentType: (opts && opts.type) || blob.type || "image/jpeg" });
    if (error) throw { code: /size|large/i.test(error.message) ? "too_large" : "upstream_error", message: error.message };
    return { id: name, url: "", sizeBytes: blob.size, contentType: "image/jpeg" };
  },
};

/* ---------------- downloads: share sheet on iPhone (email it to the accountant) ---------------- */
const MIME = { pdf: "application/pdf", csv: "text/csv" };
const downloads = {
  async save({ filename, data }) {
    const ext = filename.split(".").pop();
    const blob = data instanceof Blob ? data : new Blob([data], { type: MIME[ext] || "application/octet-stream" });
    const file = new File([blob], filename, { type: MIME[ext] || blob.type });
    if (navigator.canShare && navigator.canShare({ files: [file] })) {
      try { await navigator.share({ files: [file], title: filename }); return { status: "saved" }; }
      catch (e) { if (e && e.name === "AbortError") throw { code: "declined" }; }
    }
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob); a.download = filename; document.body.appendChild(a); a.click();
    setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 4000);
    return { status: "saved" };
  },
};

/* ---------------- Claude / voice ---------------- */
const sample = Object.assign(async () => { throw { code: "not_granted" }; }, {
  async json(prompt) {
    const { data, error } = await sb.functions.invoke("sort-note", { body: { prompt } });
    if (error || !data || data.error) throw { code: "unavailable", message: (error && error.message) || (data && data.error) };
    return data;
  },
});

let rec = null, recStream = null, recChunks = [], recTimer = null;
window.hbRecorder = {
  supported: !!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia && window.MediaRecorder),
  async start() {
    recStream = await navigator.mediaDevices.getUserMedia({ audio: true });
    const type = MediaRecorder.isTypeSupported("audio/mp4") ? "audio/mp4" : MediaRecorder.isTypeSupported("audio/webm") ? "audio/webm" : "";
    rec = new MediaRecorder(recStream, type ? { mimeType: type } : undefined);
    recChunks = [];
    rec.ondataavailable = (e) => e.data && e.data.size && recChunks.push(e.data);
    rec.start();
    recTimer = setTimeout(() => window.dispatchEvent(new Event("hb-rec-timeout")), 120000);
  },
  stop() {
    clearTimeout(recTimer);
    return new Promise((res) => {
      if (!rec) return res(null);
      rec.onstop = () => {
        recStream && recStream.getTracks().forEach((t) => t.stop());
        const blob = new Blob(recChunks, { type: rec.mimeType || "audio/mp4" });
        rec = null; res(blob);
      };
      rec.stop();
    });
  },
};
window.hbTranscribe = async (blob) => {
  const fd = new FormData();
  fd.append("audio", new File([blob], blob.type.includes("webm") ? "note.webm" : "note.m4a", { type: blob.type }));
  const { data, error } = await sb.functions.invoke("transcribe", { body: fd });
  if (error || !data || data.error) throw new Error((data && data.error) || "transcribe_failed");
  return data.text || "";
};

/* ---------------- push notifications ---------------- */
const b64ToBytes = (s) => { const p = "=".repeat((4 - (s.length % 4)) % 4); const r = atob((s + p).replace(/-/g, "+").replace(/_/g, "/")); return Uint8Array.from(r, (c) => c.charCodeAt(0)); };
const isStandalone = () => window.navigator.standalone === true || window.matchMedia("(display-mode: standalone)").matches;
window.hbPush = {
  state() {
    if (!("serviceWorker" in navigator) || !("PushManager" in window) || !("Notification" in window)) return isStandalone() ? "unsupported" : "install";
    return Notification.permission; // "default" | "granted" | "denied"
  },
  async enable(who) {
    const perm = await Notification.requestPermission();
    if (perm !== "granted") return perm;
    const reg = await navigator.serviceWorker.ready;
    const sub = (await reg.pushManager.getSubscription()) || (await reg.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: b64ToBytes(VAPID_PUBLIC_KEY) }));
    const json = sub.toJSON();
    const id = Array.from(new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(json.endpoint)))).slice(0, 12).map((b) => b.toString(16).padStart(2, "0")).join("");
    await db.doc("push/" + id).set({ who, sub: json, device: navigator.userAgent.slice(0, 120), at: new Date().toISOString() });
    return "granted";
  },
};
window.hbNotify = (payload) => sb.functions.invoke("notify", { body: payload }).catch(() => {});
window.hbResetPartner = async () => {
  const { data, error } = await sb.functions.invoke("notify", { body: { action: "reset-partner" } });
  if (error || !data || data.error) throw new Error((data && data.error) || "reset_failed");
  return data.temp;
};
window.hbChangePassword = async (pw) => {
  const { error } = await sb.auth.updateUser({ password: pw, data: { temp_pw: false } });
  if (error) throw error;
};
window.hbSignOut = async () => { await sb.auth.signOut(); localStorage.removeItem("hb-me"); location.reload(); };

/* ---------------- sign-in ---------------- */
const $ = (id) => document.getElementById(id);
async function whoAmI(email) {
  const { data } = await sb.from("members").select("who").eq("email", (email || "").toLowerCase()).maybeSingle();
  return data ? data.who : null;
}
function showLogin(msg) {
  const el = $("login"); el.hidden = false;
  if (msg) { $("lg-err").textContent = msg; $("lg-err").hidden = false; }
}
async function finish(session) {
  const who = await whoAmI(session.user.email);
  if (!who) { await sb.auth.signOut(); showLogin("That email isn't on Home Base yet. Use Nakita's or Colin's email."); return; }
  if (session.user.user_metadata && session.user.user_metadata.temp_pw) {
    // Signed in with a temporary password: choose a new one before going in.
    $("login").hidden = false; $("lg-form").hidden = true; $("pw-form").hidden = false;
    $("pw-form").onsubmit = async (e) => {
      e.preventDefault();
      const pw = $("pw-new").value;
      if (pw.length < 8) { $("pw-err").textContent = "Use at least 8 characters."; $("pw-err").hidden = false; return; }
      try { await window.hbChangePassword(pw); } catch (err) { $("pw-err").textContent = "That didn't save. Try again."; $("pw-err").hidden = false; return; }
      session.user.user_metadata.temp_pw = false; $("pw-form").hidden = true; $("lg-form").hidden = false; finish(session);
    };
    return;
  }
  $("login").hidden = true;
  const user = {
    isOwner: async () => who === "nakita",
    canEdit: async () => true,
    can: async () => true,
    id: async () => session.user.id,
    name: async () => (who === "nakita" ? "Nakita" : "Colin"),
    me: async () => ({ id: session.user.id, name: who === "nakita" ? "Nakita" : "Colin" }),
  };
  window.__hbResolve({ db, user, sample, assets, downloads, mcp: null });
}
$("lg-form").addEventListener("submit", async (e) => {
  e.preventDefault();
  const email = $("lg-email").value.trim(), password = $("lg-pass").value, mode = $("lg-form").dataset.mode;
  const btn = $("lg-go"); btn.disabled = true; btn.textContent = mode === "signup" ? "Creating…" : "Signing in…"; $("lg-err").hidden = true;
  const res = mode === "signup" ? await sb.auth.signUp({ email, password }) : await sb.auth.signInWithPassword({ email, password });
  btn.disabled = false; btn.textContent = mode === "signup" ? "Create my password" : "Sign in";
  if (res.error) { showLogin(/confirm/i.test(res.error.message) ? "Check your email and tap the confirm link first, then sign in here." : /invalid/i.test(res.error.message) ? "That email or password doesn't match. Try again." : res.error.message); return; }
  if (!res.data.session) { showLogin("Check your email and tap the confirm link, then come back and sign in."); $("lg-form").dataset.mode = "signin"; syncMode(); return; }
  finish(res.data.session);
});
function syncMode() {
  const s = $("lg-form").dataset.mode === "signup";
  $("lg-go").textContent = s ? "Create my password" : "Sign in";
  $("lg-switch").textContent = s ? "Already set up? Sign in" : "First time? Create your password";
  $("lg-pass").autocomplete = s ? "new-password" : "current-password";
}
$("lg-forgot").addEventListener("click", () => { $("lg-who").hidden = !$("lg-who").hidden; });
document.querySelectorAll("#lg-who [data-who]").forEach((b) => b.addEventListener("click", async () => {
  const who = b.dataset.who, helper = who === "nakita" ? "Colin" : "Nakita";
  b.disabled = true;
  await sb.functions.invoke("notify", { body: { action: "ask-reset", who } }).catch(() => {});
  b.disabled = false; $("lg-who").hidden = true;
  showLogin(`${helper} has been sent a notification to help. Once ${helper} tells you the temporary password, sign in with it here.`);
  $("lg-form").dataset.mode = "signin"; syncMode();
}));
$("lg-switch").addEventListener("click", () => { const f = $("lg-form"); f.dataset.mode = f.dataset.mode === "signup" ? "signin" : "signup"; syncMode(); });
syncMode();

const { data: { session } } = await sb.auth.getSession();
if (session) finish(session); else showLogin();
