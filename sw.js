// Home Base service worker: lock-screen notifications and a quick-loading app shell.
const CACHE = "hb-v1";
const SHELL = ["./", "./index.html", "./hb-runtime.js", "./config.js", "./manifest.webmanifest", "./icon-192.png", "./apple-touch-icon.png"];

self.addEventListener("install", (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).then(() => self.skipWaiting()));
});
self.addEventListener("activate", (e) => {
  e.waitUntil(caches.keys().then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))).then(() => self.clients.claim()));
});

// Network first so updates show straight away; fall back to the saved copy when offline.
self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (e.request.method !== "GET" || url.origin !== self.location.origin) return;
  e.respondWith(
    fetch(e.request)
      .then((r) => { const copy = r.clone(); caches.open(CACHE).then((c) => c.put(e.request, copy)); return r; })
      .catch(() => caches.match(e.request).then((r) => r || caches.match("./index.html")))
  );
});

self.addEventListener("push", (e) => {
  let n = { title: "Home Base", body: "" };
  try { n = Object.assign(n, e.data.json()); } catch (err) { n.body = e.data ? e.data.text() : ""; }
  e.waitUntil(self.registration.showNotification(n.title, {
    body: n.body,
    icon: "icon-192.png",
    badge: "icon-192.png",
    tag: n.tag || undefined,
    renotify: !!n.tag,
    data: { url: n.url || "./" },
  }));
});

self.addEventListener("notificationclick", (e) => {
  e.notification.close();
  const target = new URL(e.notification.data && e.notification.data.url || "./", self.registration.scope).href;
  e.waitUntil(self.clients.matchAll({ type: "window", includeUncontrolled: true }).then((list) => {
    for (const c of list) { if ("focus" in c) { c.navigate(target).catch(() => {}); return c.focus(); } }
    return self.clients.openWindow(target);
  }));
});
