#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ONE-FILE Shopify counters:
THIS CODE WORKS AND INCREASES THE METAFIELDS VIEWS AND ADD_TO_CART_TOTAL BY 1 EVERY TIME
***
DIRECTIONS TO RUN
****
1. Navigate to the folder containing this python file with cd..cd.. cd
2. copy paste below:
    source .venv/bin/activate
    pip install --upgrade pip
    pip install flask requests
    python rd_counters.py
3. In a new terminal, run: cloudflared tunnel --url http://localhost:5050 --loglevel info
4. Within a few seconds you’ll see a line with a URL like:"https://disciplinary-truck-probe-mug.trycloudflare.com ", copy this URL
5. Open shopify customer events pixels and replace the URL with this url in the fetch() lines

- Receives Web Pixel 'product_viewed' hits  → custom.views_total
- Receives Web Pixel 'product_added_to_cart' → custom.added_to_cart_total
- Receives 'orders/paid' webhook            → custom.sales_total, custom.sales_dates
- Pushes metafields via Admin GraphQL every FLUSH_INTERVAL_SEC
"""

import os, csv, json, time, hmac, base64, threading, ipaddress
from datetime import datetime, timezone
from collections import defaultdict

from flask import Flask, request, make_response
import requests

# ---------- Helpers ----------
def normalize_host(h: str) -> str:
    """lowercase, strip scheme, drop leading www."""
    h = (h or "").lower().strip()
    h = h.replace("https://","").replace("http://","")
    return h[4:] if h.startswith("www.") else h

def to_iso8601(ts):
    """Accept ms/sec numbers or ISO strings; return ISO-8601 Z string."""
    if ts is None:
        return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    try:
        val = float(ts)
        if val > 1e12:   val = val / 1e6   # microseconds
        elif val > 1e10: val = val / 1e3   # milliseconds
        return datetime.utcfromtimestamp(val).isoformat() + 'Z'
    except Exception:
        pass
    s = str(ts)
    try:
        dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
        return dt.astimezone(timezone.utc).isoformat().replace('+00:00','Z')
    except Exception:
        return datetime.now(timezone.utc).isoformat().replace('+00:00','Z')

def get_client_ip(req) -> str:
    """Best-effort real client IP behind tunnels/CDNs."""
    h = req.headers
    ip = (h.get("CF-Connecting-IP")
          or h.get("True-Client-IP")
          or (h.get("X-Forwarded-For") or "").split(",")[0].strip()
          or req.remote_addr
          or "")
    return ip

def _parse_networks(items):
    nets = []
    for s in items:
        s = (s or "").strip()
        if not s:
            continue
        try:
            if "/" in s:
                nets.append(ipaddress.ip_network(s, strict=False))
            else:
                nets.append(ipaddress.ip_network(s + ("/32" if ":" not in s else "/128"), strict=False))
        except Exception:
            print(f"[IP] invalid ignore entry: {s}")
    return nets

def ip_is_ignored(ip: str) -> bool:
    if not ip:
        return False
    try:
        ip_obj = ipaddress.ip_address(ip)
    except Exception:
        return False
    return any(ip_obj in net for net in IGNORE_NETS)

# ========= EDIT THESE =========
API_VERSION   = "2024-10"                 # Shopify Admin API version
METAFIELD_NS  = "custom"
KEY_VIEWS     = "views_total"
KEY_SALES     = "sales_total"
KEY_DATES     = "sales_dates"             # list.date
KEY_ATC       = "added_to_cart_total"     # number_integer

COUNT_MODE    = "units"                   # "units" or "orders"
SALES_DATES_LIMIT = 365                   # keep up to N unique sale dates per product

# Only this store for now; read token from environment (safer than hardcoding)
ADMIN_HOST = os.getenv("ADMIN_HOST", "silver-rudradhan.myshopify.com")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # must be set in Render/local env
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN env var is not set")
ADMIN_TOKENS = {ADMIN_HOST: ADMIN_TOKEN}

# (optional but recommended) also let secrets come from env
PIXEL_SHARED_SECRET = os.environ["PIXEL_SHARED_SECRET"]          # raises if missing
SHOPIFY_WEBHOOK_SECRET = os.environ["SHOPIFY_WEBHOOK_SECRET"]    # raises if missing



# Ignore views/ATC from these IPs/CIDRs
IGNORE_IPS = [
    # "203.0.113.42",
    # "192.168.29.0/24",
]
IGNORE_NETS = _parse_networks(IGNORE_IPS)

# Server port (change if 5000 is busy)
PORT = int(os.environ.get("PORT", 5050))
FLUSH_INTERVAL_SEC = 5
# ============================

# Files (stored alongside this script)
EVENTS_CSV        = "events.csv"            # product_viewed audit
ATC_CSV           = "atc_events.csv"        # product_added_to_cart audit
VIEWS_JSON        = "view_counts.json"      # productId -> int
ATC_JSON          = "atc_counts.json"       # productId -> int
SALES_JSON        = "sales_counts.json"     # productId -> int
SALE_DATES_JSON   = "sale_dates.json"       # productId -> sorted list of YYYY-MM-DD
PROCESSED_ORDERS  = "processed_orders.json" # set of order IDs to avoid double-count

# ---- State ----
app = Flask(__name__)
lock = threading.Lock()

view_counts  = defaultdict(int)    # productId -> views total
atc_counts   = defaultdict(int)    # productId -> added-to-cart total
sales_counts = defaultdict(int)    # productId -> sales total (units or orders)
sale_dates   = defaultdict(set)    # productId -> set of date strings
processed_orders = set()           # seen order IDs (to dedupe webhook retries)

dirty_views = set()                # {(shop_host, productId)}
dirty_atc   = set()                # {(shop_host, productId)}
dirty_sales = set()                # {(shop_host, productId)}

# ---- Persistence helpers ----
def _load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return default

def _save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)

# Init persisted state
if not os.path.exists(EVENTS_CSV):
    with open(EVENTS_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["ts_iso","shop","productId","handle","sessionId","userAgent"])
if not os.path.exists(ATC_CSV):
    with open(ATC_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["ts_iso","shop","productId","qty","handle","sessionId","userAgent","ip"])

_view_disk = _load_json(VIEWS_JSON, {})
for k, v in _view_disk.items():
    view_counts[str(k)] = int(v)

_atc_disk = _load_json(ATC_JSON, {})
for k, v in _atc_disk.items():
    atc_counts[str(k)] = int(v)

_sales_disk = _load_json(SALES_JSON, {})
for k, v in _sales_disk.items():
    sales_counts[str(k)] = int(v)

_dates_disk = _load_json(SALE_DATES_JSON, {})
for k, lst in _dates_disk.items():
    sale_dates[str(k)] = set(lst)

processed_orders = set(_load_json(PROCESSED_ORDERS, []))

def _persist_all():
    _save_json(VIEWS_JSON, {k:int(v) for k,v in view_counts.items()})
    _save_json(ATC_JSON,  {k:int(v) for k,v in atc_counts.items()})
    _save_json(SALES_JSON,{k:int(v) for k,v in sales_counts.items()})
    _save_json(SALE_DATES_JSON, {k:sorted(list(v)) for k,v in sale_dates.items()})
    _save_json(PROCESSED_ORDERS, sorted(list(processed_orders)))

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Shopify-Topic, X-Shopify-Hmac-SHA256, X-Shopify-Shop-Domain"
    return resp

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

# --------- PIXEL endpoint: /track/product (views) ---------
@app.route("/track/product", methods=["OPTIONS"])
def track_options():
    return _cors(make_response("", 204))

@app.route("/track/product", methods=["POST"])
def track_product():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        print(f"[AUTH] 403 pixel key mismatch (got len={len(key)} expected len={len(PIXEL_SHARED_SECRET)})")
        return _cors(make_response(("forbidden", 403)))

    cip = get_client_ip(request)
    if ip_is_ignored(cip):
        return _cors(make_response(("ok", 200)))

    data = request.get_json(silent=True) or {}
    product_id = str(data.get("productId") or "").strip()
    handle     = (data.get("handle") or "").strip()
    session_id = (data.get("sessionId") or "").strip()
    user_agent = (data.get("userAgent") or "").strip()
    shop_host  = normalize_host(data.get("shop"))

    # only this storefront counts views
    if shop_host != "rudradhan.com":
        return _cors(make_response(("ok", 200)))
    if not product_id:
        return _cors(make_response(("bad request", 400)))

    ts_iso = to_iso8601(data.get("ts"))

    with open(EVENTS_CSV, "a", newline="") as f:
        csv.writer(f).writerow([ts_iso, shop_host, product_id, handle, session_id, user_agent])

    with lock:
        view_counts[product_id] = int(view_counts.get(product_id, 0)) + 1
        dirty_views.add((shop_host, product_id))

    return _cors(make_response(("ok", 200)))

# --------- PIXEL endpoint: /track/atc (added-to-cart) ---------
@app.route("/track/atc", methods=["OPTIONS"])
def atc_options():
    return _cors(make_response("", 204))

@app.route("/track/atc", methods=["POST"])
def track_atc():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        print(f"[AUTH] 403 pixel key mismatch (ATC) (got len={len(key)} expected len={len(PIXEL_SHARED_SECRET)})")
        return _cors(make_response(("forbidden", 403)))

    cip = get_client_ip(request)
    if ip_is_ignored(cip):
        return _cors(make_response(("ok", 200)))

    data = request.get_json(silent=True) or {}
    product_id = str(data.get("productId") or "").strip()
    qty        = int(data.get("qty") or 1)
    handle     = (data.get("handle") or "").strip()
    session_id = (data.get("sessionId") or "").strip()
    user_agent = (data.get("userAgent") or "").strip()
    shop_host  = normalize_host(data.get("shop"))

    # only this storefront counts ATC
    if shop_host != "rudradhan.com":
        return _cors(make_response(("ok", 200)))
    if not product_id:
        return _cors(make_response(("bad request", 400)))
    if qty < 1:
        qty = 1

    ts_iso = to_iso8601(data.get("ts"))

    with open(ATC_CSV, "a", newline="") as f:
        csv.writer(f).writerow([ts_iso, shop_host, product_id, qty, handle, session_id, user_agent, cip])

    with lock:
        atc_counts[product_id] = int(atc_counts.get(product_id, 0)) + qty
        dirty_atc.add((shop_host, product_id))

    return _cors(make_response(("ok", 200)))

# --------- WEBHOOK endpoint: /webhook/orders_paid (sales) ---------
def verify_hmac(req_body: bytes, header_hmac: str) -> bool:
    try:
        digest = hmac.new(
            key=SHOPIFY_WEBHOOK_SECRET.encode("utf-8"),
            msg=req_body,
            digestmod="sha256"
        ).digest()
        expected = base64.b64encode(digest).decode("utf-8")
        return hmac.compare_digest(expected, header_hmac or "")
    except Exception:
        return False

@app.route("/webhook/orders_paid", methods=["POST"])
def orders_paid():
    h = request.headers.get("X-Shopify-Hmac-SHA256", "")
    raw = request.get_data()
    if not verify_hmac(raw, h):
        return "unauthorized", 401

    shop_host_hdr = request.headers.get("X-Shopify-Shop-Domain", "").strip()
    shop_host = normalize_host(shop_host_hdr)
    if shop_host not in ADMIN_TOKENS:
        return "ok", 200

    payload = request.get_json(silent=True) or {}
    order_id = str(payload.get("id") or payload.get("admin_graphql_api_id") or "")
    if not order_id:
        return "ok", 200

    with lock:
        if order_id in processed_orders:
            return "ok", 200
        processed_orders.add(order_id)

    created_at = payload.get("created_at") or payload.get("processed_at")
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00")) if created_at else datetime.now(timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    date_str = dt.date().isoformat()

    lines = payload.get("line_items") or []
    counted_in_order = set()  # for COUNT_MODE='orders'

    with lock:
        for li in lines:
            pid = li.get("product_id")
            qty = int(li.get("quantity") or 0)
            if not pid:
                continue
            pid = str(pid)

            if COUNT_MODE == "orders":
                if pid in counted_in_order:
                    continue
                sales_counts[pid] += 1
                counted_in_order.add(pid)
            else:
                sales_counts[pid] += max(qty, 0)

            sd = sale_dates[pid]
            if date_str not in sd:
                sd.add(date_str)
                if len(sd) > SALES_DATES_LIMIT:
                    newest_sorted = sorted(sd)[-SALES_DATES_LIMIT:]
                    sale_dates[pid] = set(newest_sorted)

            dirty_sales.add((shop_host, pid))

    return "ok", 200

# --------- Push metafields periodically ---------
def metafields_set(shop_host, token, metafields):
    url = f"https://{shop_host}/admin/api/{API_VERSION}/graphql.json"
    query = """
      mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
        metafieldsSet(metafields: $metafields) {
          metafields { id key namespace ownerType }
          userErrors { field message code }
        }
      }
    """
    try:
        r = requests.post(
            url,
            headers={"Content-Type": "application/json", "X-Shopify-Access-Token": token},
            json={"query": query, "variables": {"metafields": metafields}},
            timeout=25
        )
        text = r.text
        try:
            j = r.json()
        except Exception:
            print(f"[GRAPHQL] non-JSON response {r.status_code}: {text[:500]}")
            return False

        errs = (j.get("data", {}) or {}).get("metafieldsSet", {}).get("userErrors", [])
        if errs:
            print(f"[GRAPHQL] userErrors ({shop_host}): {errs}")
        ok = r.ok and not errs
        if not ok:
            if metafields:
                print("[GRAPHQL] first metafield payload:", metafields[0])
            print(f"[GRAPHQL] status={r.status_code}")
        return ok
    except Exception as e:
        print(f"[{shop_host}] push error:", e)
        return False

def flusher():
    # Always push to the configured myshopify Admin host (first/only key)
    ADMIN_HOST = list(ADMIN_TOKENS.keys())[0]
    TOKEN = ADMIN_TOKENS[ADMIN_HOST]

    while True:
        time.sleep(FLUSH_INTERVAL_SEC)

        with lock:
            if not dirty_views and not dirty_atc and not dirty_sales:
                continue
            to_push = {}  # pid -> set("views","atc","sales")
            for (_shop, pid) in dirty_views:
                to_push.setdefault(pid, set()).add("views")
            for (_shop, pid) in dirty_atc:
                to_push.setdefault(pid, set()).add("atc")
            for (_shop, pid) in dirty_sales:
                to_push.setdefault(pid, set()).add("sales")
            dirty_views.clear(); dirty_atc.clear(); dirty_sales.clear()

        mfs = []
        for pid, kinds in to_push.items():
            if "views" in kinds:
                mfs.append({
                    "ownerId": f"gid://shopify/Product/{pid}",
                    "namespace": METAFIELD_NS,
                    "key": KEY_VIEWS,
                    "type": "number_integer",
                    "value": str(int(view_counts.get(pid, 0)))
                })
            if "atc" in kinds:
                mfs.append({
                    "ownerId": f"gid://shopify/Product/{pid}",
                    "namespace": METAFIELD_NS,
                    "key": KEY_ATC,
                    "type": "number_integer",
                    "value": str(int(atc_counts.get(pid, 0)))
                })
            if "sales" in kinds:
                mfs.append({
                    "ownerId": f"gid://shopify/Product/{pid}",
                    "namespace": METAFIELD_NS,
                    "key": KEY_SALES,
                    "type": "number_integer",
                    "value": str(int(sales_counts.get(pid, 0)))
                })
                dates = sorted(list(sale_dates.get(pid, set())))  # list.date expects JSON array of strings
                mfs.append({
                    "ownerId": f"gid://shopify/Product/{pid}",
                    "namespace": METAFIELD_NS,
                    "key": KEY_DATES,
                    "type": "list.date",
                    "value": json.dumps(dates)
                })

        CHUNK = 25
        items = list(mfs)
        if items:
            print(f"[PUSH] preview ownerId/key/type -> {items[0].get('ownerId')}, {items[0].get('key')}, {items[0].get('type')}")
        for i in range(0, len(items), CHUNK):
            chunk = items[i:i+CHUNK]
            ok = metafields_set(ADMIN_HOST, TOKEN, chunk)
            print(f"[PUSH] {ADMIN_HOST}: {len(chunk)} -> {'OK' if ok else 'ERR'}")
            time.sleep(0.3)

        _persist_all()

# (Optional) manual flush during testing
@app.route("/flush", methods=["GET"])
def flush_now():
    return "flushing", 200

# start background flusher
threading.Thread(target=flusher, daemon=True).start()

if __name__ == "__main__":
    print(f"Running on port {PORT} …")
    app.run(host="0.0.0.0", port=PORT)
