#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ONE-FILE Shopify counters

What it does
------------
- Web Pixel 'product_viewed'              → custom.views_total
- Web Pixel 'product_added_to_cart'       → custom.added_to_cart_total
- orders/paid webhook                     → custom.sales_total, custom.sales_dates
- Age job (custom.age_in_days)            → computed ONLY from custom.dob (no fallback)
- Pushes metafields via Admin GraphQL every FLUSH_INTERVAL_SEC

NEW (as requested)
------------------
- /dob/set_all_today (GET/POST)           → One-time bulk: set custom.dob = TODAY for ALL products, recompute age
- /webhook/products_create (POST)         → On new product, set custom.dob = product.created_at and compute age

Other admin endpoints (already present)
---------------------------------------
- /dob/set (POST)                         → body {"productId":"...","dob":"YYYY-MM-DD"}
- /dob/bulk (POST)                        → body [{"productId":"...","dob":"YYYY-MM-DD"}, ...]
- /dob/backfill_created_at (GET/POST)     → Set dob = createdAt for products missing dob (optional convenience)
- /age/recompute  (GET/POST)              → recompute age for products we've seen
- /age/seed_all   (GET/POST)              → compute age for all products that already have dob

Notes
-----
- In Shopify Admin, create metafield definitions once:
  Products → Settings → Custom data → Products → Add definition
    - custom.dob          = Date
    - custom.age_in_days  = Integer
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
KEY_AGE       = "age_in_days"             # number_integer
KEY_DOB       = "dob"                     # date

COUNT_MODE    = "units"                   # "units" or "orders"
SALES_DATES_LIMIT = 365                   # keep up to N unique sale dates per product

# Only this store; token from env
ADMIN_HOST  = os.getenv("ADMIN_HOST", "silver-rudradhan.myshopify.com")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN")  # must be set in Render/local env
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN env var is not set")
ADMIN_TOKENS = {ADMIN_HOST: ADMIN_TOKEN}

# Secrets from env (required)
PIXEL_SHARED_SECRET    = os.environ["PIXEL_SHARED_SECRET"]
SHOPIFY_WEBHOOK_SECRET = os.environ["SHOPIFY_WEBHOOK_SECRET"]

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
AGE_JSON          = "age_days.json"         # productId -> int
DOB_CACHE_JSON    = "dob_cache.json"        # productId -> "YYYY-MM-DD"

# ---- State ----
app = Flask(__name__)
lock = threading.Lock()

view_counts  = defaultdict(int)    # productId -> views total
atc_counts   = defaultdict(int)    # productId -> added-to-cart total
sales_counts = defaultdict(int)    # productId -> sales total (units or orders)
sale_dates   = defaultdict(set)    # productId -> set of date strings
age_days     = defaultdict(int)    # productId -> age in days
dob_cache    = {}                  # productId -> DOB string
processed_orders = set()           # seen order IDs (to dedupe webhook retries)

dirty_views = set()                # {(shop_host, productId)}
dirty_atc   = set()                # {(shop_host, productId)}
dirty_sales = set()                # {(shop_host, productId)}
dirty_age   = set()                # {(shop_host, productId)}

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

_age_disk = _load_json(AGE_JSON, {})
for k, v in _age_disk.items():
    age_days[str(k)] = int(v)

dob_cache.update(_load_json(DOB_CACHE_JSON, {}))
processed_orders = set(_load_json(PROCESSED_ORDERS, []))

def _persist_all():
    _save_json(VIEWS_JSON, {k:int(v) for k,v in view_counts.items()})
    _save_json(ATC_JSON,  {k:int(v) for k,v in atc_counts.items()})
    _save_json(SALES_JSON,{k:int(v) for k,v in sales_counts.items()})
    _save_json(SALE_DATES_JSON, {k:sorted(list(v)) for k,v in sale_dates.items()})
    _save_json(AGE_JSON, {k:int(v) for k,v in age_days.items()})
    _save_json(DOB_CACHE_JSON, dob_cache)
    _save_json(PROCESSED_ORDERS, sorted(list(processed_orders)))

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS, GET"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type, X-Shopify-Topic, X-Shopify-Hmac-SHA256, X-Shopify-Shop-Domain, X-Forwarded-For, CF-Connecting-IP, True-Client-IP"
    return resp

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

# --------- AGE helpers (DOB → age_in_days) ---------
def fetch_product_dob(pid: str) -> str:
    """
    Return DOB 'YYYY-MM-DD' for product FROM custom.dob ONLY.
    No fallback to createdAt here.
    """
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    gid = f"gid://shopify/Product/{pid}"
    query = """
      query($id: ID!) {
        node(id: $id) {
          ... on Product {
            dob: metafield(namespace: "custom", key: "dob") { value }
          }
        }
      }
    """
    try:
        r = requests.post(
            f"https://{admin_host}/admin/api/{API_VERSION}/graphql.json",
            headers={"Content-Type": "application/json", "X-Shopify-Access-Token": token},
            json={"query": query, "variables": {"id": gid}},
            timeout=25
        )
        j = r.json()
        node = (j.get("data") or {}).get("node") or {}
        dob_val = ((node.get("dob") or {}) or {}).get("value")
        return dob_val or ""
    except Exception as e:
        print(f"[DOB] fetch error for {pid}: {e}")
    return ""

def _compute_age_for_pid(pid: str, today_date):
    """Compute & mark dirty age for one product. Returns True if changed."""
    dob = dob_cache.get(pid)
    if not dob:
        dob = fetch_product_dob(pid)
        if dob:
            dob_cache[pid] = dob
    if not dob:
        return False  # unknown DOB

    try:
        if "T" in dob:
            dob_date = datetime.fromisoformat(dob.replace("Z","+00:00")).date()
        else:
            from datetime import datetime as dtmod
            dob_date = dtmod.strptime(dob, "%Y-%m-%d").date()
    except Exception:
        return False

    new_age = max((today_date - dob_date).days, 0)
    old_age = age_days.get(pid, -1)
    if new_age != old_age:
        age_days[pid] = new_age
        dirty_age.add(("rudradhan.com", pid))
        return True
    return False

def _recompute_age_for_known_pids():
    today = datetime.now(timezone.utc).date()
    with lock:
        pids = set(view_counts.keys()) | set(atc_counts.keys()) | set(sales_counts.keys())
    changed = 0
    for pid in pids:
        try:
            if _compute_age_for_pid(pid, today):
                changed += 1
        except Exception:
            pass
    if changed:
        _persist_all()
    return changed

# --------- Admin GraphQL helpers ---------
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

# --------- Product listing ---------
def _list_all_product_nodes():
    """
    Returns: [{"pid": "1234567890", "createdAt": "...", "dob": "YYYY-MM-DD"|None}, ...]
    """
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    query = """
      query($after: String) {
        products(first: 200, after: $after) {
          edges {
            cursor
            node {
              id
              createdAt
              dob: metafield(namespace: "custom", key: "dob") { value }
            }
          }
          pageInfo { hasNextPage }
        }
      }
    """
    nodes, after = [], None
    while True:
        r = requests.post(
            f"https://{admin_host}/admin/api/{API_VERSION}/graphql.json",
            headers={"Content-Type":"application/json","X-Shopify-Access-Token":token},
            json={"query": query, "variables": {"after": after}},
            timeout=25
        )
        j = r.json()
        data = (j.get("data") or {}).get("products") or {}
        edges = data.get("edges") or []
        for e in edges:
            n = e.get("node") or {}
            gid = n.get("id") or ""
            pid = gid.split("/")[-1] if gid else ""
            nodes.append({
                "pid": pid,
                "createdAt": n.get("createdAt"),
                "dob": ((n.get("dob") or {}) or {}).get("value")
            })
        if not data.get("pageInfo", {}).get("hasNextPage"):
            break
        after = edges[-1]["cursor"] if edges else None
        if not after:
            break
    return nodes

# --------- DOB writers (single / bulk / special) ---------
def _set_product_dob(pid: str, dob_str: str) -> bool:
    """Write custom.dob for a product; also compute its age immediately."""
    try:
        datetime.strptime(dob_str, "%Y-%m-%d")
    except Exception:
        print(f"[DOB] invalid date for pid={pid}: {dob_str}")
        return False

    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    mfs = [{
        "ownerId": f"gid://shopify/Product/{pid}",
        "namespace": METAFIELD_NS,
        "key": KEY_DOB,
        "type": "date",
        "value": dob_str
    }]
    ok = metafields_set(admin_host, token, mfs)
    if ok:
        dob_cache[pid] = dob_str
        try:
            _compute_age_for_pid(pid, datetime.now(timezone.utc).date())
        except Exception:
            pass
    return ok

@app.route("/dob/set", methods=["POST"])
def dob_set_single():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return "forbidden", 403
    data = request.get_json(silent=True) or {}
    pid = str(data.get("productId") or "").strip()
    dob = str(data.get("dob") or "").strip()
    if not pid or not dob:
        return "bad request", 400
    ok = _set_product_dob(pid, dob)
    return ("ok", 200) if ok else ("failed", 500)

@app.route("/dob/bulk", methods=["POST"])
def dob_bulk():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return "forbidden", 403
    items = request.get_json(silent=True) or []
    to_write = []
    for it in items:
        pid = str(it.get("productId") or "").strip()
        dob = str(it.get("dob") or "").strip()
        try:
            datetime.strptime(dob, "%Y-%m-%d")
        except Exception:
            continue
        if pid and dob:
            to_write.append({
                "ownerId": f"gid://shopify/Product/{pid}",
                "namespace": METAFIELD_NS,
                "key": KEY_DOB,
                "type": "date",
                "value": dob
            })
    if not to_write:
        return "no valid rows", 400

    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    CHUNK = 25
    ok_all = True
    for i in range(0, len(to_write), CHUNK):
        chunk = to_write[i:i+CHUNK]
        ok = metafields_set(admin_host, token, chunk)
        ok_all = ok_all and ok
        time.sleep(0.3)

    today = datetime.now(timezone.utc).date()
    for it in items:
        pid = str(it.get("productId") or "").strip()
        dob = str(it.get("dob") or "").strip()
        if pid and dob:
            dob_cache[pid] = dob
            try:
                _compute_age_for_pid(pid, today)
            except Exception:
                pass
    return ("ok", 200) if ok_all else ("partial/fail", 207)

@app.route("/dob/backfill_created_at", methods=["POST","GET"])
def dob_backfill_created_at():
    """Set custom.dob = createdAt for products that currently don't have dob."""
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return "forbidden", 403

    nodes = _list_all_product_nodes()
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]
    to_write = []
    today = datetime.now(timezone.utc).date()
    for n in nodes:
        if n.get("dob"):
            continue
        pid = n.get("pid")
        createdAt = n.get("createdAt")
        if not pid or not createdAt:
            continue
        try:
            dt = datetime.fromisoformat(createdAt.replace("Z","+00:00")).date()
        except Exception:
            continue
        dob_str = dt.isoformat()
        to_write.append({
            "ownerId": f"gid://shopify/Product/{pid}",
            "namespace": METAFIELD_NS,
            "key": KEY_DOB,
            "type": "date",
            "value": dob_str
        })
        dob_cache[pid] = dob_str
        try:
            _compute_age_for_pid(pid, today)
        except Exception:
            pass

    if not to_write:
        return "nothing to backfill", 200

    CHUNK = 25
    wrote = 0
    for i in range(0, len(to_write), CHUNK):
        chunk = to_write[i:i+CHUNK]
        ok = metafields_set(admin_host, token, chunk)
        if ok:
            wrote += len(chunk)
        time.sleep(0.3)
    return f"backfilled dob for {wrote} product(s)", 200

@app.route("/dob/set_all_today", methods=["POST","GET"])
def dob_set_all_today():
    """
    DANGER (bulk overwrite):
    Sets custom.dob = TODAY for EVERY product, recomputes age=0, and pushes.
    """
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return "forbidden", 403

    nodes = _list_all_product_nodes()
    today = datetime.now(timezone.utc).date().isoformat()
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]

    to_write, touched = [], 0
    for n in nodes:
        pid = n.get("pid")
        if not pid:
            continue
        to_write.append({
            "ownerId": f"gid://shopify/Product/{pid}",
            "namespace": METAFIELD_NS,
            "key": KEY_DOB,
            "type": "date",
            "value": today
        })
        dob_cache[pid] = today
        age_days[pid] = 0
        dirty_age.add(("rudradhan.com", pid))
        touched += 1

    if not to_write:
        return "no products found", 200

    CHUNK = 25
    wrote = 0
    for i in range(0, len(to_write), CHUNK):
        chunk = to_write[i:i+CHUNK]
        ok = metafields_set(admin_host, token, chunk)
        if ok:
            wrote += len(chunk)
        time.sleep(0.3)

    _persist_all()
    return f"set dob=today for {wrote}/{touched} product(s)", 200

# --------- AGE recompute helpers ---------
def _seed_age_for_all_products():
    """
    Compute age_in_days ONLY for products that already have custom.dob.
    """
    today = datetime.now(timezone.utc).date()
    nodes = _list_all_product_nodes()
    changed = 0
    for n in nodes:
        pid = n["pid"]
        if not pid:
            continue
        dob_str = n.get("dob")
        if not dob_str:
            continue
        try:
            if "T" in dob_str:
                dob_date = datetime.fromisoformat(dob_str.replace("Z","+00:00")).date()
            else:
                from datetime import datetime as dtmod
                dob_date = dtmod.strptime(dob_str, "%Y-%m-%d").date()
        except Exception:
            continue

        new_age = max((today - dob_date).days, 0)
        old_age = age_days.get(pid, -1)
        if new_age != old_age:
            age_days[pid] = new_age
            dob_cache[pid] = dob_str
            dirty_age.add(("rudradhan.com", pid))
            changed += 1
    if changed:
        _persist_all()
    return changed

@app.route("/age/seed_all", methods=["GET","POST"])
def age_seed_all():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return "forbidden", 403
    changed = _seed_age_for_all_products()
    return f"seeded age for {changed} product(s)", 200

@app.route("/age/recompute", methods=["GET","POST"])
def age_recompute_now():
    key = (request.args.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return "forbidden", 403
    changed = _recompute_age_for_known_pids()
    return f"recomputed ages for {changed} product(s)", 200

# --------- PIXEL endpoints (views / ATC) ---------
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
    try:
        _compute_age_for_pid(product_id, datetime.now(timezone.utc).date())
    except Exception:
        pass

    return _cors(make_response(("ok", 200)))

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
    try:
        _compute_age_for_pid(product_id, datetime.now(timezone.utc).date())
    except Exception:
        pass

    return _cors(make_response(("ok", 200)))

# --------- WEBHOOKS ---------
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
    counted_in_order = set()

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
            try:
                _compute_age_for_pid(pid, datetime.now(timezone.utc).date())
            except Exception:
                pass

    return "ok", 200

@app.route("/webhook/products_create", methods=["POST"])
def products_create():
    """
    On product creation, set custom.dob = created_at (true DOB at birth),
    compute age_in_days (0), and push.
    """
    h = request.headers.get("X-Shopify-Hmac-SHA256", "")
    raw = request.get_data()
    if not verify_hmac(raw, h):
        return "unauthorized", 401

    shop_host_hdr = request.headers.get("X-Shopify-Shop-Domain", "").strip()
    shop_host = normalize_host(shop_host_hdr)
    if shop_host not in ADMIN_TOKENS:
        return "ok", 200

    payload = request.get_json(silent=True) or {}
    # Webhook payload has numeric product id and created_at
    pid = str(payload.get("id") or "")
    created_at = payload.get("created_at")

    if not pid or not created_at:
        return "ok", 200

    try:
        dob_date = datetime.fromisoformat(created_at.replace("Z","+00:00")).date()
    except Exception:
        dob_date = datetime.now(timezone.utc).date()

    dob_str = dob_date.isoformat()
    admin_host = list(ADMIN_TOKENS.keys())[0]
    token = ADMIN_TOKENS[admin_host]

    # Write DOB
    ok = metafields_set(admin_host, token, [{
        "ownerId": f"gid://shopify/Product/{pid}",
        "namespace": METAFIELD_NS,
        "key": KEY_DOB,
        "type": "date",
        "value": dob_str
    }])

    if ok:
        dob_cache[pid] = dob_str
        age_days[pid] = 0
        dirty_age.add((shop_host, pid))

    return "ok", 200

# --------- Pusher ---------
def flusher():
    ADMIN_HOST = list(ADMIN_TOKENS.keys())[0]
    TOKEN = ADMIN_TOKENS[ADMIN_HOST]

    while True:
        time.sleep(FLUSH_INTERVAL_SEC)

        with lock:
            if not dirty_views and not dirty_atc and not dirty_sales and not dirty_age:
                continue
            to_push = {}  # pid -> set("views","atc","sales","age")
            for (_shop, pid) in dirty_views:
                to_push.setdefault(pid, set()).add("views")
            for (_shop, pid) in dirty_atc:
                to_push.setdefault(pid, set()).add("atc")
            for (_shop, pid) in dirty_sales:
                to_push.setdefault(pid, set()).add("sales")
            for (_shop, pid) in dirty_age:
                to_push.setdefault(pid, set()).add("age")
            dirty_views.clear(); dirty_atc.clear(); dirty_sales.clear(); dirty_age.clear()

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
                dates = sorted(list(sale_dates.get(pid, set())))
                mfs.append({
                    "ownerId": f"gid://shopify/Product/{pid}",
                    "namespace": METAFIELD_NS,
                    "key": KEY_DATES,
                    "type": "list.date",
                    "value": json.dumps(dates)
                })
            if "age" in kinds:
                mfs.append({
                    "ownerId": f"gid://shopify/Product/{pid}",
                    "namespace": METAFIELD_NS,
                    "key": KEY_AGE,
                    "type": "number_integer",
                    "value": str(int(age_days.get(pid, 0)))
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

# --------- Daily age recompute (UTC) ---------
def daily_age_updater():
    """Recompute age_in_days once per UTC day for products we've seen."""
    last_run_date = None
    while True:
        today = datetime.now(timezone.utc).date()
        if today != last_run_date:
            with lock:
                pids = set(view_counts.keys()) | set(atc_counts.keys()) | set(sales_counts.keys())
            changed = 0
            for pid in pids:
                try:
                    if _compute_age_for_pid(pid, today):
                        changed += 1
                except Exception:
                    pass
            if changed:
                print(f"[AGE] recomputed ages for {changed} product(s)")
                _persist_all()
            last_run_date = today
        time.sleep(1800)

# (Optional) manual flush during testing
@app.route("/flush", methods=["GET"])
def flush_now():
    return "flushing", 200

# Start background workers
threading.Thread(target=flusher, daemon=True).start()
threading.Thread(target=daily_age_updater, daemon=True).start()

if __name__ == "__main__":
    print(f"Running on port {PORT} …")
    app.run(host="0.0.0.0", port=PORT)
