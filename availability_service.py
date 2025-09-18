#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Shopify availability → status + metafields updater (collection-scoped)
Runs as a web service with on-demand endpoints and an optional scheduler.

Behavior (same as your script):
- Availability = sum of tracked variants' inventoryQuantity.
- If availability == 0:
    - (optionally) status → DRAFT        [CHANGE_STATUS]
    - badges → **deleted** (clear all)
    - delivery_time → "12-15 Days Across India"
- If availability > 0:
    - (optionally) status → ACTIVE       [CHANGE_STATUS]
    - badges → "Ready To Ship" (exact choice)
    - delivery_time → "2-5 Days Across India"

Security:
- Endpoints require ?key=PIXEL_SHARED_SECRET (env var) for manual triggers.

Scheduler:
- If RUN_EVERY_MIN > 0, a background thread runs all handles in COLLECTION_HANDLES every N minutes.

Env you MUST set (Render → Environment):
- ADMIN_HOST                e.g. silver-rudradhan.myshopify.com
- ADMIN_TOKEN               your Admin API access token
- PIXEL_SHARED_SECRET       same one you use for the pixel service
- (optional) COLLECTION_HANDLES    comma-separated collection handles (default: pearl-pendant-gold-plated)
- (optional) RUN_EVERY_MIN         integer minutes (default: 0 = disabled)
- (optional) CHANGE_STATUS         "1" to enable product status changes (default: "0")
- (optional) DRY_RUN               "1" to simulate writes (default: "0")
- (optional) FORCE_WRITE_METAFIELDS "1" to force rewrite on READY↔MTO flips (default: "1")

Start command (Render):
/opt/render/project/src/.venv/bin/gunicorn -w 1 -t 600 -b 0.0.0.0:$PORT availability_service:app
"""

import os, time, json, sys, csv, re, warnings, threading
from datetime import datetime
from typing import Dict, Any, Optional, List, Tuple

import requests
from flask import Flask, request, jsonify, make_response

# ---- Optional: silence LibreSSL warning on older Python/urllib3
try:
    import urllib3
    from urllib3.exceptions import NotOpenSSLWarning
    warnings.simplefilter("ignore", NotOpenSSLWarning)
except Exception:
    pass

# =========================
# CONFIG from ENV
# =========================
ADMIN_HOST = os.getenv("ADMIN_HOST", "silver-rudradhan.myshopify.com").strip()
ADMIN_API_VERSION = os.getenv("ADMIN_API_VERSION", "2024-10").strip()
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "").strip()
if not ADMIN_TOKEN:
    raise RuntimeError("ADMIN_TOKEN env var is required")

PIXEL_SHARED_SECRET = os.getenv("PIXEL_SHARED_SECRET", "").strip()
if not PIXEL_SHARED_SECRET:
    raise RuntimeError("PIXEL_SHARED_SECRET env var is required")

# Multiple collection handles allowed, comma-separated
COLLECTION_HANDLES = [h.strip() for h in os.getenv("COLLECTION_HANDLES", "pearl-pendant-gold-plated").split(",") if h.strip()]
RUN_EVERY_MIN = int(os.getenv("RUN_EVERY_MIN", "0"))   # 0 = disabled
CHANGE_STATUS = os.getenv("CHANGE_STATUS", "0") == "1"
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
DEBUG_VERBOSE = os.getenv("DEBUG_VERBOSE", "1") == "1"
FORCE_WRITE_METAFIELDS = os.getenv("FORCE_WRITE_METAFIELDS", "1") == "1"

PER_PAGE = int(os.getenv("PER_PAGE", "50"))
MUTATION_SLEEP_SEC = float(os.getenv("MUTATION_SLEEP_SEC", "0.35"))

# Metafields
MF_NAMESPACE = "custom"
MF_BADGES_KEY = "badges"
MF_DELIVERY_KEY = "delivery_time"

# Exact allowed choice + values
BADGE_READY    = "Ready To Ship"
DELIVERY_READY = "2-5 Days Across India"
DELIVERY_MTO   = "12-15 Days Across India"

# If badges is a LIST (your store uses list.single_line_text_field), keep as is.
# If scalar, change to "single_line_text_field".
TYPE_OVERRIDE = {
    (MF_NAMESPACE, MF_BADGES_KEY): "list.single_line_text_field",
    # (MF_NAMESPACE, MF_DELIVERY_KEY): "single_line_text_field",  # usually scalar; leave unset to auto
}

LOG_CSV_PATH = "availability_sync_log.csv"
PORT = int(os.getenv("PORT", "5050"))

# =========================
# GraphQL Helpers
# =========================
GQL_ENDPOINT = f"https://{ADMIN_HOST}/admin/api/{ADMIN_API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": ADMIN_TOKEN,
    "Content-Type": "application/json",
}

def gql(query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {"query": query, "variables": variables or {}}
    r = requests.post(GQL_ENDPOINT, headers=HEADERS, json=payload, timeout=60)
    if r.status_code != 200:
        raise RuntimeError(f"GraphQL HTTP {r.status_code}: {r.text}")
    data = r.json()
    if "errors" in data and data["errors"]:
        raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'], indent=2)}")
    return data.get("data", {})

# =========================
# GraphQL Queries/Mutations
# =========================
QUERY_COLLECTION_PAGE = """
query ProductsInCollection($handle: String!, $cursor: String) {
  collectionByHandle(handle: $handle) {
    id
    title
    products(first: %(PER_PAGE)d, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes {
        id
        title
        status
        variants(first: 100) {
          nodes {
            id
            title
            inventoryQuantity
            inventoryPolicy
            inventoryItem { id tracked }
          }
        }
        badges: metafield(namespace: "%(NS)s", key: "%(BK)s") { id value type }
        dtime:  metafield(namespace: "%(NS)s", key: "%(DK)s") { id value type }
      }
    }
  }
}
""" % {
    "PER_PAGE": PER_PAGE,
    "NS": MF_NAMESPACE,
    "BK": MF_BADGES_KEY,
    "DK": MF_DELIVERY_KEY,
}

QUERY_PRODUCT_MF = """
query ProductMF($id: ID!) {
  product(id: $id) {
    id
    badges: metafield(namespace: "%(NS)s", key: "%(BK)s") { id value type }
    dtime:  metafield(namespace: "%(NS)s", key: "%(DK)s") { id value type }
  }
}
""".replace("%(NS)s", MF_NAMESPACE).replace("%(BK)s", MF_BADGES_KEY).replace("%(DK)s", MF_DELIVERY_KEY)

MUTATION_PRODUCT_UPDATE = """
mutation ProductUpdate($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status }
    userErrors { field message }
  }
}
"""

MUTATION_METAFIELDS_SET = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key value type }
    userErrors { field message }
  }
}
"""

MUTATION_METAFIELD_DELETE = """
mutation MetafieldDelete($input: MetafieldDeleteInput!) {
  metafieldDelete(input: $input) {
    deletedId
    userErrors { field message }
  }
}
"""

# =========================
# Utilities
# =========================
def now_iso():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _json_first_or_str(s: str) -> str:
    try:
        j = json.loads(s)
        if isinstance(j, list) and j:
            return str(j[0]).strip()
        if isinstance(j, dict) and "value" in j:
            return str(j["value"]).strip()
    except Exception:
        pass
    return s

def normalize_text_value(raw) -> str:
    if raw is None:
        return ""
    if isinstance(raw, str):
        return _json_first_or_str(raw.strip())
    try:
        return str(raw)
    except Exception:
        return ""

def canonical(s: str) -> str:
    return (s or "").strip().lower()

def ensure_log_header(path: str):
    need_header = not os.path.exists(path) or os.path.getsize(path) == 0
    if need_header:
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([
                "run_timestamp","dry_run","change_status","force_write_metafields","collection_handle",
                "product_id","product_title",
                "availability_before","status_before","badges_before","delivery_before",
                "status_after","badges_after","delivery_after",
                "applied_status_change","applied_metafields_change",
                "verified_badges","verified_delivery","verified_types",
                "mutation_errors",
                "debug_variant_details"
            ])

def append_log_row(path: str, row: Dict[str, Any]):
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            row.get("run_timestamp"),
            row.get("dry_run"),
            row.get("change_status"),
            row.get("force_write_metafields"),
            row.get("collection_handle"),
            row.get("product_id"),
            row.get("product_title"),
            row.get("availability_before"),
            row.get("status_before"),
            row.get("badges_before"),
            row.get("delivery_before"),
            row.get("status_after"),
            row.get("badges_after"),
            row.get("delivery_after"),
            row.get("applied_status_change"),
            row.get("applied_metafields_change"),
            row.get("verified_badges"),
            row.get("verified_delivery"),
            row.get("verified_types"),
            row.get("mutation_errors"),
            row.get("debug_variant_details"),
        ])

# =========================
# Availability Logic
# =========================
def compute_availability_from_variants(variants: List[Dict[str, Any]]) -> int:
    total = 0
    any_tracked = False
    for v in variants:
        tracked = bool((v.get("inventoryItem") or {}).get("tracked"))
        qty = int(v.get("inventoryQuantity") or 0)
        if tracked:
            any_tracked = True
            total += max(qty, 0)
    return total if any_tracked else 0

def desired_state_for_availability(total_avail: int) -> Tuple[str, str, str, str]:
    if total_avail > 0:
        return ("ACTIVE", BADGE_READY, DELIVERY_READY, "READY")
    else:
        return ("DRAFT", "", DELIVERY_MTO, "MTO")

# =========================
# Metafield Type & Formatting
# =========================
def resolve_mf_type(ns: str, key: str, node: Optional[Dict[str, Any]]) -> str:
    t = TYPE_OVERRIDE.get((ns, key))
    if t:
        return t
    if node and isinstance(node.get("type"), str) and node["type"]:
        return node["type"]
    return "single_line_text_field"

def format_value_for_type(desired_value: str, mf_type: str) -> str:
    if mf_type.startswith("list."):
        if not desired_value:
            return "[]"
        return json.dumps([desired_value])
    return desired_value or ""

# =========================
# Mutations & Verification
# =========================
def gql_update_product_status(product_id: str, target_status: str) -> bool:
    if DRY_RUN:
        print(f"[DRY] productUpdate {product_id} → status={target_status}", flush=True)
        return False
    variables = {"input": {"id": product_id, "status": target_status}}
    data = gql(MUTATION_PRODUCT_UPDATE, variables)
    ue = (data.get("productUpdate") or {}).get("userErrors") or []
    if ue:
        print(f"[WARN] productUpdate errors for {product_id}: {ue}", flush=True)
        return False
    time.sleep(MUTATION_SLEEP_SEC)
    return True

def gql_delete_metafield(metafield_id: Optional[str]) -> Tuple[bool, str]:
    if not metafield_id:
        return True, ""
    if DRY_RUN:
        print(f"[DRY] metafieldDelete {metafield_id}", flush=True)
        return False, ""
    data = gql(MUTATION_METAFIELD_DELETE, {"input": {"id": metafield_id}})
    ue = (data.get("metafieldDelete") or {}).get("userErrors") or []
    if ue:
        msg = f"metafieldDelete errors: {ue}"
        print(f"[WARN] {msg}", flush=True)
        return False, msg
    time.sleep(MUTATION_SLEEP_SEC)
    return True, ""

def _parse_choices_from_error(ue: List[Dict[str, Any]]) -> List[str]:
    for e in ue or []:
        msg = (e.get("message") or "")
        m = re.search(r'\[.*\]', msg)
        if m:
            try:
                choices = json.loads(m.group(0))
                if isinstance(choices, list):
                    return [str(c) for c in choices]
            except Exception:
                pass
    return []

def _closest_choice(target: str, choices: List[str]) -> Optional[str]:
    tl = (target or "").lower().strip()
    for c in choices:
        if (c or "").lower().strip() == tl:
            return c
    return choices[0] if choices else None

def _metafields_set_with_retry(mf_inputs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    data = gql(MUTATION_METAFIELDS_SET, {"metafields": mf_inputs})
    ue = (data.get("metafieldsSet") or {}).get("userErrors") or []
    if not ue:
        return []
    choices = _parse_choices_from_error(ue)
    if choices:
        for inp in mf_inputs:
            if inp.get("key") == MF_BADGES_KEY:
                current_type = inp.get("type", "")
                raw_val = inp.get("value", "")
                desired_scalar = ""
                if current_type.startswith("list."):
                    try:
                        arr = json.loads(raw_val) if isinstance(raw_val, str) else raw_val
                        desired_scalar = (arr[0] if isinstance(arr, list) and arr else "")
                    except Exception:
                        desired_scalar = ""
                else:
                    desired_scalar = raw_val
                corrected = _closest_choice(desired_scalar, choices)
                if corrected is not None:
                    if current_type.startswith("list."):
                        inp["value"] = json.dumps([corrected])
                    else:
                        inp["value"] = corrected
                    print(f"[INFO] Retrying badges with allowed choice: {corrected}", flush=True)
                    data2 = gql(MUTATION_METAFIELDS_SET, {"metafields": mf_inputs})
                    return (data2.get("metafieldsSet") or {}).get("userErrors") or []
    return ue

def set_product_metafields(product_id: str,
                           badges_value: str,
                           delivery_value: str,
                           badges_node: Optional[Dict[str, Any]],
                           dtime_node: Optional[Dict[str, Any]]) -> Tuple[bool, str, str, str, str]:
    mutation_errors = []

    badges_type = resolve_mf_type(MF_NAMESPACE, MF_BADGES_KEY, badges_node)
    delivery_type = resolve_mf_type(MF_NAMESPACE, MF_DELIVERY_KEY, dtime_node)

    # BADGES
    if not badges_value:
        if DRY_RUN:
            print(f"[DRY] metafieldDelete (badges) { (badges_node or {}).get('id') }", flush=True)
        else:
            ok, msg = gql_delete_metafield((badges_node or {}).get("id"))
            if not ok and msg:
                mutation_errors.append(msg)
        mf_inputs = []
    else:
        mf_inputs = []
        if badges_type.startswith("list."):
            mf_inputs.append({
                "ownerId": product_id,
                "namespace": MF_NAMESPACE,
                "key": MF_BADGES_KEY,
                "type": badges_type,
                "value": json.dumps([badges_value]),
            })
        else:
            mf_inputs.append({
                "ownerId": product_id,
                "namespace": MF_NAMESPACE,
                "key": MF_BADGES_KEY,
                "type": badges_type,
                "value": badges_value,
            })

    # DELIVERY
    delivery_payload_value = format_value_for_type(delivery_value, delivery_type)
    mf_inputs.append({
        "ownerId": product_id,
        "namespace": MF_NAMESPACE,
        "key": MF_DELIVERY_KEY,
        "type": delivery_type,
        "value": delivery_payload_value,
    })

    if mf_inputs:
        if DRY_RUN:
            print(f"[DRY] metafieldsSet {product_id} → " +
                  ", ".join([f"{x['key']}({x['type']})={x['value']}" for x in mf_inputs]), flush=True)
        else:
            ue = _metafields_set_with_retry(mf_inputs)
            if ue:
                msg = f"metafieldsSet errors: {ue}"
                print(f"[WARN] {msg}", flush=True)
                mutation_errors.append(msg)
            time.sleep(MUTATION_SLEEP_SEC)

    # Verify
    verify = gql(QUERY_PRODUCT_MF, {"id": product_id})
    p = (verify.get("product") or {})
    vb_node = (p.get("badges") or {})
    vd_node = (p.get("dtime") or {})
    vb = normalize_text_value(vb_node.get("value"))
    vd = normalize_text_value(vd_node.get("value"))
    vbt = vb_node.get("type")
    vdt = vd_node.get("type")
    verified_types = f"{vbt}|{vdt}"
    return (len(mutation_errors) == 0, vb, vd, verified_types, "; ".join(mutation_errors))

# =========================
# Core runner
# =========================
def process_collection(handle: str) -> Dict[str, Any]:
    ensure_log_header(LOG_CSV_PATH)
    run_ts = now_iso()
    cursor = None
    page = 0
    total_seen = 0
    changed = 0

    QUERY = QUERY_COLLECTION_PAGE  # already formatted with PER_PAGE & keys

    while True:
        page += 1
        if DEBUG_VERBOSE:
            print(f"\n[PAGE {page}] Fetching products from '{handle}'…", flush=True)
        data = gql(QUERY, {"handle": handle, "cursor": cursor})
        coll = data.get("collectionByHandle")
        if not coll:
            raise RuntimeError(f"No collection found for handle='{handle}'")

        products_edge = coll.get("products") or {}
        prods = products_edge.get("nodes", [])
        page_info = products_edge.get("pageInfo", {})

        if DEBUG_VERBOSE:
            print(f"[INFO] Collection: {coll.get('title','?')} | products in page: {len(prods)}", flush=True)

        for p in prods:
            total_seen += 1
            pid = p["id"]
            ptitle = p.get("title", "")
            status_before = p.get("status")
            variants = (p.get("variants") or {}).get("nodes", [])

            avail = compute_availability_from_variants(variants)
            badges_node = p.get("badges") or None
            dtime_node  = p.get("dtime")  or None
            badges_before = normalize_text_value((badges_node or {}).get("value"))
            delivery_before = normalize_text_value((dtime_node  or {}).get("value"))

            target_status, target_badge, target_delivery, target_mode = desired_state_for_availability(avail)
            current_ready_mode = ("ready" in (badges_before or "").lower()) and ("2-5" in (delivery_before or "").lower())
            desired_ready_mode = (target_mode == "READY")

            if DEBUG_VERBOSE:
                print(f"[CHECK] {ptitle} | avail={avail} | "
                      f"mode_now={'READY' if current_ready_mode else 'MTO'} -> {target_mode} | "
                      f"status={status_before} -> {target_status} | "
                      f"badges='{badges_before}'→'{target_badge}' | "
                      f"delivery='{delivery_before}'→'{target_delivery}'", flush=True)

            need_status   = (status_before != target_status)
            need_badge    = (canonical(badges_before)  != canonical(target_badge))
            need_delivery = (canonical(delivery_before)!= canonical(target_delivery))

            if FORCE_WRITE_METAFIELDS and (current_ready_mode != desired_ready_mode):
                need_badge = True
                need_delivery = True

            applied_status = False
            applied_metafields = False
            verified_badges = badges_before
            verified_delivery = delivery_before
            verified_types = f"{(badges_node or {}).get('type')}|{(dtime_node or {}).get('type')}"
            mutation_errors_str = ""

            if need_status or need_badge or need_delivery:
                changed += 1
                if need_status:
                    if CHANGE_STATUS:
                        applied_status = gql_update_product_status(pid, target_status)
                    else:
                        if DEBUG_VERBOSE:
                            print(f"[SKIP] change_status=False → not updating status for {pid}", flush=True)
                if need_badge or need_delivery:
                    ok, vb, vd, vtypes, merrs = set_product_metafields(
                        pid, target_badge, target_delivery, badges_node, dtime_node
                    )
                    applied_metafields = ok
                    verified_badges = vb or verified_badges
                    verified_delivery = vd or verified_delivery
                    verified_types = vtypes or verified_types
                    mutation_errors_str = merrs

            status_after   = (target_status if (CHANGE_STATUS and (applied_status or DRY_RUN) and need_status) else status_before)
            badges_after   = (target_badge if (applied_metafields or DRY_RUN) and (need_badge or need_delivery) else badges_before)
            delivery_after = (target_delivery if (applied_metafields or DRY_RUN) and (need_badge or need_delivery) else delivery_before)

            variant_debug_summary = "; ".join(
                [f"{(v.get('title') or '').strip()}|tracked={bool((v.get('inventoryItem') or {}).get('tracked'))}|qty={int(v.get('inventoryQuantity') or 0)}|policy={(v.get('inventoryPolicy') or '').strip()}"
                 for v in variants]
            )

            append_log_row(LOG_CSV_PATH, {
                "run_timestamp": run_ts,
                "dry_run": DRY_RUN,
                "change_status": CHANGE_STATUS,
                "force_write_metafields": FORCE_WRITE_METAFIELDS,
                "collection_handle": handle,
                "product_id": pid,
                "product_title": ptitle,
                "availability_before": avail,
                "status_before": status_before,
                "badges_before": badges_before,
                "delivery_before": delivery_before,
                "status_after": status_after,
                "badges_after": badges_after,
                "delivery_after": delivery_after,
                "applied_status_change": bool(applied_status),
                "applied_metafields_change": bool(applied_metafields),
                "verified_badges": verified_badges,
                "verified_delivery": verified_delivery,
                "verified_types": verified_types,
                "mutation_errors": mutation_errors_str,
                "debug_variant_details": variant_debug_summary,
            })

        if page_info.get("hasNextPage"):
            cursor = page_info.get("endCursor")
        else:
            break

    summary = {
        "run_timestamp": run_ts,
        "collection_handle": handle,
        "total_products_scanned": total_seen,
        "products_changed": changed,
        "dry_run": DRY_RUN,
        "change_status": CHANGE_STATUS,
        "force_write_metafields": FORCE_WRITE_METAFIELDS,
        "log_file": os.path.abspath(LOG_CSV_PATH),
    }
    print(f"\n[DONE] {summary}", flush=True)
    return summary

# =========================
# Flask app + endpoints
# =========================
app = Flask(__name__)
run_lock = threading.Lock()
is_running = False

def _cors(resp):
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp

@app.route("/health", methods=["GET"])
def health():
    return "ok", 200

@app.route("/availability/run", methods=["OPTIONS"])
def run_options():
    return _cors(make_response("", 204))

@app.route("/availability/run", methods=["GET", "POST"])
def run_now():
    # simple key check
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))

    handle = (request.args.get("handle") or request.form.get("handle") or "").strip()
    if not handle:
        # default to first configured handle
        handle = COLLECTION_HANDLES[0]

    # allow ad-hoc overrides via query
    global DRY_RUN, CHANGE_STATUS, FORCE_WRITE_METAFIELDS
    if "dry_run" in request.args:
        DRY_RUN = request.args.get("dry_run") in ("1", "true", "True")
    if "change_status" in request.args:
        CHANGE_STATUS = request.args.get("change_status") in ("1", "true", "True")
    if "force_write" in request.args:
        FORCE_WRITE_METAFIELDS = request.args.get("force_write") in ("1", "true", "True")

    # prevent concurrent runs
    global is_running
    with run_lock:
        if is_running:
            return _cors(make_response(("busy", 409)))
        is_running = True
    try:
        summary = process_collection(handle)
        return _cors(jsonify(summary)), 200
    except Exception as e:
        return _cors(make_response((f"error: {e}", 500)))
    finally:
        with run_lock:
            is_running = False

@app.route("/availability/run_all", methods=["GET", "POST"])
def run_all():
    key = (request.args.get("key") or request.form.get("key") or "").strip()
    if key != PIXEL_SHARED_SECRET:
        return _cors(make_response(("forbidden", 403)))

    handles = [h for h in COLLECTION_HANDLES if h]
    results = []
    global is_running
    with run_lock:
        if is_running:
            return _cors(make_response(("busy", 409)))
        is_running = True
    try:
        for h in handles:
            results.append(process_collection(h))
            time.sleep(0.5)
        return _cors(jsonify({"results": results})), 200
    except Exception as e:
        return _cors(make_response((f"error: {e}", 500)))
    finally:
        with run_lock:
            is_running = False

# =========================
# Optional scheduler
# =========================
def scheduler_loop():
    if RUN_EVERY_MIN <= 0:
        return
    print(f"[SCHED] Enabled. Will run every {RUN_EVERY_MIN} min for: {COLLECTION_HANDLES}", flush=True)
    while True:
        try:
            for h in COLLECTION_HANDLES:
                with run_lock:
                    if is_running:
                        print("[SCHED] Skipping run (another job is active).", flush=True)
                        break
                    # mark running to avoid overlap
                    globals()["is_running"] = True
                try:
                    process_collection(h)
                finally:
                    with run_lock:
                        globals()["is_running"] = False
                time.sleep(0.5)
        except Exception as e:
            print(f"[SCHED] Error: {e}", flush=True)
        # sleep the configured interval
        time.sleep(max(RUN_EVERY_MIN, 1) * 60)

# kick off scheduler (daemon)
threading.Thread(target=scheduler_loop, daemon=True).start()

if __name__ == "__main__":
    print(f"[BOOT] Availability service on {ADMIN_HOST} | API {ADMIN_API_VERSION}", flush=True)
    print(f"[CFG] DRY_RUN={DRY_RUN} CHANGE_STATUS={CHANGE_STATUS} FORCE_WRITE_METAFIELDS={FORCE_WRITE_METAFIELDS} DEBUG_VERBOSE={DEBUG_VERBOSE}", flush=True)
    from werkzeug.serving import run_simple
    run_simple("0.0.0.0", PORT, app)
