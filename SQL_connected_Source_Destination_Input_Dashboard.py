#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Interactive Dashboard — TOP input map + BOTTOM animation map

New in this version
-------------------
• A “Live Calculations” table is added **under the mini map** (left column).
• As the animation reaches each stop, one row is appended with:
  Current Weight, LVP, Convert to Tonne, Consumption (1.92),
  Consumption Car × Km, SFD, TCEs.
• Rows scroll inside the card; header stays visible.

Notes
-----
- “Current Weight” uses the cumulative (Load − Unload) up to that stop, which
  matches =SUM(Loads up to row) − SUM(Unloads up to row).
- Distance is the leg’s distance (km). If Google’s Distance Matrix value is
  unavailable, it is derived from the drawn route polyline.
"""

import json
import math
import sqlite3
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone
from typing import List, Tuple, Optional

import os
import googlemaps
import mysql.connector
import pandas as pd

# =======================
# CONFIG
# =======================
API_KEY = "AIzaSyATHWWCm1SCMNMtQx-uEr6IaBr3YOF8eKg"
if not API_KEY:
    raise RuntimeError("Set GOOGLE_MAPS_API_KEY")

DB_HOST = "localhost"
DB_USER = "root"
DB_PASSWORD = "Aurnil_Anshika_2025"
DB_NAME = "thesis_university"

# MySQL table / columns
TABLE          = "SavingsRoute"
COL_ID         = "id"
COL_LOOP       = "loop"
COL_ROUTE      = "route"
COL_SRC        = "Source"
COL_SRC_LAT    = "Source_Lat"
COL_SRC_LNG    = "Source_Lng"
COL_DST        = "Destination"
COL_DST_LAT    = "Destination_Lat"
COL_DST_LNG    = "Destination_Lng"
COL_DIST_KM    = "Distance_km"
COL_DUR_MIN    = "Duration_min"
COL_LVAL       = "Load_Value"
COL_UVAL       = "Unload_Value"
COL_CWEIGHT    = "Current_Weight"
COL_TS         = "created_at"

# thresholds
EPS_KM   = 0.05
EPS_MIN  = 0.5
EPS_COORD = 1e-5

# HTTP
HOST = "127.0.0.1"
PORT = 8765

# =======================
# Geocode cache (sqlite)
# =======================
CACHE_DB = "geocode_cache.db"
_conn_cache = sqlite3.connect(CACHE_DB, check_same_thread=False)
_conn_cache.execute("""
CREATE TABLE IF NOT EXISTS cache(
  addr TEXT PRIMARY KEY,
  lat REAL, lng REAL, formatted TEXT, raw_json TEXT
)""")
_conn_cache.execute("CREATE INDEX IF NOT EXISTS idx_cache_addr ON cache(addr)")
_conn_cache.commit()

def cache_get(addr: str):
    cur = _conn_cache.execute("SELECT lat,lng,formatted FROM cache WHERE addr=?", (addr.strip(),))
    row = cur.fetchone()
    return (row[0], row[1], row[2]) if row else None

def cache_put(addr: str, lat: float, lng: float, formatted: str, raw: dict):
    _conn_cache.execute(
        "INSERT OR REPLACE INTO cache(addr,lat,lng,formatted,raw_json) VALUES(?,?,?,?,?)",
        (addr.strip(), lat, lng, formatted, json.dumps(raw, ensure_ascii=False))
    )
    _conn_cache.commit()

gmaps_client = googlemaps.Client(key=API_KEY)

def batched_distance_matrix(origins, destinations, mode="driving", retry=3, pause=0.25):
    """origins/destinations are [(lat,lng), ...]"""
    import time
    O, D = len(origins), len(destinations)
    dist = [[math.inf]*D for _ in range(O)]
    dur  = [[math.inf]*D for _ in range(O)]
    OB, DB = 10, 10
    for oi in range(0, O, OB):
        for dj in range(0, D, DB):
            o_slice = origins[oi:oi+OB]; d_slice = destinations[dj:dj+DB]
            last_err = None
            for attempt in range(1, retry+1):
                try:
                    resp = gmaps_client.distance_matrix(origins=o_slice, destinations=d_slice, mode=mode)
                    for i_row, row in enumerate(resp.get("rows", [])):
                        for j_el, el in enumerate(row.get("elements", [])):
                            if el.get("status") == "OK":
                                dist_km = el["distance"]["value"] / 1000.0
                                dur_min = el["duration"]["value"] / 60.0
                                dist[oi + i_row][dj + j_el] = dist_km
                                dur[oi + i_row][dj + j_el]  = dur_min
                    break
                except Exception as e:
                    last_err = e; time.sleep(pause * attempt)
            if last_err:
                print(f"[WARN] Distance Matrix batch failed (oi={oi}, dj={dj}): {last_err}")
            time.sleep(0.05)
    return dist, dur

# =======================
# MySQL helpers
# =======================
def mysql_conn():
    return mysql.connector.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, autocommit=False
    )

def ensure_table_and_columns(cur):
    # Create the table if it doesn't exist (with all columns)
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS `{TABLE}` (
            `{COL_ID}` INT AUTO_INCREMENT PRIMARY KEY,
            `{COL_LOOP}`  VARCHAR(64) NULL,
            `{COL_ROUTE}` VARCHAR(64) NULL,
            `{COL_SRC}`   VARCHAR(255) NULL,
            `{COL_SRC_LAT}` DOUBLE NULL,
            `{COL_SRC_LNG}` DOUBLE NULL,
            `{COL_DST}`   VARCHAR(255) NULL,
            `{COL_DST_LAT}` DOUBLE NULL,
            `{COL_DST_LNG}` DOUBLE NULL,
            `{COL_DIST_KM}` DOUBLE NULL,
            `{COL_DUR_MIN}` DOUBLE NULL,
            `{COL_LVAL}`  DOUBLE NULL,
            `{COL_UVAL}`  DOUBLE NULL,
            `{COL_CWEIGHT}` DOUBLE NULL,
            `{COL_TS}`    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """)

    # What columns are there now?
    cur.execute(f"SHOW COLUMNS FROM `{TABLE}`")
    existing = {row[0] for row in cur.fetchall()}

    # Safe column adder: try placing AFTER X, otherwise just append
    def add_after(colname, def_sql, after):
        if colname not in existing:
            try:
                cur.execute(f"ALTER TABLE `{TABLE}` ADD COLUMN {def_sql} AFTER `{after}`")
            except mysql.connector.Error:
                cur.execute(f"ALTER TABLE `{TABLE}` ADD COLUMN {def_sql}")
            existing.add(colname)

    # Ensure key columns exist on legacy tables too
    add_after(COL_LOOP,   f"`{COL_LOOP}` VARCHAR(64) NULL",           COL_ID)
    add_after(COL_ROUTE,  f"`{COL_ROUTE}` VARCHAR(64) NULL",          COL_LOOP)

    # Existing backfills (now safe even if the 'after' column is missing)
    add_after(COL_SRC,       f"`{COL_SRC}` VARCHAR(255) NULL",      COL_ROUTE)
    add_after(COL_SRC_LAT,   f"`{COL_SRC_LAT}` DOUBLE NULL",        COL_SRC)
    add_after(COL_SRC_LNG,   f"`{COL_SRC_LNG}` DOUBLE NULL",        COL_SRC_LAT)
    add_after(COL_DST,       f"`{COL_DST}` VARCHAR(255) NULL",      COL_SRC_LNG)
    add_after(COL_DST_LAT,   f"`{COL_DST_LAT}` DOUBLE NULL",        COL_DST)
    add_after(COL_DST_LNG,   f"`{COL_DST_LNG}` DOUBLE NULL",        COL_DST_LAT)
    add_after(COL_DIST_KM,   f"`{COL_DIST_KM}` DOUBLE NULL",        COL_DST_LNG)
    add_after(COL_DUR_MIN,   f"`{COL_DUR_MIN}` DOUBLE NULL",        COL_DIST_KM)
    add_after(COL_LVAL,      f"`{COL_LVAL}` DOUBLE NULL",           COL_DUR_MIN)
    add_after(COL_UVAL,      f"`{COL_UVAL}` DOUBLE NULL",           COL_LVAL)
    add_after(COL_CWEIGHT,   f"`{COL_CWEIGHT}` DOUBLE NULL",        COL_UVAL)
    add_after(COL_TS,        f"`{COL_TS}` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP", COL_CWEIGHT)

    # Helpful indexes (ignore if already exist)
    try:
        cur.execute(f"CREATE INDEX idx_{TABLE}_loop_ts ON `{TABLE}` (`{COL_LOOP}`, `{COL_TS}`)")
    except mysql.connector.Error:
        pass
    try:
        cur.execute(f"CREATE INDEX idx_{TABLE}_ts ON `{TABLE}` (`{COL_TS}`)")
    except mysql.connector.Error:
        pass


def insert_leg(cur,
               loop_val, route_val,
               src_addr, src_lat, src_lng,
               dst_addr, dst_lat, dst_lng,
               dist_km, dur_min,
               load_val, unload_val,
               run_ts: str):
    cur.execute(
        f"""
        INSERT INTO `{TABLE}` (
            `{COL_LOOP}`, `{COL_ROUTE}`,
            `{COL_SRC}`, `{COL_SRC_LAT}`, `{COL_SRC_LNG}`,
            `{COL_DST}`, `{COL_DST_LAT}`, `{COL_DST_LNG}`,
            `{COL_DIST_KM}`, `{COL_DUR_MIN}`,
            `{COL_LVAL}`, `{COL_UVAL}`, `{COL_TS}`
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
        (
            loop_val, route_val,
            src_addr, src_lat, src_lng,
            dst_addr, dst_lat, dst_lng,
            None if math.isinf(dist_km) else dist_km,
            None if math.isinf(dur_min) else dur_min,
            float(load_val) if load_val is not None else None,
            float(unload_val) if unload_val is not None else None,
            run_ts,
        ),
    )

def recompute_current_weight_global(cur):
    sql = f"""
        UPDATE `{TABLE}` t
        JOIN (
            SELECT
                `{COL_ID}` AS rid,
                SUM(COALESCE(`{COL_LVAL}`,0) - COALESCE(`{COL_UVAL}`,0))
                    OVER (ORDER BY `{COL_TS}`, `{COL_ID}`) AS cw
            FROM `{TABLE}`
        ) s ON s.rid = t.`{COL_ID}`
        SET t.`{COL_CWEIGHT}` = s.cw;
    """
    cur.execute(sql)

# =======================
# Routing helpers
# =======================
def _same_place(a_lat, a_lng, b_lat, b_lng, eps=EPS_COORD):
    return abs(a_lat-b_lat) < eps and abs(a_lng-b_lng) < eps

def nearest_from(start_idx: int, candidates: List[int], dist: List[List[float]]) -> Optional[int]:
    best, bestd = None, math.inf
    for j in candidates:
        d = dist[start_idx][j]
        if d < bestd:
            best, bestd = j, d
    return best

# =======================
# planner (keeps priorities; visits **every** leg once; never negative weight)
# =======================
def solve_phased_route(src, dests):
    """
    src: {'name','lat','lng'}
    dests: [{'name','lat','lng','load','unload'} ...]
      • de-dup same place (merge loads/unloads)
      • priority = LOAD > BOTH > UNLOAD
      • visit a stop only when its unload is feasible (otherwise unload is capped)
      • current weight never negative
      • visit each destination exactly once
    """
    def same_place(d1, d2, eps=EPS_COORD):
        return abs(d1['lat']-d2['lat']) < eps and abs(d1['lng']-d2['lng']) < eps

    # 1) merge duplicates
    merged = []
    def norm_name(s): return (s or "").strip().lower()
    for d in dests:
        d = {
            "name": (d.get("name") or "").strip(),
            "lat": float(d["lat"]), "lng": float(d["lng"]),
            "load": float(d.get("load") or 0.0),
            "unload": float(d.get("unload") or 0.0),
        }
        hit = None
        for m in merged:
            if same_place(d, m) or (d["name"] and norm_name(d["name"]) == norm_name(m["name"])):
                hit = m; break
        if hit:
            hit["load"]   += d["load"]
            hit["unload"] += d["unload"]
        else:
            merged.append(d)
    dests = merged

    # 2) fold any stop at source into the return payload
    ret_load = 0.0; ret_unload = 0.0
    filtered = []
    for d in dests:
        if same_place(d, {"lat":src["lat"], "lng":src["lng"]}):
            ret_load   += d["load"]
            ret_unload += d["unload"]
        else:
            filtered.append(d)
    dests = filtered

    # 3) matrices (0 = source, 1..N = D1..DN)
    points = [(src['lat'], src['lng'])] + [(d['lat'], d['lng']) for d in dests]
    full_dist, full_dur = batched_distance_matrix(points, points)

    # 4) greedy scheduler with feasibility + priority
    N = len(dests)
    idx_to_stop = {i+1: dests[i] for i in range(N)}  # 1..N
    remaining = set(range(1, N+1))
    route_global = [0]
    phase_legs   = []
    curr = 0
    onboard = 0.0

    def kind(i):
        di = idx_to_stop[i]
        if di["load"] > 0 and di["unload"] == 0: return "LOAD"
        if di["load"] > 0 and di["unload"] > 0:  return "BOTH"
        if di["load"] == 0 and di["unload"] > 0: return "UNLOAD"
        return "PASS"

    def nearest(start_idx, set_indices):
        best, bestd = None, math.inf
        for j in set_indices:
            d = full_dist[start_idx][j]
            if d < bestd:
                best, bestd = j, d
        return best

    while remaining:
        loads   = [i for i in remaining if kind(i) == "LOAD"]
        boths   = [i for i in remaining if kind(i) == "BOTH"
                   and (onboard + idx_to_stop[i]["load"] >= idx_to_stop[i]["unload"])]
        unloads = [i for i in remaining if kind(i) == "UNLOAD"
                   and (onboard >= idx_to_stop[i]["unload"])]

        pick = None
        if loads:
            pick = nearest(curr, loads)                      # 1) LOAD
        elif boths:
            pick = nearest(curr, boths)                      # 2) BOTH
        elif unloads:
            pick = nearest(curr, unloads)                    # 3) UNLOAD (feasible)
        else:
            # still pick something to ensure EVERY LEG is visited;
            # if nothing is feasible, pick the nearest remaining (unload will be capped)
            pick = nearest(curr, list(remaining))

        phase_legs.append((kind(pick), curr, pick))
        route_global.append(pick)
        di = idx_to_stop[pick]

        # apply load/unload with safety clamps (never negative)
        if di["load"] > 0:
            onboard += di["load"]
        if di["unload"] > 0:
            feasible_unload = min(di["unload"], onboard)
            di["unload"] = feasible_unload
            onboard -= feasible_unload

        remaining.remove(pick)
        curr = pick

    # return to source
    if route_global[-1] != 0:
        phase_legs.append(("RETURN", route_global[-1], 0))
        route_global.append(0)

    # coalesce duplicate consecutive legs (defensive)
    coalesced = []
    for leg in phase_legs:
        if not coalesced or (coalesced[-1][1], coalesced[-1][2]) != (leg[1], leg[2]):
            coalesced.append(leg)
    phase_legs = coalesced

    return {
        "phase_legs": phase_legs,
        "route_global": route_global,
        "full_dist": full_dist,
        "full_dur": full_dur,
        "return_payload": (ret_load, ret_unload),
        "dests": dests,
        "src": src
    }

def legs_table_for_preview(payload):
    """Build table rows + totals + order string for preview."""
    phase_legs = payload["phase_legs"]
    full_dist  = payload["full_dist"]
    full_dur   = payload["full_dur"]

    def label(idx): return "S" if idx == 0 else f"D{idx}"

    rows = []
    total_dist = 0.0
    total_time = 0.0
    order = ["S"]
    prev = 0
    for tag, a, b in phase_legs:
        dk = full_dist[a][b]
        dm = full_dur[a][b]
        total_dist += 0 if math.isinf(dk) else dk
        total_time += 0 if math.isinf(dm) else dm
        rows.append({
            "phase": tag,
            "leg": f"{label(a)} → {label(b)}",
            "dist": "∞ km" if math.isinf(dk) else f"{dk:.2f} km",
            "dur":  "∞ min" if math.isinf(dm) else f"{dm:.1f} min"
        })
        if a == prev and b != prev:
            order.append(label(b))
            prev = b
    order_str = " → ".join(order)
    return rows, total_dist, total_time, order_str

def save_run_to_mysql(payload, loop_id: str):
    """Insert legs into DB using ONE loop_id; return arrays for animation."""
    phase_legs = payload["phase_legs"]
    full_dist  = payload["full_dist"]
    full_dur   = payload["full_dur"]
    dests      = payload["dests"]
    src        = payload["src"]
    ret_load, ret_unload = payload["return_payload"]

    idx_to_stop = {i+1: dests[i] for i in range(len(dests))}
    run_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    conn = mysql_conn(); cur = conn.cursor()
    try:
        ensure_table_and_columns(cur); conn.commit()
        route_counter = 1
        pend_return_load   = ret_load
        pend_return_unload = ret_unload

        for tag, a, b in phase_legs:
            dk = full_dist[a][b]; dm = full_dur[a][b]
            # Do NOT skip near-zero-distance legs → every leg must be saved.
            if (a == b):
                continue

            if a == 0:
                s_addr, s_lat, s_lng = src["name"], src["lat"], src["lng"]
            else:
                sa = idx_to_stop[a]; s_addr, s_lat, s_lng = sa["name"], sa["lat"], sa["lng"]

            if b == 0:
                d_addr, d_lat, d_lng = src["name"], src["lat"], src["lng"]
                load_val, unload_val = float(pend_return_load), float(pend_return_unload)
                pend_return_load = pend_return_unload = 0.0
            else:
                sb = idx_to_stop[b]
                d_addr, d_lat, d_lng = sb["name"], sb["lat"], sb["lng"]
                load_val, unload_val = float(sb.get("load") or 0.0), float(sb.get("unload") or 0.0)

            insert_leg(cur, loop_id, str(route_counter),
                       s_addr, s_lat, s_lng,
                       d_addr, d_lat, d_lng,
                       dk, dm,
                       load_val, unload_val,
                       run_ts)
            route_counter += 1

        recompute_current_weight_global(cur)
        conn.commit()
    except mysql.connector.Error:
        conn.rollback()
        raise
    finally:
        try: cur.close(); conn.close()
        except: pass

    # ---------- Build animation arrays (exact leg order) ----------
    def stop_info(idx):
        if idx == 0:
            return {"name": src["name"], "lat": src["lat"], "lng": src["lng"]}
        d = dests[idx - 1]
        return {"name": d["name"], "lat": d["lat"], "lng": d["lng"]}

    stops = [stop_info(0)]
    for _, a, b in payload["phase_legs"]:
        stops.append(stop_info(b))

    # ==== LIVE CALC ARRAYS (CW never negative; ends at 0 at source) ====
    segDistKm, segDurMin, segLoadKg, segUnloadKg, cw_before, cw_after = [], [], [], [], [], []
    cumsum = 0.0
    for tag, a, b in payload["phase_legs"]:
        segDistKm.append(float(payload["full_dist"][a][b]) if not math.isinf(payload["full_dist"][a][b]) else None)
        segDurMin.append(float(payload["full_dur"][a][b]) if not math.isinf(payload["full_dur"][a][b]) else None)

        if b == 0:
            load = 0.0
            unload = cumsum  # drop all remaining at source
        else:
            info = dests[b-1]
            load = float(info.get("load") or 0.0)
            unload = float(info.get("unload") or 0.0)
            if unload > cumsum + load:
                unload = cumsum + load

        segLoadKg.append(load)
        segUnloadKg.append(unload)

        cw_before.append(cumsum)
        cumsum = max(0.0, cumsum + load - unload)
        if b == 0:
            cumsum = 0.0
        cw_after.append(cumsum)
    # ===================================================================

    return {
        "run_ts": run_ts,
        "stops": stops,
        "segDistKm": segDistKm,
        "segDurMin": segDurMin,
        "segLoadKg": segLoadKg,
        "segUnloadKg": segUnloadKg,
        "cwBefore": cw_before,
        "cwAfter": cw_after
    }

# ---------- DB → animation helpers for loop selector ----------
def latest_run_ts_for_loop(loop_id: str) -> Optional[str]:
    conn = mysql_conn(); cur = conn.cursor()
    try:
        cur.execute(f"SELECT MAX(`{COL_TS}`) FROM `{TABLE}` WHERE `{COL_LOOP}`=%s", (loop_id,))
        row = cur.fetchone()
        return row[0].strftime("%Y-%m-%d %H:%M:%S") if row and row[0] else None
    finally:
        try: cur.close(); conn.close()
        except: pass

def anim_arrays_for_loop(loop_id: str):
    run_ts = latest_run_ts_for_loop(loop_id)
    if not run_ts:
        return None

    conn = mysql_conn(); cur = conn.cursor()
    try:
        cur.execute(f"""
            SELECT `{COL_SRC}`, `{COL_SRC_LAT}`, `{COL_SRC_LNG}`,
                   `{COL_DST}`, `{COL_DST_LAT}`, `{COL_DST_LNG}`,
                   `{COL_DIST_KM}`, `{COL_DUR_MIN}`,
                   `{COL_LVAL}`, `{COL_UVAL}`
            FROM `{TABLE}`
            WHERE `{COL_LOOP}`=%s AND `{COL_TS}`=%s
            ORDER BY `{COL_ID}`
        """, (loop_id, run_ts))
        rows = cur.fetchall()
    finally:
        try: cur.close(); conn.close()
        except: pass

    if not rows:
        return None

    src_name, src_lat, src_lng = rows[0][0], float(rows[0][1]), float(rows[0][2])
    stops = [{"name": src_name, "lat": src_lat, "lng": src_lng}]

    segDistKm, segDurMin, segLoadKg, segUnloadKg = [], [], [], []
    for r in rows:
        dst_name, dst_lat, dst_lng = r[3], float(r[4]), float(r[5])
        stops.append({"name": dst_name, "lat": dst_lat, "lng": dst_lng})
        segDistKm.append(None if r[6] is None else float(r[6]))
        segDurMin.append(None if r[7] is None else float(r[7]))  # keep duration
        segLoadKg.append(0.0 if r[8] is None else float(r[8]))
        segUnloadKg.append(0.0 if r[9] is None else float(r[9]))

    cw_before, cw_after = [], []
    c = 0.0
    for i in range(len(segLoadKg)):
        cw_before.append(c)
        c = max(0.0, c + (segLoadKg[i] - segUnloadKg[i]))
        # zero at final (back at source)
        if i == len(segLoadKg) - 1 and stops[-1]["name"] == src_name:
            c = 0.0
        cw_after.append(c)

    return {
        "run_ts": run_ts,
        "stops": stops,
        "segDistKm": segDistKm,
        "segDurMin": segDurMin,   # returned for the UI to use
        "segLoadKg": segLoadKg,
        "segUnloadKg": segUnloadKg,
        "cwBefore": cw_before,
        "cwAfter": cw_after
    }

HTML_PAGE = """<!doctype html>
<html>
<head>
<meta charset="utf-8"/>
<title>Interactive Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<style>
  :root{
    --gaugeW: 330px; 
    --bg:#f6f7fb; --text:#1f2937; --muted:#6b7280;
    --card:#fff; --brd:#e9edf3; --shadow:0 4px 16px rgba(15,23,42,.08);
    --mini:560px; --map-h:500px; --radius:08px;
  }
  *{box-sizing:border-box}
  html,body{margin:0;padding:0;background:var(--bg);color:var(--text);font-family:Inter,system-ui,-apple-system,Segoe UI,Roboto,Arial,sans-serif}
  .dashboard{min-height:100vh;display:flex;flex-direction:column}
  .header{width:100vw}
  .title-bar{display:flex;align-items:center;justify-content:flex-end;padding:12px 18px}
  .title{font-weight:800;font-size:24px}
  .panel{width:100vw;display:grid;grid-template-columns:var(--mini) 1fr;gap:12px;padding:12px 0;border-top:1px solid var(--brd)}
  .left-col{display:flex;flex-direction:column;gap:10px;margin-left:0}
  .mini-wrap{height:var(--mini);width:var(--mini);overflow:hidden;border:1px solid var(--brd);border-radius:12px;box-shadow:var(--shadow)}
  #finland{height:100%;width:100%}
  .metrics{width:var(--mini);border:1px solid var(--brd);border-radius:12px;box-shadow:var(--shadow);overflow:hidden}
  .metrics .card-header{padding:8px 12px;border-bottom:1px solid var(--brd);font-weight:600;background:#fff;display:flex;align-items:center;justify-content:space-between;gap:8px}
  .metrics-table{max-height:none;overflow:auto;overflow-y:visible;background:#fff}
  .metrics table{width:100%;border-collapse:collapse;font-size:12.5px}
  .metrics th,.metrics td{padding:6px 8px;border-bottom:1px solid var(--brd);text-align:right;white-space:nowrap}
  .metrics th{position:sticky;top:0;background:#fff;z-index:1}
  .controls{padding:12px 18px;background:#fff;border:0px solid var(--brd);border-radius:12px;box-shadow:var(--shadow);margin-right:18px;position: relative;}
  .controls h3{margin:0 0 8px 0}
  .row{display:flex;gap:8px;margin:6px 0;flex-wrap:wrap}
  input[type=text],input[type=number],select{padding:8px 10px;border:1px solid var(--brd);border-radius:8px;min-width:160px}
  button{padding:8px 12px;border:1px solid var(--brd);background:#fff;border-radius:999px;cursor:pointer;font-weight:700}
  button.icon{padding:8px 10px}
  button:disabled{opacity:.6;cursor:not-allowed}
  .dest-list{margin:8px 0;max-height:152px;overflow:auto;border:1px dashed var(--brd);border-radius:8px;padding:8px}
  .dest-item{display:flex;justify-content:space-between;align-items:center;margin:4px 0}
  .rm{border:1px solid #fee2e2;color:#ef4444;background:#fff;border-radius:10px;padding:4px 10px}
  .table{margin-top:10px;border:1px solid var(--brd);border-radius:8px;overflow:hidden;background:#fff}
  .table-scroll{max-height:220px;overflow:auto}
  .table table{width:100%;border-collapse:collapse;font-size:14px}
  .table th,.table td{padding:8px;border-bottom:1px solid var(--brd);text-align:left}
  .table thead th{position:sticky;top:0;background:#fff;z-index:1}
  .table .totals{padding:8px;border-top:1px solid var(--brd);background:#fff}
  .card{background:var(--card);border:1px solid var(--brd);border-radius:var(--radius);box-shadow:var(--shadow);overflow:hidden}
  .card-header{padding:12px 16px;border-bottom:1px solid var(--brd);font-weight:600}
  .map-wrap{position:relative}
  #route{height:var(--map-h);width:100%}
  .legend{position:absolute;top:10px;left:10px;background:rgba(255,255,255,.95);padding:10px;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.12);font-size:13px}
  .play{position:absolute;bottom:14px;right:14px;background:#fff;border:1px solid var(--brd);padding:8px 14px;border-radius:999px;box-shadow:var(--shadow);cursor:pointer;font-weight:700}

  /* === QR Scanner overlay + HUD === */
  .qr-overlay{position:fixed;inset:0;background:rgba(0,0,0,.55);display:none;align-items:center;justify-content:center;z-index:9999}
  .qr-modal{position:relative;background:#000;border-radius:12px;box-shadow:var(--shadow);max-width:820px;width:92vw;aspect-ratio:16/9;overflow:hidden}
  .qr-video{width:100%;height:100%;object-fit:cover;display:block}
  .qr-paint{position:absolute;inset:0;width:100%;height:100%;pointer-events:none}
  .qr-bar{position:absolute;left:10px;right:10px;bottom:10px;display:flex;align-items:center;justify-content:space-between;padding:8px 10px;background:rgba(255,255,255,.96);border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.18)}
  .qr-msg{display:inline-flex;align-items:center;gap:8px;font-size:14px;color:#374151;font-weight:600}
  .qr-led{width:12px;height:12px;border-radius:50%;}
  .qr-ctl{display:inline-flex;align-items:center;gap:6px;color:#374151}
  .qr-ctl input[type=range]{width:140px}
  /* --- Average Speed gauge (square, top-right of controls) --- */
.gauge-card{
  position: static;              /* keep it in the flow next to the table */
  width: var(--gaugeW);
  aspect-ratio: 1 / 1;
  background: transparent;       /* no white box */
  border: 0;                     /* ⬅️ removes the border */
  box-shadow: none;              /* no shadow */
  pointer-events: auto;
}
.gauge-card canvas{ width:100%; height:100%; display:block; }

/* NEW: wrap the TRU table and gauge side-by-side */
.tru-row{
  display:flex;
  gap:12px;
  align-items:flex-start;
}
.tru-row .table{ flex:1 1 auto; }     /* TRU table grows */
.gauge-inline{ flex:0 0 var(--gaugeW);}/* gauge fixed width on the right */


</style>
</head>
<body>
<div class="dashboard">

  <div class="header">
    <div class="title-bar"><div class="title">Interactive Dashboard</div></div>
  </div>

  <!-- Builder row: LEFT = (mini map + metrics), RIGHT = controls -->
  <div class="panel">
    <div class="left-col">
      <div class="mini-wrap"><div id="finland"></div></div>

      <!-- Live calculations card -->
      <div class="metrics card">
        <div class="card-header">
          <span>Live Calculations</span>
          <button id="dlExcel" class="icon" disabled title="Download Excel">⬇︎ Download</button>
        </div>
        <div class="metrics-table">
          <table id="calcTable">
            <thead>
              <tr>
                <th>Loading_Location</th>
                <th>Unloading_Location</th>
                <th>Load&nbsp;(kg)</th>
                <th>Unload&nbsp;(kg)</th>
                <th>Current&nbsp;Weight</th>
                <th>LVP</th>
                <th>SFD&nbsp;(km)</th>
                <th>Time&nbsp;(minutes)</th>
                <th>Hours</th>
                <th>Average&nbsp;Speed</th>
                <th>Class</th>
                <th>Liter/Hour&nbsp;(a)</th>
                <th>L/100km&nbsp;(b)</th>
                <th>TRU_L&nbsp;(L)</th>
                <th>LPH&nbsp;(L/h)</th>
                <th title="TRU_L (L) × Emission Factor (WTW per liter)">Cooling Machine's CO2e_Fuel (kg)</th>
                <th title="SFD (km) divided by total SFD (km) of the run">Per TCE Value Identification-SFD</th>
                <th title="(Per TCE Value Identification) × (KG Refrigerant/Year ÷ #Journeys)">Default Leakage shared each SFD wise</th>
                <th title="CO2e_fuel (kg) + Default Leakage shared each SFD wise">Consumption1</th>
                <th title="((SFD×32)/100) × EF for Converting LBG">Consumption2</th>
                <th title="Consumption1 + Consumption2">TCE WTW emissions (kgCO₂e)</th>
                <th title="(Unload (kg)/1000) × SFD (km)">Notional_Activity</th>
                <th title="Per-row Notional_Activity / SUM(Notional_Activity) across rows">Allocation_%</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="controls">
      <h3>Build your run</h3>
      <div class="row">
        <button id="pickSource">Select Source</button>
        <button id="scanSourceQR" class="icon" title="Scan QR for Source">📷</button>
        <input id="srcName" type="text" placeholder="Source Name (paste/typing allowed)"/>
        <input id="loopId" type="text" placeholder="Loop ID (e.g., 1)" style="width:120px"/>
      </div>
      <!-- TRU inputs -->
      <div class="row">
        <input id="truCharge" type="number" step="0.001" placeholder="Per TRU Charge (kg)"/>
        <input id="leakYear"  type="number" step="0.001" placeholder="Default Leakage per Year (fraction)"/>
        <input id="kgRefYear" type="text" placeholder="KG Refrigerant/Year" readonly
         title="Per TRU Charge × Default Leakage per Year"/>

          <!-- NEW: Density, GHG Emission, and computed Emission Factor (EF) -->
        <input id="densityKgL"    type="number" step="0.001" placeholder="Density kg/L"/>
        <input id="ghgEmission"   type="number" step="0.001" placeholder="GHG Emission"/>
        <input id="emissionFactor" type="text"  placeholder="Emission Factor (EF)" readonly
         title="Density kg/L × GHG Emission"/>
      </div>

      <div class="row">
        <input id="loadVal" type="number" step="0.01" placeholder="Load Value (kg)"/>
        <input id="unloadVal" type="number" step="0.01" placeholder="Unload Value (kg)"/>
        <input id="efLbg" type="number" step="0.0001" placeholder="EF for Converting LBG" title="Used in Consumption2 = ((SFD×32)/100) × EF"/>
        <input id="capacity" type="number" step="0.0001" placeholder="Capacity" title="Optional capacity value"/>
      </div>

      <div class="row">
        <input id="destText" type="text" placeholder="Destination Address (paste/typing)"/>
        <button id="scanDestQR" class="icon" title="Scan QR for Destination">📷</button>
        <button id="addDestText" disabled>Add by text</button>
        <button id="pickDest" disabled>Select on Map</button>
      </div>

      <div class="dest-list" id="destList" style="display:none"></div>

      <div class="row">
        <button id="endBtn" disabled>End (preview shortest path)</button>
        <button id="okBtn" disabled>OK (save & animate)</button>
      </div>

      <div class="table" id="previewTable" style="display:none"></div>

      <div class="row" style="margin-top:10px">
        <label for="loopSelect" style="font-weight:600">Saved loops:</label>
        <select id="loopSelect"><option value="">— select —</option></select>
        <button id="playSelected" disabled>Play selected loop</button>
      </div>
      <!-- Decision "Class" (computed per leg using km=SFD and hr=K2) -->
      <div class="table" id="classBox">
        <div class="table-scroll">
          <table>
            <thead>
              <tr><th>Decision</th><th>Current State</th></tr>
            </thead>
            <tbody>
              <tr><td>Class</td><td id="classNow">—</td></tr>
            </tbody>
          </table>
        </div>
      </div>

      <!-- TRU parameters + Average Speed gauge side-by-side -->
      <div class="tru-row" style="margin-top:8px">
        <div class="table" id="truParamsTable">
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Category</th>
                  <th>a (L/h)</th>
                  <th>b (L/100km)</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>TOWN</td>
                  <td><input id="tru_a_TOWN" type="number" step="0.01" value="1.8"/></td>
                  <td><input id="tru_b_TOWN" type="number" step="0.01" value="1.2"/></td>
                </tr>
                <tr>
                  <td>CITY</td>
                  <td><input id="tru_a_CITY" type="number" step="0.01" value="1.6"/></td>
                  <td><input id="tru_b_CITY" type="number" step="0.01" value="1.2"/></td>
                </tr>
                <tr>
                  <td>MIXED</td>
                  <td><input id="tru_a_MIXED" type="number" step="0.01" value="1.2"/></td>
                  <td><input id="tru_b_MIXED" type="number" step="0.01" value="1.0"/></td>
                </tr>
                <tr>
                  <td>LINEHAUL</td>
                  <td><input id="tru_a_LINEHAUL" type="number" step="0.01" value="0.8"/></td>
                  <td><input id="tru_b_LINEHAUL" type="number" step="0.01" value="0.8"/></td>
                </tr>
              </tbody>
            </table>
        </div>
      </div>

      <!-- Moved gauge lives here, at the right of the TRU table -->
      <div id="avgSpeedGauge" class="gauge-card gauge-inline" title="Average speed per leg">
        <canvas id="gaugeCanvas"></canvas>
      </div>
    </div>


    </div>
  </div>

  <section class="card" style="width:100vw;border-left:0;border-right:0;border-radius:0">
    <div class="card-header">Route Distance Map <span id="runTs" style="color:#6b7280;font-weight:400"></span></div>
    <div class="map-wrap">
      <div id="route"></div>
      <div class="legend">
        <div><strong>Animated delivery loop</strong></div>
        <div>Stops: <span id="stopCount">—</span></div>
        <div>Next stop: <span id="currStop">—</span></div>
        <div>Leg distance: <span id="segDist">—</span></div>
        <div>Load: <span id="segLoad">—</span> | Unload: <span id="segUnload">—</span></div>
        <div><strong>Current weight:</strong> <span id="currWeight">0 kg</span></div>
        <div>Speed: <code>DRAW_DELAY_MS</code>=<span id="spd">40</span> ms, <code>POINT_STEP</code>=<span id="step">1</span></div>
      </div>
      <button id="playBtn" class="play" disabled>▶ Play</button>
    </div>
  </section>

  <!-- QR Scanner overlay with HUD -->
  <div id="qrOverlay" class="qr-overlay">
    <div class="qr-modal">
      <video id="qrVideo" class="qr-video" autoplay playsinline></video>
      <canvas id="qrPaint" class="qr-paint"></canvas>
      <canvas id="qrCanvas" style="display:none"></canvas>
      <div class="qr-bar">
        <div class="qr-msg">
          <span id="qrLed" class="qr-led" style="background:#ef4444"></span>
          <span id="qrMsg">Align the code</span>
        </div>
        <div class="qr-ctl">
          <span>Brightness: <strong id="qrBri">–</strong></span>
          <span>|</span>
          <label for="qrZoom">Zoom</label>
          <input type="range" id="qrZoom" min="1" max="1" step="0.1" value="1" disabled>
          <button id="qrClose">Close</button>
        </div>
      </div>
    </div>
  </div>

</div>

<!-- Google Maps -->
<script src="https://maps.googleapis.com/maps/api/js?key={{API_KEY}}&libraries=geometry,places"></script>
<!-- QR decoder -->
<script src="https://cdn.jsdelivr.net/npm/jsqr@1.4.0/dist/jsQR.js"></script>
<!-- XLSX for Excel export -->
<script src="https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"></script>

<script>
  // ===== GLOBALS =====
  const FINLAND_BOUNDS = new google.maps.LatLngBounds(
    new google.maps.LatLng(59.3,19.1), new google.maps.LatLng(70.31,31.6)
  );
  const FIT_PADDING = { top: 12, right: 12, bottom: 12, left: 12 };
  function fitFinland(){ if(!finMap) return; finMap.fitBounds(FINLAND_BOUNDS, FIT_PADDING); const z=finMap.getZoom(); if(z>5) finMap.setZoom(5); }

  let finMap, picking = null;
  let geocoder = new google.maps.Geocoder();
  let srcMarker = null;
  let destMarkers = [];

  // refs
  const pickSourceBtn = document.getElementById('pickSource');
  const scanSourceQRBtn = document.getElementById('scanSourceQR');
  const scanDestQRBtn = document.getElementById('scanDestQR');
  const srcNameInput = document.getElementById('srcName');
  const loopIdInput = document.getElementById('loopId');
  const loadInput = document.getElementById('loadVal');
  const unloadInput = document.getElementById('unloadVal');
  const destText = document.getElementById('destText');
  const addDestTextBtn = document.getElementById('addDestText');
  const pickDestBtn = document.getElementById('pickDest');
  const destListDiv = document.getElementById('destList');
  const endBtn = document.getElementById('endBtn');
  const okBtn = document.getElementById('okBtn');
  const loopSelect = document.getElementById('loopSelect');
  const playSelectedBtn = document.getElementById('playSelected');
  // === TRU & Class refs ===
  const truChargeInput = document.getElementById('truCharge');
  const leakInput      = document.getElementById('leakYear');
  const kgRefYearOut   = document.getElementById('kgRefYear');
  const classNowEl     = document.getElementById('classNow');
  const densityInput = document.getElementById('densityKgL');
  const ghgInput     = document.getElementById('ghgEmission');
  const efOut        = document.getElementById('emissionFactor');
  const efLbgInput = document.getElementById('efLbg'); // NEW
  const capacityInput = document.getElementById('capacity');
  // ===== Average Speed Gauge (canvas; white theme) =====
  const gaugeCard   = document.getElementById('avgSpeedGauge');
  const gaugeCanvas = document.getElementById('gaugeCanvas');

  const GAUGE_MIN = 0;
  const GAUGE_MAX = 180;  // change if you want a different top mark

  let gaugeState = { cur: 0, target: 0, animId: null };

  function clamp(v,min,max){ return Math.max(min, Math.min(max, v)); }
  function ease(t){ return 3*t*t - 2*t*t*t; } // smoothstep

  function drawGauge(value){
  if(!gaugeCanvas) return;
  // crisp canvas with devicePixelRatio
  const dpr = window.devicePixelRatio || 1;
  const cssW = gaugeCard.clientWidth;
  const cssH = gaugeCard.clientHeight; // square via CSS
  gaugeCanvas.width  = Math.round(cssW * dpr);
  gaugeCanvas.height = Math.round(cssH * dpr);

  const ctx = gaugeCanvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0); // draw using CSS px

  const W = cssW, H = cssH;
  const cx = W/2, cy = H*0.64;           // center a bit lower
  const r  = Math.min(W,H)*0.46;

  const a0 = -90, a1 = 90;               // degrees (half circle)
  const span = a1 - a0;

  ctx.clearRect(0,0,W,H);

  // title
  ctx.fillStyle = "#1e293b";
  ctx.font = "bold 18px system-ui,-apple-system,Segoe UI,Roboto,Arial";
  ctx.textAlign = "center";
  ctx.fillText("Average Speed", cx, H*0.11);

  // track arc
  ctx.lineWidth = 22;
  ctx.strokeStyle = "#e2e8f0";
  arc(ctx, cx, cy, r, a0, a1);

  // progress arc
  const norm = (clamp(value, GAUGE_MIN, GAUGE_MAX)-GAUGE_MIN)/(GAUGE_MAX-GAUGE_MIN);
  ctx.lineWidth = 6;
  ctx.strokeStyle = "#2563eb";
  arc(ctx, cx, cy, r, a0, a0 + span*norm);

  // ticks (every 10 minor, 20 major)
  for(let v = GAUGE_MIN; v <= GAUGE_MAX; v += 10){
    const ang = deg2rad(a0 + span*(v-GAUGE_MIN)/(GAUGE_MAX-GAUGE_MIN));
    const outer = r - 8;
    const inner = outer - (v % 20 === 0 ? 16 : 10);
    const x1 = cx + inner*Math.cos(ang), y1 = cy + inner*Math.sin(ang);
    const x2 = cx + outer*Math.cos(ang), y2 = cy + outer*Math.sin(ang);
    ctx.strokeStyle = "#64748b";
    ctx.lineWidth = (v % 20 === 0 ? 2 : 1);
    ctx.beginPath(); ctx.moveTo(x1,y1); ctx.lineTo(x2,y2); ctx.stroke();

    if(v % 20 === 0 && v !== GAUGE_MIN){
      const lx = cx + (inner-18)*Math.cos(ang);
      const ly = cy + (inner-18)*Math.sin(ang);
      ctx.fillStyle = "#475569";
      ctx.font = "bold 13px system-ui,-apple-system,Segoe UI,Roboto,Arial";
      ctx.fillText(String(v), lx, ly);
    }
  }

  // needle
  const ang = deg2rad(a0 + span*norm);
  const nx = cx + (r-38)*Math.cos(ang);
  const ny = cy + (r-38)*Math.sin(ang);
  ctx.strokeStyle = "#1e293b"; ctx.lineWidth = 3;
  ctx.beginPath(); ctx.moveTo(cx,cy); ctx.lineTo(nx,ny); ctx.stroke();
  ctx.fillStyle = "#1e293b";
  ctx.beginPath(); ctx.arc(cx,cy,6,0,Math.PI*2); ctx.fill();

  // center value + unit
  ctx.fillStyle = "#1e293b";
  ctx.font = "bold 54px system-ui,-apple-system,Segoe UI,Roboto,Arial";
  ctx.fillText(String(Math.round(value)), cx, cy-10);

  ctx.fillStyle = "#475569";
  ctx.font = "16px system-ui,-apple-system,Segoe UI,Roboto,Arial";
  ctx.fillText("Km/h", cx, cy+30);

  function arc(c, x,y, R, degFrom, degTo){
    c.beginPath();
    c.arc(x, y, R, deg2rad(degFrom), deg2rad(degTo), false);
    c.stroke();
  }
  function deg2rad(d){ return d * Math.PI / 180; }
}

// animate from current to new value
function gaugeSet(newValue){
  if(!gaugeCanvas) return;
  const start = gaugeState.cur;
  const end   = clamp(newValue, GAUGE_MIN, GAUGE_MAX);
  const dur   = 900; // ms
  const t0 = performance.now();
  cancelAnimationFrame(gaugeState.animId);

  function tick(now){
    const t = Math.min(1, (now - t0) / dur);
    const v = start + (end - start) * ease(t);
    gaugeState.cur = v;
    drawGauge(v);
    if(t < 1) gaugeState.animId = requestAnimationFrame(tick);
  }
  gaugeState.animId = requestAnimationFrame(tick);
}

// init once
function initGauge(){
  if(!gaugeCanvas) return;
  drawGauge(0);
  window.addEventListener('resize', ()=>drawGauge(gaugeState.cur));
}
initGauge();


// Editable parameters (a,b) per class
  const truParams = {
  TOWN:     { a: 1.8, b: 1.2 },
  CITY:     { a: 1.6, b: 1.2 },
  MIXED:    { a: 1.2, b: 1.0 },
  LINEHAUL: { a: 0.8, b: 0.8 },
  };
  ['TOWN','CITY','MIXED','LINEHAUL'].forEach(k=>{
  const aEl = document.getElementById('tru_a_'+k);
  const bEl = document.getElementById('tru_b_'+k);
  if(aEl) aEl.addEventListener('input', ()=>{ truParams[k].a = Number(aEl.value)||0; });
  if(bEl) bEl.addEventListener('input', ()=>{ truParams[k].b = Number(bEl.value)||0; });
  });
  function updateKGRefYear(){
  const q = Number(truChargeInput.value)||0;
  const l = Number(leakInput.value)||0;  // enter as fraction (e.g., 0.15)
  const kg = q * l;
  kgRefYearOut.value = kg ? kg.toFixed(3) : '';
  }
  truChargeInput?.addEventListener('input', updateKGRefYear);
  leakInput?.addEventListener('input', updateKGRefYear);

  function updateEmissionFactor(){
  const d = Number(densityInput?.value) || 0;
  const g = Number(ghgInput?.value) || 0;
  const ef = d * g;
  efOut.value = ef ? ef.toFixed(3) : '';
  }
  densityInput?.addEventListener('input', updateEmissionFactor);
  ghgInput?.addEventListener('input', updateEmissionFactor);

  // live calcs
  const calcBody = document.querySelector('#calcTable tbody');
  const calcScroll = document.querySelector('.metrics-table');
  const dlExcelBtn = document.getElementById('dlExcel');

  // state
  let source = null, dests = [], preview = null;

  // animation
  const DRAW_DELAY_MS = 40, POINT_STEP = 1, ROUTE_ZOOM = 12;
  let routeMap, dirService, mover, drawnPolys=[], animToken=0, animData=null;
  const stopCountEl = document.getElementById('stopCount');
  const currStopEl = document.getElementById('currStop');
  const segDistEl = document.getElementById('segDist');
  const segLoadEl = document.getElementById('segLoad');
  const segUnloadEl = document.getElementById('segUnload');
  const currWeightEl = document.getElementById('currWeight');
  const runTsEl = document.getElementById('runTs');
  document.getElementById('spd').textContent = DRAW_DELAY_MS;
  document.getElementById('step').textContent = POINT_STEP;

  // storage for Excel export
  let liveRows = [];
  let lastCWForTable = null; // to avoid duplicate trailing 0 rows
  let perTceCells = [];
  let sfdKmValues = [];
  let leakAllocCells = [];
  let cons1Cells = []; // Consumption1 cells
  let cons2Values = [];    // numeric values of Consumption2 per row
  let tceWtwCells = [];    // cells for TCE WTW emissions
  let notionalValues = [];    // NEW: numeric Notional_Activity per row
  let notionalCells  = [];    // NEW: <td> refs to display Notional_Activity (we fill immediately)
  let allocCells     = [];    // NEW: <td> refs to display Allocation_% (we fill after all rows)

  // ===== QR overlay refs =====
  const qrOverlay = document.getElementById('qrOverlay');
  const qrVideo   = document.getElementById('qrVideo');
  const qrCanvas  = document.getElementById('qrCanvas');  // work canvas
  const qrPaint   = document.getElementById('qrPaint');   // HUD canvas
  const qrMsg     = document.getElementById('qrMsg');
  const qrBri     = document.getElementById('qrBri');
  const qrLed     = document.getElementById('qrLed');
  const qrClose   = document.getElementById('qrClose');
  const qrZoomCtl = document.getElementById('qrZoom');
  let qrMode = null;     // 'src' | 'dest'
  let qrStream = null;
  let qrTicking = false;
  let currentTrack = null;

// ===== HUD parameters (with stability lock + blink) =====
const SIZE_OK_MIN_PX = 150;                 // min QR side (px) for “lock”
const BRIGHT_MIN = 60, BRIGHT_MAX = 215;    // acceptable brightness
const REQ_STABLE_FRAMES = 3;                // need same text N frames to lock

let qrStableText = null;
let qrStableCount = 0;
let hudBlink = false;                       // toggles each frame while scanning

function dist(a,b){ const dx=a.x-b.x, dy=a.y-b.y; return Math.hypot(dx,dy); }
function avgBrightness(img){
  const d = img.data; let sum=0, n=0;
  for (let i=0;i<d.length;i+=12){ const r=d[i], g=d[i+1], b=d[i+2]; sum += 0.2126*r + 0.7152*g + 0.0722*b; n++; }
  return n? (sum/n) : 0;
}
// Class decision — LET(km,SFD; hr,K2; sp=IF(hr>0,km/hr,0); rules…)
function decideClass(km, hr){
  const sp = (hr > 0) ? (km / hr) : 0;
  if (km <= 5)  return 'TOWN';
  if (km <= 15) return 'CITY';
  if (sp <= 25) return 'CITY';
  if (sp <= 60) return 'MIXED';
  return 'LINEHAUL';
}

/**
 * drawHUD: draws a guide box + (optional) detected QR polygon.
 * When `blink` is true, dashed lines animate by offsetting the dash.
 */
function drawHUD(ctx, w, h, corners, color, hint, blink=false){
  ctx.clearRect(0,0,w,h);
  const cw = Math.floor(Math.min(w,h)*0.55), ch=cw, cx=(w-cw)/2, cy=(h-ch)/2;

  // dim background
  ctx.fillStyle = "rgba(0,0,0,.10)";
  ctx.fillRect(0,0,w,h);

  // guide box (white dashed, blinking)
  ctx.save();
  ctx.strokeStyle = "rgba(255,255,255,.92)";
  ctx.lineWidth = 2;
  ctx.setLineDash([8,6]);
  ctx.lineDashOffset = blink ? 7 : 0;     // <-- blink effect
  ctx.strokeRect(cx, cy, cw, ch);
  ctx.restore();

  // detected QR polygon outline (colored)
  if (corners && corners.length >= 4){
    ctx.save();
    ctx.strokeStyle = color;
    ctx.lineWidth = 4;
    ctx.beginPath();
    ctx.moveTo(corners[0].x, corners[0].y);
    for (let i=1;i<corners.length;i++) ctx.lineTo(corners[i].x, corners[i].y);
    ctx.closePath();
    ctx.stroke();
    ctx.restore();
  }

  // hint banner
  if (hint){
    ctx.font = "600 16px system-ui, -apple-system, Segoe UI, Roboto, Arial";
    const m = ctx.measureText(hint);
    const cx2=(w-m.width)/2;
    ctx.fillStyle = "rgba(0,0,0,.55)";
    ctx.fillRect(cx2-8, 14, m.width+16, 24);
    ctx.fillStyle = "#fff";
    ctx.fillText(hint, cx2, 32);
  }
}


  // === QR helpers (parse + apply) ======================================
  function parseQRLatLng(text){
    if(!text) return null;
    const raw = text.trim();

    // geo:lat,lng
    let m = raw.match(/^geo:\s*(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)/i);
    if(m) return { lat: +m[1], lng: +m[2], label: null, src: 'geo' };

    // geo:0,0?q=lat,lng(label)
    m = raw.match(/^geo:\s*0\s*,\s*0\s*\?q=\s*(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)(?:\(([^)]*)\))?/i);
    if(m) return { lat: +m[1], lng: +m[2], label: m[3] || null, src: 'geo_q' };

    // Google Maps links
    try{
      const u = new URL(raw);
      const host = u.hostname.replace(/^www\./,'');
      if (host.endsWith('google.com') || host === 'maps.app.goo.gl' || host === 'goo.gl' || host === 'goo.gle'){
        const q = u.searchParams.get('query') || u.searchParams.get('q');
        if(q){
          const qm = q.match(/(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)/);
          if(qm) return { lat:+qm[1], lng:+qm[2], label: null, src:'gmaps_q' };
        }
        const at = u.pathname.match(/@(-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)/);
        if(at) return { lat:+at[1], lng:+at[2], label: null, src:'gmaps_at' };
      }
    }catch(_){/* not a URL */}
    return null;
  }

  // ⬇️ Replacement: applyScannedLocation that reverse-geocodes names
  function applyScannedLocation(mode, lat, lng, label){
    const latLngObj = new google.maps.LatLng(lat, lng);
    const initialName = (label && label.trim()) || 'Fetching address…';

    // helper to resolve a pretty name and update UI
    const resolveAndApplyName = async (destIndex=null)=>{
      try{
        const addr = await reverseGeocode(latLngObj);
        if(!addr) return;
        if(mode === 'src'){
          source.name = addr;
          srcNameInput.value = addr;
          if (srcMarker) srcMarker.setTitle(addr);
        }else if(destIndex != null && dests[destIndex]){
          dests[destIndex].name = addr;
          if (destMarkers[destIndex]) destMarkers[destIndex].setTitle(addr);
          renderDestList();
        }
      }catch(_){/* ignore */}
    };

    if(mode === 'src'){
      source = { name: initialName, lat, lng };
      if (srcMarker) srcMarker.setMap(null);
      srcMarker = new google.maps.Marker({
        position:{lat,lng}, map: finMap, title: initialName,
        icon:'http://maps.google.com/mapfiles/ms/icons/green-dot.png'
      });
      srcNameInput.value = initialName;
      fitFinland(); updateButtons();
      if (!label) resolveAndApplyName(); // only reverse-geocode when label absent
    }else if(mode === 'dest'){
      let load = parseFloat(loadInput.value);  if(!Number.isFinite(load)) load = 0;
      let unload = parseFloat(unloadInput.value); if(!Number.isFinite(unload)) unload = 0;

      const dest = { name: initialName, lat, lng, load, unload };
      const idx = upsertDest(dest);

      destText.value = '';
      fitFinland(); updateButtons();

      if (!label) resolveAndApplyName(idx); // upgrade placeholder to address
    }
  }
  // =====================================================================

  async function openQR(mode){
    qrMode = mode;
    qrMsg.textContent = 'Align the code';
    qrBri.textContent = '–';
    qrLed.style.background = '#ef4444';
    qrZoomCtl.disabled = true; qrZoomCtl.min = 1; qrZoomCtl.max = 1; qrZoomCtl.value = 1;
    qrOverlay.style.display = 'flex';

    try{
      const constraints = { video: { facingMode: 'environment', width:{ideal:1920}, height:{ideal:1080} } };
      if (!(navigator.mediaDevices && navigator.mediaDevices.getUserMedia)) {
        throw new Error('Camera API not available in this browser.');
      }
      qrStream = await navigator.mediaDevices.getUserMedia(constraints).catch(()=>navigator.mediaDevices.getUserMedia({video:true}));
      qrVideo.srcObject = qrStream;
      await qrVideo.play();

      // Zoom control if supported
      currentTrack = qrStream.getVideoTracks()[0];
      const caps = currentTrack.getCapabilities ? currentTrack.getCapabilities() : {};
      if (caps && caps.zoom){
        qrZoomCtl.min = caps.zoom.min || 1;
        qrZoomCtl.max = caps.zoom.max || 1;
        qrZoomCtl.step = caps.zoom.step || 0.1;
        qrZoomCtl.value = Math.min(Math.max(1, qrZoomCtl.min), qrZoomCtl.max);
        qrZoomCtl.disabled = false;
        qrZoomCtl.oninput = async (e)=>{
          try{ await currentTrack.applyConstraints({ advanced:[{ zoom: Number(e.target.value) }] }); }catch(_){}
        };
      }

      if(!qrTicking){ qrTicking = true; requestAnimationFrame(qrLoop); }
    }catch(err){
      qrMsg.textContent = 'Camera error: ' + (err && err.message ? err.message : err);
      qrLed.style.background = '#ef4444';
    }
  }
  function stopQR(){
    if(qrStream){ qrStream.getTracks().forEach(t=>t.stop()); qrStream = null; }
    qrOverlay.style.display = 'none';
    qrTicking = false;
    currentTrack = null;
  }
  qrClose.addEventListener('click', stopQR);

function qrLoop(){
  if(!qrTicking) return;
  const w = qrVideo.videoWidth || 640, h = qrVideo.videoHeight || 480;
  qrCanvas.width = w; qrCanvas.height = h;
  qrPaint.width  = w; qrPaint.height  = h;
  const ctx = qrCanvas.getContext('2d', {willReadFrequently:true});
  const hud = qrPaint.getContext('2d');

  ctx.drawImage(qrVideo, 0, 0, w, h);
  const img = ctx.getImageData(0, 0, w, h);

  let result = null, corners = null;
  try{
    result = jsQR(img.data, img.width, img.height, { inversionAttempts: 'attemptBoth' });
    if (result && result.location){
      corners = [
        result.location.topLeftCorner,
        result.location.topRightCorner,
        result.location.bottomRightCorner,
        result.location.bottomLeftCorner
      ].map(p=>({x:p.x, y:p.y}));
    }
  }catch(e){ /* ignore */ }

  const bright = Math.round(avgBrightness(img));
  qrBri.textContent = String(bright);
  const brightOK = (bright >= BRIGHT_MIN && bright <= BRIGHT_MAX);

  let sizeOK = false;
  if (corners){
    const w1 = dist(corners[0], corners[1]);
    const w2 = dist(corners[2], corners[3]);
    const h1 = dist(corners[1], corners[2]);
    const h2 = dist(corners[3], corners[0]);
    sizeOK = Math.min(w1,w2,h1,h2) >= SIZE_OK_MIN_PX;
  }

  let color = '#ef4444', hint = 'Align the code';
  let locked = false;

  if (result && sizeOK && brightOK){
    const text = String(result.data || '').trim();
    if (text){
      if (text === qrStableText){
        qrStableCount++;
      }else{
        qrStableText = text;
        qrStableCount = 1;
      }
      if (qrStableCount >= REQ_STABLE_FRAMES){
        locked = true;
        color = '#10b981'; hint = 'Locked';
        // Final HUD (no blink)
        drawHUD(hud, w, h, corners, color, hint, false);
        qrLed.style.background = color;

        // Try to parse coords; otherwise treat as free text
        const parsed = parseQRLatLng(text);
        if (parsed){
          applyScannedLocation(qrMode, parsed.lat, parsed.lng, parsed.label);
        }else{
          if(qrMode === 'src'){
            srcNameInput.value = text;
            srcNameInput.dispatchEvent(new Event('change'));
          }else if(qrMode === 'dest'){
            destText.value = text;
            updateButtons();
            setTimeout(()=> addDestTextBtn.click(), 10);
          }
        }
        stopQR();
        return;
      }else{
        color = '#10b981';
        hint = `Hold steady (${qrStableCount}/${REQ_STABLE_FRAMES})`;
      }
    }
  }else{
    // any failure → reset stability
    qrStableText = null;
    qrStableCount = 0;
    if (!brightOK){
      color = '#f59e0b';
      hint = (bright < BRIGHT_MIN ? 'More light' : 'Reduce glare');
    }else if (corners && !sizeOK){
      color = '#f59e0b';
      hint = 'Move closer';
    }else{
      color = '#ef4444';
      hint = 'Align the code';
    }
  }

  // Blink while not locked
  hudBlink = !hudBlink;
  drawHUD(hud, w, h, corners, color, hint, !locked && hudBlink);
  qrLed.style.background = color;

  requestAnimationFrame(qrLoop);
}

  // ===== TOP map, geocoding, builder logic =====
  pickSourceBtn.addEventListener('click', ()=>{ picking='src'; fitFinland(); });
  scanSourceQRBtn.addEventListener('click', ()=>openQR('src'));
  scanDestQRBtn.addEventListener('click', ()=>openQR('dest'));

  function reverseGeocode(latlng){
    return new Promise((resolve)=>{ geocoder.geocode({location:latlng},(res,st)=>{ if(st==='OK'&&res&&res.length){ resolve(res[0].formatted_address); } else resolve(null); }); });
  }
  function forwardGeocode(text){
    return new Promise((resolve)=>{ geocoder.geocode({
      address:text,
      bounds: FINLAND_BOUNDS,
      region: 'FI',
      componentRestrictions:{country:'fi'}
    },(res,st)=>{
      if(st==='OK'&&res&&res.length){
        const g=res[0].geometry.location;
        resolve({name:res[0].formatted_address, lat:g.lat(), lng:g.lng()});
      }else{
        const t2 = /finland/i.test(text) ? text : (text + ", Finland");
        geocoder.geocode({address:t2, region:'FI', componentRestrictions:{country:'fi'}},(res2,st2)=>{
          if(st2==='OK'&&res2&&res2.length){
            const g2=res2[0].geometry.location;
            resolve({name:res2[0].formatted_address, lat:g2.lat(), lng:g2.lng()});
          }else resolve(null);
        });
      }
    });});
  }

  // ===== de-dup + merge destinations (UI side) =====
  function _samePlace(a, b, eps = 0.0002){ return Math.abs(a.lat-b.lat) < eps && Math.abs(a.lng-b.lng) < eps; } // ~20m
  function _normName(s){ return (s||'').toLowerCase().replace(/\s+/g,' ').trim(); }
  function upsertDest(dest){
    const n = _normName(dest.name);
    for (let i=0;i<dests.length;i++){
      const d = dests[i];
      if (_samePlace(dest, d) || (_normName(d.name) && _normName(d.name) === n)){
        d.load   = (Number(d.load)||0)   + (Number(dest.load)||0);
        d.unload = (Number(d.unload)||0) + (Number(dest.unload)||0);
        if (destMarkers[i]) destMarkers[i].setTitle(d.name);
        renderDestList(); updateButtons();
        return i;
      }
    }
    dests.push(dest);
    const m = new google.maps.Marker({
      position:{lat:dest.lat,lng:dest.lng}, map:finMap, title:dest.name,
      icon:'http://maps.google.com/mapfiles/ms/icons/red-dot.png'
    });
    destMarkers.push(m);
    renderDestList(); updateButtons();
    return dests.length - 1;
  }

  function updateButtons(){
    const load = parseFloat(loadInput.value);
    const unload = parseFloat(unloadInput.value);
    const hasDestText = destText.value.trim().length > 0;

    addDestTextBtn.disabled = !hasDestText;
    const canPickDest = Number.isFinite(load) && Number.isFinite(unload);
    pickDestBtn.disabled = !canPickDest;

    destListDiv.style.display = dests.length ? 'block':'none';
    endBtn.disabled = !(source && dests.length);
    okBtn.disabled = !preview;
    document.getElementById('playBtn').disabled = !animData;
    playSelectedBtn.disabled = (loopSelect.value === '');
  }

  function initFinland(){
    finMap = new google.maps.Map(document.getElementById('finland'),{
      center:{lat:64.5,lng:26.0}, zoom:5, minZoom:4, maxZoom:18,
      restriction:{latLngBounds:FINLAND_BOUNDS,strictBounds:true},
      zoomControl:true, mapTypeControl:true, streetViewControl:true, fullscreenControl:true,
      gestureHandling:'greedy', draggableCursor:'pointer', draggingCursor:'grabbing'
    });
    google.maps.event.addListenerOnce(finMap, 'idle', fitFinland);
    setTimeout(fitFinland, 200);
    window.addEventListener('resize', fitFinland);
    try { new ResizeObserver(fitFinland).observe(document.getElementById('finland')); } catch(e){}
    finMap.addEventListener?.('click',()=>{});

    finMap.addListener('click',(e)=>{
      if(!picking) return;
      reverseGeocode(e.latLng).then(name=>{
        if(picking==='src'){
          source = {name:name||'Source', lat:e.latLng.lat(), lng:e.latLng.lng()};
          srcNameInput.value = source.name;
          if(srcMarker) srcMarker.setMap(null);
          srcMarker = new google.maps.Marker({position:e.latLng, map:finMap, title:'Source', icon:'http://maps.google.com/mapfiles/ms/icons/green-dot.png'});
          picking = null; fitFinland(); updateButtons();
        }else if(picking==='dest'){
          let load = parseFloat(loadInput.value); if(!Number.isFinite(load)) load = 0;
          let unload = parseFloat(unloadInput.value); if(!Number.isFinite(unload)) unload = 0;
          const dest = {name:name||'Destination', lat:e.latLng.lat(), lng:e.latLng.lng(), load, unload};
          upsertDest(dest);
          picking = null; fitFinland(); updateButtons();
        }
      });
    });
    try{
      new google.maps.places.Autocomplete(srcNameInput, {bounds:FINLAND_BOUNDS, componentRestrictions:{country:'fi'}});
      new google.maps.places.Autocomplete(destText,   {bounds:FINLAND_BOUNDS, componentRestrictions:{country:'fi'}});
    }catch(e){ /* ignore if Places not available */ }
  }

  srcNameInput.addEventListener('change', async ()=>{
    if(srcNameInput.value.trim().length){
      const g = await forwardGeocode(srcNameInput.value.trim());
      if(g && FINLAND_BOUNDS.contains(new google.maps.LatLng(g.lat,g.lng))){
        source = {name:g.name, lat:g.lat, lng:g.lng};
        if(srcMarker) srcMarker.setMap(null);
        srcMarker = new google.maps.Marker({position:{lat:g.lat,lng:g.lng}, map:finMap, title:'Source', icon:'http://maps.google.com/mapfiles/ms/icons/green-dot.png'});
        fitFinland();
      }
    }
    updateButtons();
  });

  loadInput.addEventListener('input', updateButtons);
  unloadInput.addEventListener('input', updateButtons);
  destText.addEventListener('input', updateButtons);

  document.getElementById('addDestText').addEventListener('click', async ()=>{
    const text = destText.value.trim();
    if(!text) return;

    let load = parseFloat(loadInput.value); if(!Number.isFinite(load)) load = 0;
    let unload = parseFloat(unloadInput.value); if(!Number.isFinite(unload)) unload = 0;

    const g = await forwardGeocode(text);
    if(g && FINLAND_BOUNDS.contains(new google.maps.LatLng(g.lat,g.lng))){
      const dest = {name:g.name, lat:g.lat, lng:g.lng, load, unload};
      upsertDest(dest);
      destText.value=''; updateButtons();
    }else{
      alert('Could not geocode that text. Try adding ", Finland" or type a simpler address.');
    }
  });

  pickDestBtn.addEventListener('click', ()=>{ let load=parseFloat(loadInput.value); let unload=parseFloat(unloadInput.value);
    if(!Number.isFinite(load)) load=0; if(!Number.isFinite(unload)) unload=0; picking='dest'; fitFinland(); });

  function renderDestList(){
    destListDiv.innerHTML = '';
    dests.forEach((d,i)=>{
      const row = document.createElement('div'); row.className='dest-item';
      row.innerHTML = `<div>${i+1}. ${d.name} &nbsp;|&nbsp; Load ${d.load} kg, Unload ${d.unload} kg</div>
                       <button class="rm" data-i="${i}">Remove</button>`;
      row.querySelector('button').onclick = (e)=>{ const idx = parseInt(e.target.getAttribute('data-i')); dests.splice(idx,1); const m = destMarkers.splice(idx,1)[0]; if(m) m.setMap(null); renderDestList(); updateButtons(); };
      destListDiv.appendChild(row);
    });
  }

  // ===== Preview / Save / Play =====
  async function refreshLoops(){
    const res = await fetch('/api/loops');
    const data = await res.json();
    while(loopSelect.firstChild) loopSelect.removeChild(loopSelect.firstChild);
    const opt0 = document.createElement('option'); opt0.value=''; opt0.textContent='— select —'; loopSelect.appendChild(opt0);
    data.loops.forEach(x=>{ const o=document.createElement('option'); o.value=x.loop; o.textContent=`Loop ${x.loop} (last ${x.last})`; loopSelect.appendChild(o); });
    updateButtons();
  }
  loopSelect.addEventListener('change', updateButtons);

  document.getElementById('endBtn').addEventListener('click', async ()=>{
    if(!(source && dests.length)) return;
    const res = await fetch('/api/preview', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({source, dests}) });
    const data = await res.json(); preview = data;
    const box = document.getElementById('previewTable');
    const rows = data.rows;
    let html = '<div class="table-scroll"><table><thead><tr><th>Phase</th><th>Leg</th><th>Distance</th><th>Duration</th></tr></thead><tbody>';
    rows.forEach(r=>{ html += `<tr><td>${r.phase}</td><td>${r.leg}</td><td>${r.dist}</td><td>${r.dur}</td></tr>`; });
    html += `</tbody></table></div><div class="totals"><b>Order:</b> ${data.order} &nbsp; | &nbsp; Total distance: <b>${data.total_dist.toFixed(2)} km</b> &nbsp;|&nbsp; Total duration: <b>${data.total_time.toFixed(1)} min</b></div>`;
    box.innerHTML = html; box.style.display='block'; updateButtons();
  });

  document.getElementById('okBtn').addEventListener('click', async ()=>{
    if(!preview) return;
    const loop_id = (loopIdInput.value||'').trim(); if(!loop_id){ alert('Enter a Loop ID first.'); return; }
    const res = await fetch('/api/save', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({source, dests, loop: loop_id}) });
    const data = await res.json(); if(!data.ok){ alert('Save failed.'); return; }
    animData = data.anim; runTsEl.textContent = `(run ${animData.run_ts})`; stopCountEl.textContent = animData.stops.length;
    if(!routeMap) initRouteMap(); startAnimation(); updateButtons(); await refreshLoops();
  });

  document.getElementById('playSelected').addEventListener('click', async ()=>{
    if(!loopSelect.value) return;
    const res = await fetch('/api/loop_data?loop='+encodeURIComponent(loopSelect.value));
    const data = await res.json(); if(!data.ok){ alert('No data for that loop.'); return; }
    animData = data.anim; runTsEl.textContent = `(run ${animData.run_ts})`; stopCountEl.textContent = animData.stops.length;
    if(!routeMap) initRouteMap(); startAnimation(); updateButtons();
  });

  // ===== Bottom map & animation =====
  function initRouteMap(){
    routeMap = new google.maps.Map(document.getElementById('route'),{
      zoom: 5, center: {lat:64.5,lng:26.0},
      zoomControl:true, mapTypeControl:true, streetViewControl:true, fullscreenControl:true,
      gestureHandling:'greedy'
    });
    dirService = new google.maps.DirectionsService();
    mover = new google.maps.Marker({ position:{lat:64.5,lng:26.0}, map:routeMap, icon:"http://maps.google.com/mapfiles/ms/icons/blue-dot.png" });
    document.getElementById('playBtn').addEventListener('click', ()=>startAnimation());
  }

function resetCalcTable(){
  calcBody.innerHTML = '';
  liveRows = [];
  lastCWForTable = null;
  dlExcelBtn.disabled = true;

  perTceCells = [];
  sfdKmValues = [];
  leakAllocCells = [];
  cons1Cells = [];
  cons2Values = [];
  tceWtwCells = [];

  // NEW: clear the extra arrays too
  notionalValues = [];
  notionalCells  = [];
  allocCells     = [];
}

function appendCalcRow(loadKg, unloadKg, currentWeightKg, legDistanceKm, timeMinutes, loadingName, unloadingName){
  // Avoid duplicate trailing 0 rows
  const EPS = 1e-9;
  if (lastCWForTable !== null &&
      Math.abs(lastCWForTable) < EPS &&
      Math.abs(Number(currentWeightKg) || 0) < EPS){
    lastCWForTable = Number(currentWeightKg) || 0;
    return;
  }

  // base values
  const safeW = Math.max(0, Number(currentWeightKg) || 0);
  const lvp = safeW / 900.0;

  // SFD = distance in km (exact)
  const sfdKm = Number(legDistanceKm) || 0;

  // Time & Hours
  const tMin = Number(timeMinutes) || 0;
  const hours = tMin / 60.0;

  // Average speed
  const avgSpeed = hours > 0 ? (sfdKm / hours) : 0;
  gaugeSet(avgSpeed);

  // Class (LET rules)
  const klass = decideClass(sfdKm, hours);
  if (classNowEl) classNowEl.textContent = klass;

  // Lookups from editable table
  const p = truParams[klass] || { a: 0, b: 0 };
  const aLph = Number(p.a) || 0;
  const bLper100 = Number(p.b) || 0;

  // TRU_L = a*Hours + (b/100)*SFD
  const truLiters = (aLph * hours) + ((bLper100 / 100) * sfdKm);

  // NEW: LPH = TRU_L / Hours (avoid divide-by-zero)
  const lph = hours > 0 ? (truLiters / hours) : 0;

  // CO2e_fuel = TRU_L (L) × Emission Factor (WTW per liter)
  const emissionFactor = Number(efOut?.value) || 0;
  const co2eFuel = (Number(truLiters) || 0) * emissionFactor;

  // Build DOM row — order MUST match header
  const tr = document.createElement('tr');
  function td(v){ const x=document.createElement('td'); x.textContent=v; return x; }

  tr.appendChild(td(loadingName || '—'));
  tr.appendChild(td(unloadingName || '—'));
  tr.appendChild(td((Number(loadKg)   || 0).toFixed(0)));
  tr.appendChild(td((Number(unloadKg) || 0).toFixed(0)));
  tr.appendChild(td(safeW.toFixed(2)));
  tr.appendChild(td(lvp.toFixed(3)));
  tr.appendChild(td(sfdKm.toFixed(3)));
  // Time, hours, speed, class & fuel params
  tr.appendChild(td(tMin.toFixed(1)));
  tr.appendChild(td(hours.toFixed(3)));
  tr.appendChild(td(avgSpeed.toFixed(2)));
  tr.appendChild(td(klass));
  tr.appendChild(td(aLph ? aLph.toFixed(2) : ''));
  tr.appendChild(td(bLper100 ? bLper100.toFixed(2) : ''));
  tr.appendChild(td(truLiters ? truLiters.toFixed(3) : ''));
  tr.appendChild(td((hours > 0 && truLiters) ? lph.toFixed(3) : ''));
  tr.appendChild(td(co2eFuel ? co2eFuel.toFixed(3) : ''));

  // Per TCE & Leakage placeholders (now in the right place)
  const perTceTd = td('');
  tr.appendChild(perTceTd);
  perTceCells.push(perTceTd);
  sfdKmValues.push(sfdKm);

  const leakTd = td('');
  tr.appendChild(leakTd);
  leakAllocCells.push(leakTd);

  // Consumption1 placeholder
  const cons1Td = td('');
  tr.appendChild(cons1Td);
  cons1Cells.push(cons1Td);

  // Consumption2
  const efLbg = Number(efLbgInput?.value) || 0;
  const cons2 = (sfdKm * 32 / 100) * efLbg;
  tr.appendChild(td(cons2 ? cons2.toFixed(6) : ''));

  // TCE WTW placeholder
  const tceWtwTd = td('');
  tr.appendChild(tceWtwTd);
  tceWtwCells.push(tceWtwTd);

  // Notional_Activity
  const notional = ((Number(unloadKg) || 0) / 1000) * sfdKm;
  const notionalTd = td(notional ? notional.toFixed(6) : '');
  tr.appendChild(notionalTd);
  notionalValues.push(notional);
  notionalCells.push(notionalTd);

  // Allocation_% placeholder
  const allocTd = td('');
  tr.appendChild(allocTd);
  allocCells.push(allocTd);

  // Keep Consumption2 for final sum
  cons2Values.push(cons2);

  calcBody.appendChild(tr);
  calcScroll.scrollTop = calcScroll.scrollHeight;

  // Excel export (columns align with header)
  liveRows.push({
    "Loading_Location": loadingName || "",
    "Unloading_Location": unloadingName || "",
    "Load (kg)": Number(loadKg) || 0,
    "Unload (kg)": Number(unloadKg) || 0,
    "Current Weight (kg)": +safeW.toFixed(2),
    "LVP": +lvp.toFixed(3),
    "SFD (km)": +sfdKm.toFixed(3),
    "Time (minutes)": +tMin.toFixed(1),
    "Hours": +hours.toFixed(3),
    "Average Speed": +avgSpeed.toFixed(2),
    "Class": klass,
    "Liter/Hour (a)": aLph ? +aLph.toFixed(2) : "",
    "L/100km (b)": bLper100 ? +bLper100.toFixed(2) : "",
    "TRU_L (L)": truLiters ? +truLiters.toFixed(3) : 0,
    "LPH (L/h)": (hours > 0 && truLiters) ? +lph.toFixed(3) : 0,
    "Cooling Machine's CO2e_fuel (kg)": co2eFuel ? +co2eFuel.toFixed(3) : 0,
    "Per TCE Value Identification-SFD": 0,
    "Default Leakage shared each SFD wise": 0,
    "Consumption1": 0,
    "Consumption2": cons2 ? +cons2.toFixed(6) : 0,
    "TCE WTW emissions (kgCO₂e)": 0,
    "Notional_Activity": notional ? +notional.toFixed(6) : 0,
    "Allocation_%": ""   // will be filled in finalize step as a percent string
  });

  lastCWForTable = safeW;
}

function exportExcel(){
  try{
    if (!liveRows.length){
      alert("No data to export yet.");
      return;
    }
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet(liveRows);
    XLSX.utils.book_append_sheet(wb, ws, "Live Calculations");
    const fname = `live_calculations_${new Date().toISOString().slice(0,19).replace(/[:T]/g,'-')}.xlsx`;
    XLSX.writeFile(wb, fname);
  }catch(e){
    alert("Excel export failed: " + e);
  }
}

  dlExcelBtn.addEventListener('click', exportExcel);

function finalizeAllocations(){
  // 1) Per-TCE = SFD_i / SUM(SFD)
  const totalSfd = sfdKmValues.reduce((a,b)=>a+(Number(b)||0), 0);
  const nRows = perTceCells.length || 0;

  // 2) KG Refrigerant/Year — prefer the computed readonly box
  let kgYear = Number(kgRefYearOut?.value);
  if (!Number.isFinite(kgYear)) {
    const q = Number(truChargeInput?.value) || 0;
    const l = Number(leakInput?.value) || 0;
    kgYear = q * l;
  }
  const perRowShare = (nRows > 0) ? (kgYear / nRows) : 0;

  for (let i = 0; i < nRows; i++){
    const sfd = Number(sfdKmValues[i]) || 0;

    // Per-TCE share
    const perTce = (totalSfd > 0) ? (sfd / totalSfd) : 0;
    perTceCells[i].textContent = perTce.toFixed(6);
    if (liveRows[i]) liveRows[i]["Per TCE Value Identification-SFD"] = +perTce.toFixed(6);

    // Default Leakage shared each SFD wise
    const leakAlloc = perTce * perRowShare;
    leakAllocCells[i].textContent = leakAlloc ? leakAlloc.toFixed(6) : '';
    if (liveRows[i]) liveRows[i]["Default Leakage shared each SFD wise"] = leakAlloc ? +leakAlloc.toFixed(6) : 0;

    // Consumption1 = Cooling Machine's CO2e_fuel + leakage share
    const co2e = Number(liveRows[i]?.["Cooling Machine's CO2e_fuel (kg)"]) || 0;
    const cons1 = co2e + (Number(leakAlloc) || 0);
    if (cons1Cells[i]) cons1Cells[i].textContent = cons1 ? cons1.toFixed(6) : '';
    if (liveRows[i])    liveRows[i]["Consumption1"] = cons1 ? +cons1.toFixed(6) : 0;

    // TCE WTW emissions = Consumption1 + Consumption2
    const cons2 = Number(cons2Values[i]) || Number(liveRows[i]?.["Consumption2"]) || 0;
    const totalWtw = cons1 + cons2;
    if (tceWtwCells[i]) tceWtwCells[i].textContent = totalWtw ? totalWtw.toFixed(6) : '';
    if (liveRows[i])     liveRows[i]["TCE WTW emissions (kgCO₂e)"] = totalWtw ? +totalWtw.toFixed(6) : 0;
  }

  // 3) Allocation_% on Notional_Activity
  const totalNotional = notionalValues.reduce((a,b)=>a+(Number(b)||0), 0);
  for (let i = 0; i < allocCells.length; i++){
    const v = Number(notionalValues[i]) || 0;
    const pct = (totalNotional > 0) ? (v / totalNotional) : 0;   // 0..1
    const pctStr = (pct * 100).toFixed(3) + '%';
    if (allocCells[i]) allocCells[i].textContent = pctStr;
    if (liveRows[i])   liveRows[i]["Allocation_%"] = pctStr;     // store as string with %
  }
}


  async function startAnimation(){ if(!animData) return; resetAnimation(); resetCalcTable();gaugeSet(0);
    const btn=document.getElementById('playBtn'); btn.textContent="⏳ Running…"; btn.disabled=true;
    animData.stops.forEach(s=> new google.maps.Marker({position:{lat:s.lat,lng:s.lng}, map:routeMap, title:s.name}));
    await runLegs(0, animToken); finalizeAllocations(); btn.textContent="▶ Play"; btn.disabled=false; dlExcelBtn.disabled = false; }

  function resetAnimation(){ animToken++; for(const p of drawnPolys) p.setMap(null); drawnPolys=[];
    if(animData && animData.stops.length){ mover.setPosition({lat:animData.stops[0].lat,lng:animData.stops[0].lng}); routeMap.setZoom(12);
      routeMap.setCenter({lat:animData.stops[0].lat,lng:animData.stops[0].lng}); currWeightEl.textContent=(animData.cwBefore[0]||0).toFixed(2)+" kg";
      currStopEl.textContent = animData.stops[1]?animData.stops[1].name:"—"; segDistEl.textContent="—"; segLoadEl.textContent="—"; segUnloadEl.textContent="—"; } }

  function getRoutePoints(origin, destination){
    return new Promise((resolve)=>{
      dirService.route({ origin, destination, travelMode: google.maps.TravelMode.DRIVING, provideRouteAlternatives:false, avoidFerries:true },
        (result, status)=>{
          if(status !== google.maps.DirectionsStatus.OK || !result.routes?.length){
            resolve([new google.maps.LatLng(origin.lat,origin.lng), new google.maps.LatLng(destination.lat,destination.lng)]); return;
          }
          const route=result.routes[0];
          if(route.overview_polyline?.points){ resolve(google.maps.geometry.encoding.decodePath(route.overview_polyline.points)); }
          else if(route.overview_path?.length){ resolve(route.overview_path); }
          else{
            const pts=[]; for(const leg of route.legs){ for(const step of leg.steps){ if(step.polyline?.points){ const dec=google.maps.geometry.encoding.decodePath(step.polyline.points); for(const p of dec) pts.push(p); } } }
            resolve(pts.length?pts:[new google.maps.LatLng(origin.lat,origin.lng), new google.maps.LatLng(destination.lat,destination.lng)]);
          }
        });
    });
  }

  async function runLegs(i, token){
    if(i >= animData.stops.length - 1) return; if(token !== animToken) return;
    const origin = animData.stops[i], destination = animData.stops[i+1];
    const points = await getRoutePoints(origin, destination); if(token !== animToken) return;
    const segLine = new google.maps.Polyline({ path:[], geodesic:true, strokeColor:"#0078FF", strokeOpacity:1.0, strokeWeight:4, map:routeMap }); drawnPolys.push(segLine);
    let distKm = null;
    if(animData.segDistKm[i] != null){ distKm = Number(animData.segDistKm[i]); }
    else { let meters=0; for(let k=1;k<points.length;k++){ meters+=google.maps.geometry.spherical.computeDistanceBetween(points[k-1],points[k]); } distKm = meters/1000; }
    segDistEl.textContent = distKm.toFixed(2) + " km";
    currStopEl.textContent = destination.name;
    segLoadEl.textContent = (animData.segLoadKg[i]!=null?Number(animData.segLoadKg[i]).toFixed(0):"—");
    segUnloadEl.textContent = (animData.segUnloadKg[i]!=null?Number(animData.segUnloadKg[i]).toFixed(0):"—");
    currWeightEl.textContent = (animData.cwBefore[i]||0).toFixed(2) + " kg";
    await drawPoints(segLine, points, token); if(token !== animToken) return;
    const cwAfter = Math.max(0, (animData.cwAfter[i]||0));
    currWeightEl.textContent = cwAfter.toFixed(2) + " kg";

    // Duration (minutes) comes from backend; fall back to 0 if missing
    const timeMin = (animData.segDurMin && animData.segDurMin[i] != null) ? Number(animData.segDurMin[i]) : 0;

    // Add row with locations
    appendCalcRow(
      animData.segLoadKg[i],
      animData.segUnloadKg[i],
      cwAfter,
      distKm,
      timeMin,
      origin.name,
      destination.name
    );

    await runLegs(i+1, token);
  }

  function drawPoints(polyline, points, token){
    return new Promise((resolve)=>{ let idx=0; function tick(){ if(token!==animToken){ resolve(); return; } if(idx>=points.length){ resolve(); return; }
      for(let c=0;c<POINT_STEP && idx<points.length;c++,idx++){ polyline.getPath().push(points[idx]); }
      const pos = points[Math.min(idx-1, points.length-1)]; mover.setPosition(pos); routeMap.panTo(pos); setTimeout(tick, DRAW_DELAY_MS); } tick(); });
  }

  async function boot(){ initFinland(); initRouteMap(); updateButtons(); await refreshLoops(); }
  window.onload = boot;
</script>
</body>
</html>

"""

# =======================
# HTTP handler
# =======================
class Handler(BaseHTTPRequestHandler):
    def _send(self, code=200, ctype="text/html", body=b""):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.end_headers()
        if isinstance(body, str): body = body.encode("utf-8")
        self.wfile.write(body)

    def do_GET(self):
        if self.path in ("/", "/index.html"):
            html = HTML_PAGE.replace("{{API_KEY}}", API_KEY)
            self._send(200, "text/html; charset=utf-8", html); return

        # list loops
        if self.path.startswith("/api/loops"):
            conn = mysql_conn(); cur = conn.cursor()
            try:
                cur.execute(f"""
                    SELECT `{COL_LOOP}`, MAX(`{COL_TS}`)
                    FROM `{TABLE}`
                    WHERE `{COL_LOOP}` IS NOT NULL AND `{COL_LOOP}` <> ''
                    GROUP BY `{COL_LOOP}`
                    ORDER BY MAX(`{COL_TS}`) DESC
                """)
                data = [{"loop": str(r[0]), "last": (r[1].strftime("%Y-%m-%d %H:%M:%S") if r[1] else "")} for r in cur.fetchall()]
            finally:
                try: cur.close(); conn.close()
                except: pass
            self._send(200, "application/json", json.dumps({"ok": True, "loops": data})); return

        # loop data → anim arrays
        if self.path.startswith("/api/loop_data"):
            from urllib.parse import urlparse, parse_qs
            q = parse_qs(urlparse(self.path).query)
            loop = (q.get("loop", [""])[0] or "").strip()
            if not loop:
                self._send(400, "application/json", json.dumps({"ok":False,"error":"missing loop"})); return
            anim = anim_arrays_for_loop(loop)
            if not anim:
                self._send(200, "application/json", json.dumps({"ok":False})); return
            self._send(200, "application/json", json.dumps({"ok":True, "anim":anim})); return

        self._send(404, "text/plain", "Not Found")

    def do_POST(self):
        length = int(self.headers.get("Content-Length","0") or "0")
        raw = self.rfile.read(length).decode("utf-8") if length else ""
        try:
            data = json.loads(raw) if raw else {}
        except Exception:
            return self._send(400, "application/json", json.dumps({"ok":False,"error":"bad json"}))

        if self.path == "/api/preview":
            try:
                src = data["source"]; dests = data["dests"]
                payload = solve_phased_route(src, dests)
                rows, td, tt, order = legs_table_for_preview(payload)
                resp = {"ok":True, "rows":rows, "total_dist":td, "total_time":tt, "order":order}
                return self._send(200, "application/json", json.dumps(resp))
            except Exception as e:
                return self._send(500, "application/json", json.dumps({"ok":False,"error":str(e)}))

        if self.path == "/api/save":
            try:
                src = data["source"]; dests = data["dests"]; loop_id = str(data.get("loop") or "").strip()
                if not loop_id:
                    return self._send(400, "application/json", json.dumps({"ok":False,"error":"loop required"}))
                payload = solve_phased_route(src, dests)
                anim = save_run_to_mysql(payload, loop_id)
                return self._send(200, "application/json", json.dumps({"ok":True, "anim":anim}))
            except Exception as e:
                return self._send(500, "application/json", json.dumps({"ok":False,"error":str(e)}))

        return self._send(404, "application/json", json.dumps({"ok":False,"error":"not found"}))

# =======================
# main
# =======================
def main():
    print(f"\nOpen this link in your browser:\n  http://{HOST}:{PORT}\n")
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down…")
        server.shutdown()

if __name__ == "__main__":
    main()
