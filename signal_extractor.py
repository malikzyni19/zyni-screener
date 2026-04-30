"""
Zone Intelligence v1 — Signal Extractor
Converts api_scan result dicts into normalized signal dicts
ready for logging via signal_logger.log_signal().

v1 supported setups:
  OB_APPROACH   → module=ob,  zone from meta.obTop / meta.obBottom
  OB_CONSOL     → module=ob,  zone from meta.obTop / meta.obBottom
  FVG           → module=fvg, zone from meta.fvgTop / meta.fvgBottom
  BREAKER_APPROACH → module=bb, zone from meta.obTop / meta.obBottom
  BREAKER_INSIDE   → module=bb, zone from meta.obTop / meta.obBottom

Skipped in v1:
  FIB_APPROACH, FIB_REACTION — no natural zone; deferred to v2
  RSI and any unrecognised setup
"""

import json


# Maps setup strings → (module, zone_high_key, zone_low_key)
_SETUP_MAP = {
    "OB_APPROACH":       ("ob",  "obTop",  "obBottom"),
    "OB_CONSOL":         ("ob",  "obTop",  "obBottom"),
    "FVG":               ("fvg", "fvgTop", "fvgBottom"),
    "BREAKER_APPROACH":  ("bb",  "obTop",  "obBottom"),
    "BREAKER_INSIDE":    ("bb",  "obTop",  "obBottom"),
}


# ─────────────────────────────────────────────────────────────────────────────
# Internal per-alert extractor (used by both public functions)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_one_alert(
    alert: dict,
    result: dict,
    exchange: str,
    timeframe: str,
    allowed_modules: set | None,
) -> dict | None:
    """
    Extract a normalized signal from one alert dict.

    Returns None if the alert is unrecognised, filtered by allowed_modules,
    has an invalid zone, or is missing required fields.
    """
    try:
        raw_setup = alert.get("setup", "")
        if not raw_setup:
            return None

        setup_key = raw_setup.strip().upper()
        mapping = _SETUP_MAP.get(setup_key)
        if mapping is None:
            return None  # FIB, RSI, unknown

        module, zh_key, zl_key = mapping

        if allowed_modules is not None and module not in allowed_modules:
            return None  # excluded by scan module filter

        meta = alert.get("meta") or {}
        zone_high = meta.get(zh_key)
        zone_low  = meta.get(zl_key)

        if zone_high is None or zone_low is None:
            return None
        zone_high = float(zone_high)
        zone_low  = float(zone_low)
        if zone_high <= zone_low or zone_low <= 0:
            return None

        direction      = alert.get("direction", "bullish")
        score          = int(result.get("score", 0))
        detected_price = float(result.get("price", 0))
        pair           = result.get("symbol", "")
        tf             = result.get("timeframe") or timeframe

        if not pair or detected_price <= 0:
            return None

        try:
            raw_meta_json = json.dumps(meta, default=str)
        except Exception:
            raw_meta_json = None

        return {
            "pair":           pair,
            "module":         module,
            "timeframe":      tf,
            "direction":      direction,
            "score":          score,
            "zone_high":      zone_high,
            "zone_low":       zone_low,
            "detected_price": detected_price,
            "exchange":       exchange,
            "setup_type":     setup_key,
            "raw_setup":      raw_setup,
            "raw_meta_json":  raw_meta_json,
        }
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Public: single-signal extractor (backward-compatible, topAlert only)
# ─────────────────────────────────────────────────────────────────────────────

def extract_scan_signal(result: dict, exchange: str, timeframe: str) -> dict | None:
    """
    Extract a normalized signal dict from a single api_scan result using
    only result["topAlert"].

    Kept for backward compatibility. Prefer extract_zone_signals_from_api_scan_result
    for new code (it respects allowed_modules and reads all alerts).
    """
    try:
        top_alert = result.get("topAlert") or {}
        if not top_alert:
            return None
        return _extract_one_alert(top_alert, result, exchange, timeframe, None)
    except Exception:
        return None


def extract_zone_signal_from_api_scan_result(result: dict, exchange: str = "binance") -> dict | None:
    """
    Convenience wrapper (backward-compatible).
    Reads topAlert only. Use extract_zone_signals_from_api_scan_result for
    module-filtered, multi-alert extraction.
    """
    return extract_scan_signal(
        result,
        exchange=exchange,
        timeframe=result.get("timeframe", "1h"),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public: multi-signal extractor with allowed_modules filter
# ─────────────────────────────────────────────────────────────────────────────

def extract_zone_signals_from_api_scan_result(
    result: dict,
    exchange: str = "binance",
    allowed_modules: set | None = None,
) -> list:
    """
    Extract ALL loggable signals from result["alerts"], respecting allowed_modules.

    Unlike extract_zone_signal_from_api_scan_result (topAlert only), this
    iterates every alert in the result so that module-filtered scans (e.g.
    OB-only) correctly log OB signals even when FVG is the topAlert.

    Deduplicates by (module, direction, zone_high_6sf, zone_low_6sf) within
    one result — the same zone detected via multiple setups logs only once.

    Args:
        result:          One element from api_scan's "results" list.
        exchange:        Exchange string (e.g. "binance").
        allowed_modules: Set of module strings to include: "ob", "fvg", "bb".
                         None or empty = all supported modules accepted.

    Returns:
        List of normalized signal dicts (may be empty, never raises).
    """
    try:
        alerts = result.get("alerts") or []
        tf     = result.get("timeframe", "1h")

        # Normalise: None/empty set → accept all supported modules
        _allowed = allowed_modules if allowed_modules else None

        signals = []
        seen: set = set()  # dedup key per result

        for alert in alerts:
            sig = _extract_one_alert(alert, result, exchange, tf, _allowed)
            if sig is None:
                continue

            dedup_key = (
                sig["module"],
                sig["direction"],
                f"{sig['zone_high']:.6g}",
                f"{sig['zone_low']:.6g}",
            )
            if dedup_key in seen:
                continue
            seen.add(dedup_key)
            signals.append(sig)

        return signals

    except Exception:
        return []


# ─────────────────────────────────────────────────────────────────────────────
# Tests — run directly: python3 signal_extractor.py
# ─────────────────────────────────────────────────────────────────────────────

def _run_extractor_tests():
    PASS = FAIL = 0

    def chk(label, condition, detail=""):
        nonlocal PASS, FAIL
        if condition:
            print(f"  ✓  {label}")
            PASS += 1
        else:
            print(f"  ✗  {label}" + (f" — {detail}" if detail else ""))
            FAIL += 1

    # ── Shared fixture builders ──────────────────────────────────────────────
    def _result(symbol="BTCUSDT", price=100.0, score=70, timeframe="1h", alerts=None, topAlert=None):
        a = alerts or []
        return {
            "symbol":    symbol,
            "price":     price,
            "score":     score,
            "timeframe": timeframe,
            "alerts":    a,
            "topAlert":  topAlert or (a[0] if a else {}),
        }

    def _ob_alert(direction="bullish", top=100.0, bottom=95.0):
        return {
            "setup": "OB_APPROACH", "direction": direction, "strength": 6,
            "meta":  {"obTop": top, "obBottom": bottom},
        }

    def _fvg_alert(direction="bullish", top=100.0, bottom=95.0):
        return {
            "setup": "FVG", "direction": direction, "strength": 5,
            "meta":  {"fvgTop": top, "fvgBottom": bottom},
        }

    def _fib_alert():
        return {"setup": "FIB_APPROACH", "direction": "bullish", "strength": 3, "meta": {}}

    def _bb_alert(direction="bullish", top=100.0, bottom=95.0):
        return {
            "setup": "BREAKER_APPROACH", "direction": direction, "strength": 7,
            "meta":  {"obTop": top, "obBottom": bottom},
        }

    print()
    print("=" * 60)
    print("signal_extractor — extract_zone_signals tests")
    print("=" * 60)

    # ── 1. OB-only allowed_modules: logs OB, skips FVG ──────────────────────
    print("\n[1] OB-only allowed_modules logs OB, skips FVG")
    r = _result(alerts=[_ob_alert(), _fvg_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"ob"})
    chk("one signal returned",      len(sigs) == 1, str(len(sigs)))
    chk("module=ob",                sigs[0]["module"] == "ob" if sigs else False)

    # ── 2. FVG-only allowed_modules: logs FVG, skips OB ─────────────────────
    print("\n[2] FVG-only allowed_modules logs FVG, skips OB")
    r = _result(alerts=[_ob_alert(), _fvg_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"fvg"})
    chk("one signal returned",      len(sigs) == 1, str(len(sigs)))
    chk("module=fvg",               sigs[0]["module"] == "fvg" if sigs else False)

    # ── 3. OB+FVG allowed_modules: logs both ────────────────────────────────
    print("\n[3] OB+FVG allowed_modules logs both")
    r = _result(alerts=[_ob_alert(), _fvg_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"ob", "fvg"})
    chk("two signals returned",     len(sigs) == 2, str(len(sigs)))
    modules = {s["module"] for s in sigs}
    chk("ob and fvg present",       modules == {"ob", "fvg"}, str(modules))

    # ── 4. FIB skipped even when present ────────────────────────────────────
    print("\n[4] FIB skipped even if present")
    r = _result(alerts=[_fib_alert(), _ob_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"ob", "fvg", "bb"})
    chk("one signal (no FIB)",      len(sigs) == 1, str(len(sigs)))
    chk("module=ob",                sigs[0]["module"] == "ob" if sigs else False)

    # ── 5. topAlert=FVG, alerts has OB, allowed_modules={"ob"} → OB logged ──
    print("\n[5] topAlert=FVG but alerts has OB, allowed_modules=ob → logs OB only")
    fvg = _fvg_alert()
    ob  = _ob_alert()
    r = _result(alerts=[fvg, ob], topAlert=fvg)  # FVG is topAlert
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"ob"})
    chk("one signal returned",      len(sigs) == 1, str(len(sigs)))
    chk("module=ob (not fvg)",      sigs[0]["module"] == "ob" if sigs else False)

    # ── 6. topAlert=OB, allowed_modules={"fvg"} → FVG logged if present ────
    print("\n[6] topAlert=OB, allowed_modules=fvg → logs FVG only if present")
    ob  = _ob_alert()
    fvg = _fvg_alert()
    r = _result(alerts=[ob, fvg], topAlert=ob)
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"fvg"})
    chk("one signal returned",      len(sigs) == 1, str(len(sigs)))
    chk("module=fvg",               sigs[0]["module"] == "fvg" if sigs else False)

    # ── 7. OB-only scan with only FVG alerts → logs nothing ─────────────────
    print("\n[7] OB-only allowed_modules, only FVG alerts → logs nothing")
    r = _result(alerts=[_fvg_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"ob"})
    chk("zero signals",             len(sigs) == 0, str(len(sigs)))

    # ── 8. Missing alerts[] → empty list, no crash ──────────────────────────
    print("\n[8] Missing alerts[] → empty, no crash")
    r = {"symbol": "BTCUSDT", "price": 100.0, "score": 50}
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("returns empty list",       sigs == [], str(sigs))

    # ── 9. Invalid zone skipped ──────────────────────────────────────────────
    print("\n[9] Invalid zones skipped")
    bad_alerts = [
        {"setup": "OB_APPROACH", "direction": "bullish", "meta": {"obTop": 90.0, "obBottom": 100.0}},  # top < bottom
        {"setup": "OB_APPROACH", "direction": "bullish", "meta": {"obTop": 100.0, "obBottom": 0.0}},   # bottom=0
        {"setup": "OB_APPROACH", "direction": "bullish", "meta": {}},                                   # missing keys
    ]
    r = _result(alerts=bad_alerts)
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("zero signals (all invalid zones)", len(sigs) == 0, str(len(sigs)))

    # ── 10. Dedup: same zone via OB_APPROACH and OB_CONSOL logs once ────────
    print("\n[10] Same zone via two OB setups deduplicates to one signal")
    alert1 = {"setup": "OB_APPROACH", "direction": "bullish", "strength": 6,
               "meta": {"obTop": 100.0, "obBottom": 95.0}}
    alert2 = {"setup": "OB_CONSOL",   "direction": "bullish", "strength": 5,
               "meta": {"obTop": 100.0, "obBottom": 95.0}}
    r = _result(alerts=[alert1, alert2])
    sigs = extract_zone_signals_from_api_scan_result(r)
    chk("one signal (deduped)",     len(sigs) == 1, str(len(sigs)))
    chk("module=ob",                sigs[0]["module"] == "ob" if sigs else False)

    # ── 11. BB (BREAKER) included when allowed_modules has bb ────────────────
    print("\n[11] BREAKER_APPROACH logged when bb in allowed_modules")
    r = _result(alerts=[_bb_alert(), _fvg_alert()])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules={"bb"})
    chk("one signal",               len(sigs) == 1, str(len(sigs)))
    chk("module=bb",                sigs[0]["module"] == "bb" if sigs else False)

    # ── 12. allowed_modules=None → all modules accepted ─────────────────────
    print("\n[12] allowed_modules=None accepts all supported modules")
    r = _result(alerts=[_ob_alert(), _fvg_alert(), _bb_alert(top=110.0, bottom=105.0)])
    sigs = extract_zone_signals_from_api_scan_result(r, allowed_modules=None)
    chk("three signals",            len(sigs) == 3, str(len(sigs)))

    print()
    print("=" * 60)
    print(f"Results: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    return FAIL == 0


if __name__ == "__main__":
    import sys
    ok = _run_extractor_tests()
    sys.exit(0 if ok else 1)
