"""
verify_live_db.py — Phase 2B: Live Neon DB insert verification
Run from Koyeb shell (where DATABASE_URL is set):

  python3 verify_live_db.py

Safe: only touches pair=TESTUSDT, source=test.
Test rows are deleted at the end unless deletion fails.
"""

import os
import sys

db_url = os.environ.get("DATABASE_URL", "")
if not db_url:
    print("ERROR: DATABASE_URL not set in this environment.")
    print("Run this script from the Koyeb shell, or set DATABASE_URL first.")
    sys.exit(1)

print("DATABASE_URL found — loading Flask app...")

try:
    from main import app
except Exception as e:
    print(f"ERROR: Could not import Flask app: {e}")
    sys.exit(1)

from signal_logger import log_normalized_signal
from models import db, SignalEvent, SignalOutcome
from sqlalchemy import text as _t

_TEST_SIGNAL = {
    "pair":           "TESTUSDT",
    "module":         "ob",
    "timeframe":      "1h",
    "direction":      "bullish",
    "score":          80,
    "zone_high":      100.0,
    "zone_low":       95.0,
    "detected_price": 110.0,
    "setup_type":     "OB_APPROACH",
    "raw_setup":      "OB_APPROACH",
    "raw_meta_json":  '{"test": true}',
}

PASS = FAIL = 0

def chk(label, condition, detail=""):
    global PASS, FAIL
    if condition:
        print(f"  ✓  {label}")
        PASS += 1
    else:
        print(f"  ✗  {label}" + (f" — {detail}" if detail else ""))
        FAIL += 1

print()
print("=" * 60)
print("Phase 2B — Live DB insert verification")
print("=" * 60)

with app.app_context():

    # ── 1. Migration column check ─────────────────────────────────────────
    print("\n[1] Migration column verification")
    try:
        col_rows = db.session.execute(_t(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='signal_events' "
            "AND column_name IN ('setup_type','raw_setup','raw_meta_json')"
        )).fetchall()
        found_cols = {r[0] for r in col_rows}
        for col in ("setup_type", "raw_setup", "raw_meta_json"):
            chk(f"column '{col}' exists", col in found_cols)
    except Exception as e:
        chk("migration column query", False, str(e))

    # ── 2. First insert ───────────────────────────────────────────────────
    print("\n[2] First insert (source=test)")
    r1 = log_normalized_signal(_TEST_SIGNAL, source="test")
    print(f"     result: {r1}")
    chk("logged=True",          r1.get("logged") is True, str(r1))
    chk("signal_id present",    bool(r1.get("signal_id")), str(r1))

    # ── 3. Duplicate insert ───────────────────────────────────────────────
    print("\n[3] Duplicate insert (same signal, same candle bucket)")
    r2 = log_normalized_signal(_TEST_SIGNAL, source="test")
    print(f"     result: {r2}")
    chk("logged=False",         r2.get("logged") is False, str(r2))
    chk("reason=duplicate",     r2.get("reason") == "duplicate", str(r2))
    chk("signal_id echoed back",bool(r2.get("signal_id")), str(r2))

    # ── 4. Row verification ───────────────────────────────────────────────
    signal_id = r1.get("signal_id") or r2.get("signal_id")
    event   = None
    outcome = None

    print("\n[4] SignalEvent row check")
    if signal_id:
        event = SignalEvent.query.filter_by(signal_id=signal_id).first()
        chk("SignalEvent row exists",       event is not None)
        chk("pair   = TESTUSDT",            event.pair      == "TESTUSDT"     if event else False)
        chk("module = ob",                  event.module    == "ob"           if event else False)
        chk("status = WAITING_FOR_ENTRY",   event.status    == "WAITING_FOR_ENTRY" if event else False)
        chk("source = test",                event.source    == "test"         if event else False)
        chk("setup_type = OB_APPROACH",     event.setup_type == "OB_APPROACH" if event else False)
        chk("raw_setup stored",             bool(event.raw_setup)             if event else False)
        chk("raw_meta_json stored",         bool(event.raw_meta_json)         if event else False)
        if event:
            print(f"     signal_id:     {event.signal_id}")
            print(f"     setup_type:    {event.setup_type}")
            print(f"     raw_meta_json: {event.raw_meta_json}")

    print("\n[5] SignalOutcome row check")
    if signal_id:
        outcome = SignalOutcome.query.filter_by(signal_id=signal_id).first()
        chk("SignalOutcome row exists",          outcome is not None)
        chk("target_price IS NULL",              outcome.target_price is None  if outcome else False,
            f"got {outcome.target_price if outcome else '?'}")
        chk("stop_price IS NULL",                outcome.stop_price   is None  if outcome else False,
            f"got {outcome.stop_price   if outcome else '?'}")
        chk("bounce_threshold_pct = 0.008 (1h)", outcome.bounce_threshold_pct == 0.008 if outcome else False,
            f"got {outcome.bounce_threshold_pct if outcome else '?'}")
        chk("entry_price IS NULL",               outcome.entry_price  is None  if outcome else False)
        chk("result IS NULL",                    outcome.result       is None  if outcome else False)

    # ── 5. Clean up ───────────────────────────────────────────────────────
    print("\n[6] Cleanup — deleting TESTUSDT test rows")
    try:
        if outcome:
            db.session.delete(outcome)
        if event:
            db.session.delete(event)
        db.session.commit()
        chk("Test rows deleted (pair=TESTUSDT source=test)", True)
    except Exception as del_err:
        db.session.rollback()
        chk("Cleanup", False, str(del_err))
        print("     Rows left in DB with source='test' — safe to ignore or delete manually:")
        print(f"     DELETE FROM signal_outcomes WHERE signal_id='{signal_id}';")
        print(f"     DELETE FROM signal_events   WHERE signal_id='{signal_id}';")

    # ── Summary ───────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print(f"Results: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    sys.exit(0 if FAIL == 0 else 1)
