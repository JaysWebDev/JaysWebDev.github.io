#!/usr/bin/env python3
"""
Stale Price Detection System for Trading Dashboard
Identifies securities with unchanged prices, penny stocks, and data quality issues.
Reads from SQLite database (trading_central.db on Extreme SSD).

Features:
- Business-day-only execution (Mon-Fri)
- Persistent failure tracking (5 consecutive business days before flagging)
- No-data detection for delisted/removed symbols
"""

import sqlite3
import numpy as np
from datetime import datetime, date
import json
import os

# Database path (same as pipeline config)
DB_PATH = "/media/j/Extreme SSD/Trading_Plans/Database/trading_central.db"

# Watchlist path (primary symbol universe)
WATCHLIST_PATH = "/home/j/trading-pipeline/watchlists/my_main_512.txt"

# Thresholds
PENNY_STOCK_THRESHOLD = 1.00       # Flag stocks under $1.00
EXTREME_PENNY_THRESHOLD = 0.01     # Likely delisted/worthless
STALE_DAYS_THRESHOLD = 5           # Consecutive same-price days to flag
LOW_VOLUME_THRESHOLD = 1000        # Daily avg volume
PRICE_TOLERANCE = 0.0001           # Float comparison tolerance
CONSECUTIVE_FAILURES_REQUIRED = 5  # Business days a symbol must fail before appearing in report

# State file for persistent failure tracking
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(SCRIPT_DIR, 'data', 'stale_tracking_state.json')


def get_db_connection():
    """Connect to SQLite database"""
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"Database not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def load_watchlist():
    """Load symbol list from primary watchlist file"""
    if not os.path.exists(WATCHLIST_PATH):
        return set()
    with open(WATCHLIST_PATH) as f:
        return set(line.strip().upper() for line in f
                   if line.strip() and not line.strip().startswith('#'))


def load_tracking_state():
    """Load persistent failure tracking state"""
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"last_run": None, "failure_counts": {}}


def save_tracking_state(state):
    """Save persistent failure tracking state"""
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=2)


def detect_stale_prices():
    """Main stale price detection function using SQLite.
    Returns all flagged securities (before filtering by consecutive failures).
    """

    # Business-day guard
    today = date.today()
    if today.weekday() >= 5:  # Saturday=5, Sunday=6
        print("Skipping stale price detection - not a business day")
        return []

    conn = get_db_connection()
    cursor = conn.cursor()

    # Get last 10 trading dates
    cursor.execute("SELECT DISTINCT date FROM daily_prices ORDER BY date DESC LIMIT 10")
    recent_dates = [row[0] for row in cursor.fetchall()]

    if len(recent_dates) < 5:
        print("Not enough recent data for stale price detection")
        conn.close()
        return []

    earliest_date = min(recent_dates)

    # Get all symbols with recent data
    cursor.execute("""
        SELECT symbol, date, close, volume
        FROM daily_prices
        WHERE date >= ?
        ORDER BY symbol, date
    """, (earliest_date,))

    rows = cursor.fetchall()
    conn.close()

    # Group by symbol
    symbol_data = {}
    for symbol, dt, close, volume in rows:
        if symbol not in symbol_data:
            symbol_data[symbol] = {'dates': [], 'closes': [], 'volumes': []}
        symbol_data[symbol]['dates'].append(dt)
        symbol_data[symbol]['closes'].append(close)
        symbol_data[symbol]['volumes'].append(volume)

    total_symbols = len(symbol_data)
    print(f"Analyzing {total_symbols} securities for stale prices and penny stocks")

    stale_securities = []

    for symbol, data in symbol_data.items():
        if len(data['closes']) < 5:
            continue

        closes = data['closes']
        volumes = data['volumes']
        dates = data['dates']
        last_price = closes[-1]
        avg_volume = np.mean(volumes)

        # --- Check 1: Consecutive same-price days ---
        consecutive_count = 1
        max_consecutive = 1

        for i in range(1, len(closes)):
            if abs(closes[i] - closes[i-1]) < PRICE_TOLERANCE:
                consecutive_count += 1
                if consecutive_count > max_consecutive:
                    max_consecutive = consecutive_count
            else:
                consecutive_count = 1

        is_stale = max_consecutive >= STALE_DAYS_THRESHOLD

        # --- Check 2: Penny stock (under $1.00) ---
        is_penny = last_price < PENNY_STOCK_THRESHOLD
        is_extreme_penny = last_price < EXTREME_PENNY_THRESHOLD

        # Only flag if stale OR penny stock
        if not is_stale and not is_penny:
            continue

        # Risk assessment
        zero_vol_days = sum(1 for v in volumes if v == 0)

        if is_extreme_penny or zero_vol_days >= 2:
            risk_level = "HIGH"
        elif is_penny and avg_volume < LOW_VOLUME_THRESHOLD:
            risk_level = "HIGH"
        elif is_penny:
            risk_level = "MEDIUM"
        elif is_stale and avg_volume < LOW_VOLUME_THRESHOLD:
            risk_level = "MEDIUM"
        else:
            risk_level = "LOW"

        # Determine flag reason
        reasons = []
        if is_extreme_penny:
            reasons.append(f"Extreme penny stock (${last_price:.4f})")
        elif is_penny:
            reasons.append(f"Penny stock (${last_price:.2f} < ${PENNY_STOCK_THRESHOLD:.2f})")
        if is_stale:
            reasons.append(f"{max_consecutive} consecutive days same price")
        if zero_vol_days >= 2:
            reasons.append(f"{zero_vol_days} zero-volume days")

        stale_securities.append({
            'symbol': symbol,
            'price': float(last_price),
            'consecutive_days': int(max_consecutive) if is_stale else 0,
            'avg_volume': float(avg_volume),
            'zero_volume_days': int(zero_vol_days),
            'risk_level': risk_level,
            'reasons': reasons,
            'is_penny_stock': is_penny,
            'is_stale_price': is_stale,
            'is_no_data': False,
            'last_date': str(dates[-1]),
            'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M')
        })

    # --- Check 3: No-data detection (delisted symbols) ---
    watchlist_symbols = load_watchlist()
    if watchlist_symbols:
        # Symbols in watchlist but with no recent DB data
        db_symbols = set(symbol_data.keys())
        # Use 5th most recent date as cutoff for "no recent data"
        recent_cutoff = recent_dates[4] if len(recent_dates) > 4 else recent_dates[-1]

        for symbol in watchlist_symbols:
            if symbol not in db_symbols:
                stale_securities.append({
                    'symbol': symbol,
                    'price': 0.0,
                    'consecutive_days': 0,
                    'avg_volume': 0.0,
                    'zero_volume_days': 0,
                    'risk_level': 'HIGH',
                    'reasons': ['No recent data from Yahoo Finance (possibly delisted)'],
                    'is_penny_stock': False,
                    'is_stale_price': False,
                    'is_no_data': True,
                    'last_date': 'N/A',
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M')
                })
            elif symbol in db_symbols:
                # Check if symbol's latest date is way behind (data stopped updating)
                sym_latest = symbol_data[symbol]['dates'][-1]
                if sym_latest < recent_cutoff:
                    stale_securities.append({
                        'symbol': symbol,
                        'price': float(symbol_data[symbol]['closes'][-1]),
                        'consecutive_days': 0,
                        'avg_volume': float(np.mean(symbol_data[symbol]['volumes'])),
                        'zero_volume_days': 0,
                        'risk_level': 'HIGH',
                        'reasons': [f'Data stopped updating (last: {sym_latest}, expected: {recent_dates[0]})'],
                        'is_penny_stock': False,
                        'is_stale_price': False,
                        'is_no_data': True,
                        'last_date': str(sym_latest),
                        'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M')
                    })

    # Sort by risk level then volume
    risk_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    stale_securities.sort(key=lambda x: (risk_order[x['risk_level']], x['avg_volume']))

    return stale_securities


def apply_failure_tracking(stale_securities):
    """Apply persistent failure tracking. Securities must be flagged for
    CONSECUTIVE_FAILURES_REQUIRED business days before appearing in the report.
    Returns (confirmed_securities, tracking_state)."""

    state = load_tracking_state()
    today_str = date.today().isoformat()
    flagged_symbols = {s['symbol'] for s in stale_securities}

    # Update failure counts
    new_counts = {}
    for sec in stale_securities:
        sym = sec['symbol']
        prev = state['failure_counts'].get(sym, {})
        new_counts[sym] = {
            'count': prev.get('count', 0) + 1,
            'first_flagged': prev.get('first_flagged', today_str),
            'reason': '; '.join(sec['reasons'])
        }

    # Reset symbols that recovered (were tracked but not flagged today)
    for sym in state['failure_counts']:
        if sym not in flagged_symbols:
            # Don't delete — set to 0 so we can see it recovered
            new_counts.setdefault(sym, {
                'count': 0,
                'first_flagged': state['failure_counts'][sym].get('first_flagged', ''),
                'reason': 'Recovered'
            })

    # Save updated state
    state['last_run'] = today_str
    state['failure_counts'] = new_counts
    save_tracking_state(state)

    # Filter: only return securities with enough consecutive failures
    confirmed = [s for s in stale_securities
                 if new_counts.get(s['symbol'], {}).get('count', 0) >= CONSECUTIVE_FAILURES_REQUIRED]

    # Add failure count info to each confirmed security
    for sec in confirmed:
        sec['consecutive_failures'] = new_counts[sec['symbol']]['count']
        sec['first_flagged'] = new_counts[sec['symbol']]['first_flagged']

    return confirmed, state


def generate_stale_price_report():
    """Generate comprehensive stale price analysis report"""

    all_flagged = detect_stale_prices()

    if not all_flagged and date.today().weekday() >= 5:
        # Weekend skip — return empty report
        return None

    # Apply failure tracking — only confirmed securities go in the report
    confirmed_securities, tracking_state = apply_failure_tracking(all_flagged)

    total_flagged = len(confirmed_securities)
    high_risk = len([s for s in confirmed_securities if s['risk_level'] == 'HIGH'])
    medium_risk = len([s for s in confirmed_securities if s['risk_level'] == 'MEDIUM'])
    low_risk = len([s for s in confirmed_securities if s['risk_level'] == 'LOW'])
    penny_count = len([s for s in confirmed_securities if s['is_penny_stock']])
    stale_count = len([s for s in confirmed_securities if s['is_stale_price']])
    no_data_count = len([s for s in confirmed_securities if s.get('is_no_data')])

    # Count how many are being tracked but not yet confirmed
    tracking_count = sum(1 for v in tracking_state['failure_counts'].values()
                        if 0 < v['count'] < CONSECUTIVE_FAILURES_REQUIRED)

    report_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'summary': {
            'total_flagged': total_flagged,
            'high_risk': high_risk,
            'medium_risk': medium_risk,
            'low_risk': low_risk,
            'penny_stocks': penny_count,
            'stale_prices': stale_count,
            'no_data': no_data_count,
            'tracking_pending': tracking_count,
            'penny_stock_threshold': PENNY_STOCK_THRESHOLD,
            'failures_required': CONSECUTIVE_FAILURES_REQUIRED
        },
        'securities': confirmed_securities[:50],
        'recommendations': generate_cleanup_recommendations(confirmed_securities)
    }

    return report_data


def generate_cleanup_recommendations(stale_securities):
    """Generate database cleanup recommendations"""

    recommendations = []

    high_risk = [s for s in stale_securities if s['risk_level'] == 'HIGH']
    penny_stocks = [s for s in stale_securities if s['is_penny_stock']]
    stale_only = [s for s in stale_securities if s['is_stale_price'] and not s['is_penny_stock']]
    no_data = [s for s in stale_securities if s.get('is_no_data')]

    if no_data:
        recommendations.append({
            'action': 'REMOVE_NO_DATA',
            'description': f'Remove {len(no_data)} securities with no Yahoo Finance data (likely delisted)',
            'symbols': [s['symbol'] for s in no_data[:10]],
            'priority': 'HIGH'
        })

    if high_risk:
        recommendations.append({
            'action': 'IMMEDIATE_REVIEW',
            'description': f'Review {len(high_risk)} high-risk securities (likely delisted or worthless)',
            'symbols': [s['symbol'] for s in high_risk[:10]],
            'priority': 'HIGH'
        })

    if penny_stocks:
        recommendations.append({
            'action': 'PENNY_STOCK_REVIEW',
            'description': f'Review {len(penny_stocks)} securities under ${PENNY_STOCK_THRESHOLD:.2f}',
            'symbols': [s['symbol'] for s in penny_stocks[:10]],
            'priority': 'MEDIUM'
        })

    if stale_only:
        recommendations.append({
            'action': 'STALE_PRICE_MONITOR',
            'description': f'Monitor {len(stale_only)} securities with unchanged prices',
            'symbols': [s['symbol'] for s in stale_only[:10]],
            'priority': 'LOW'
        })

    return recommendations


if __name__ == "__main__":
    report_data = generate_stale_price_report()

    if report_data is None:
        print("No report generated (weekend or no data)")
        exit(0)

    # Save to JSON for dashboard consumption
    output_path = os.path.join(SCRIPT_DIR, 'data', 'stale_price_report.json')
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(report_data, f, indent=2)

    # Print summary
    print("STALE PRICE & PENNY STOCK DETECTION COMPLETE")
    print("=" * 50)
    print(f"Total confirmed (>={CONSECUTIVE_FAILURES_REQUIRED} days): {report_data['summary']['total_flagged']}")
    print(f"  High risk: {report_data['summary']['high_risk']}")
    print(f"  Medium risk: {report_data['summary']['medium_risk']}")
    print(f"  Low risk: {report_data['summary']['low_risk']}")
    print(f"  Penny stocks (<${PENNY_STOCK_THRESHOLD:.2f}): {report_data['summary']['penny_stocks']}")
    print(f"  Stale prices: {report_data['summary']['stale_prices']}")
    print(f"  No data (delisted): {report_data['summary']['no_data']}")
    print(f"  Tracking (not yet confirmed): {report_data['summary']['tracking_pending']}")

    if report_data['securities']:
        print(f"\nConfirmed flagged securities:")
        for i, sec in enumerate(report_data['securities'][:15], 1):
            reasons = '; '.join(sec['reasons'])
            days = sec.get('consecutive_failures', '?')
            print(f"{i:2d}. {sec['symbol']:6s} ${sec['price']:<8.4f} [{sec['risk_level']}] ({days}d) {reasons}")

    print(f"\nReport saved to: {output_path}")
