#!/usr/bin/env python3
"""
Security Status Validation System
Validates flagged securities using database data and heuristic analysis.
Reads from SQLite database and stale_price_report.json.
"""

import sqlite3
import json
import os
from datetime import datetime

# Database path
DB_PATH = "/media/j/Extreme SSD/Trading_Plans/Database/trading_central.db"

# Thresholds (must match stale_price_detector.py)
PENNY_STOCK_THRESHOLD = 1.00
EXTREME_PENNY_THRESHOLD = 0.01


def validate_security_status(symbol, stale_data_entry=None):
    """
    Validate if a security is still actively traded.
    Uses SQLite data + heuristic analysis.
    """

    validation_result = {
        'symbol': symbol,
        'status': 'UNKNOWN',
        'reason': 'Validation pending',
        'last_price': None,
        'volume': 0,
        'validation_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'data_source': 'N/A'
    }

    try:
        # Get latest data from SQLite
        if os.path.exists(DB_PATH):
            conn = sqlite3.connect(DB_PATH, timeout=30)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT close, volume, date
                FROM daily_prices
                WHERE symbol = ?
                ORDER BY date DESC
                LIMIT 10
            """, (symbol,))
            rows = cursor.fetchall()
            conn.close()

            if not rows:
                return {**validation_result,
                    'status': 'DELISTED',
                    'reason': 'No price data in database (Yahoo Finance returns no data)',
                    'data_source': 'Data Absence'}

            if rows:
                latest_price = rows[0][0]
                latest_volume = rows[0][1]
                latest_date = rows[0][2]
                avg_volume = sum(r[1] for r in rows) / len(rows)
                zero_vol_days = sum(1 for r in rows if r[1] == 0)

                # Consecutive same-price days (compare adjacent rows)
                consecutive = 1
                max_consecutive = 1
                for i in range(1, len(rows)):
                    if abs(rows[i][0] - rows[i-1][0]) < 0.0001:
                        consecutive += 1
                        if consecutive > max_consecutive:
                            max_consecutive = consecutive
                    else:
                        consecutive = 1
                consecutive = max_consecutive

                validation_result['last_price'] = latest_price
                validation_result['volume'] = avg_volume

                # Classification logic
                if latest_price < 0.001:
                    return {**validation_result,
                        'status': 'DELISTED',
                        'reason': f'Extreme penny stock (${latest_price:.4f})',
                        'data_source': 'Price Analysis'}

                if zero_vol_days >= 5:
                    return {**validation_result,
                        'status': 'SUSPENDED',
                        'reason': f'{zero_vol_days} days with zero volume',
                        'data_source': 'Volume Analysis'}

                if latest_price < EXTREME_PENNY_THRESHOLD:
                    return {**validation_result,
                        'status': 'DELISTED',
                        'reason': f'Price below ${EXTREME_PENNY_THRESHOLD} (${latest_price:.4f})',
                        'data_source': 'Price Analysis'}

                if latest_price < PENNY_STOCK_THRESHOLD and avg_volume < 10000:
                    return {**validation_result,
                        'status': 'AT_RISK',
                        'reason': f'Penny stock with low volume (${latest_price:.2f}, {avg_volume:.0f} avg vol)',
                        'data_source': 'Risk Analysis'}

                if latest_price < PENNY_STOCK_THRESHOLD:
                    return {**validation_result,
                        'status': 'PENNY_STOCK',
                        'reason': f'Price ${latest_price:.2f} below ${PENNY_STOCK_THRESHOLD:.2f} threshold',
                        'data_source': 'Price Analysis'}

                if consecutive >= 10:
                    return {**validation_result,
                        'status': 'SUSPICIOUS',
                        'reason': f'{consecutive} consecutive days same price',
                        'data_source': 'Pattern Analysis'}

                if consecutive >= 3:
                    return {**validation_result,
                        'status': 'MONITOR',
                        'reason': f'Stale price ({consecutive} days) but appears active',
                        'data_source': 'Heuristic'}

                return {**validation_result,
                    'status': 'ACTIVE',
                    'reason': 'Normal trading activity',
                    'data_source': 'Database'}

        # Fallback to stale report data if DB not available
        if stale_data_entry:
            return heuristic_validation(symbol, stale_data_entry)

        return validation_result

    except Exception as e:
        validation_result.update({
            'status': 'ERROR',
            'reason': f'Validation failed: {str(e)}',
            'data_source': 'ERROR'
        })
        return validation_result


def heuristic_validation(symbol, data):
    """Fallback heuristic validation from report data"""
    price = data.get('price', 0)
    avg_volume = data.get('avg_volume', 0)
    consecutive_days = data.get('consecutive_days', 0)

    if price < 0.001:
        return {'status': 'DELISTED', 'reason': f'Extreme penny stock (${price:.4f})', 'data_source': 'Heuristic'}
    if price < PENNY_STOCK_THRESHOLD and avg_volume < 10000:
        return {'status': 'AT_RISK', 'reason': f'Penny stock with low volume', 'data_source': 'Heuristic'}
    if price < PENNY_STOCK_THRESHOLD:
        return {'status': 'PENNY_STOCK', 'reason': f'Price ${price:.2f} below threshold', 'data_source': 'Heuristic'}
    if consecutive_days >= 10:
        return {'status': 'SUSPICIOUS', 'reason': f'{consecutive_days} days same price', 'data_source': 'Heuristic'}
    return {'status': 'MONITOR', 'reason': 'Stale but appears active', 'data_source': 'Heuristic'}


def batch_validate_securities():
    """Validate all securities flagged with stale prices"""

    script_dir = os.path.dirname(os.path.abspath(__file__))
    report_path = os.path.join(script_dir, 'data', 'stale_price_report.json')

    try:
        with open(report_path, 'r') as f:
            stale_data = json.load(f)
    except FileNotFoundError:
        print("No stale price report found. Run stale_price_detector.py first.")
        return []

    stale_securities = stale_data.get('securities', [])

    if not stale_securities:
        print("No flagged securities to validate")
        return []

    print(f"Validating {len(stale_securities)} flagged securities...")
    print("=" * 60)

    validation_results = []

    for security in stale_securities:
        symbol = security['symbol']
        result = validate_security_status(symbol, security)
        validation_results.append(result)

        status_markers = {
            'DELISTED': '[!!]', 'SUSPENDED': '[!]', 'AT_RISK': '[?]',
            'PENNY_STOCK': '[$]', 'SUSPICIOUS': '[~]', 'MONITOR': '[.]',
            'ACTIVE': '[OK]', 'ERROR': '[ERR]', 'UNKNOWN': '[??]'
        }
        marker = status_markers.get(result['status'], '[??]')
        print(f"  {marker} {symbol:6s} {result['status']:12s} - {result['reason']}")

    # Save validation results
    validation_report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_validated': len(validation_results),
        'summary': {},
        'results': validation_results
    }

    # Build summary
    for result in validation_results:
        status = result['status']
        validation_report['summary'][status] = validation_report['summary'].get(status, 0) + 1

    output_path = os.path.join(script_dir, 'data', 'security_validation.json')
    with open(output_path, 'w') as f:
        json.dump(validation_report, f, indent=2)

    print(f"\nVALIDATION SUMMARY")
    print("=" * 30)
    for status, count in sorted(validation_report['summary'].items()):
        print(f"  {status}: {count}")

    return validation_results


if __name__ == "__main__":
    batch_validate_securities()
    print(f"\nValidation report saved to: data/security_validation.json")
