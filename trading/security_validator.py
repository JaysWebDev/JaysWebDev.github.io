#!/usr/bin/env python3
"""
Security Status Validation System
Checks if securities with stale prices are still actively traded or delisted
"""

import requests
import json
import time
from datetime import datetime

def validate_security_status(symbol):
    """
    Validate if a security is still actively traded
    Uses multiple data sources for validation
    """

    validation_result = {
        'symbol': symbol,
        'status': 'UNKNOWN',
        'reason': 'Validation pending',
        'last_price': None,
        'volume': 0,
        'market_cap': None,
        'validation_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
        'data_source': 'N/A'
    }

    try:
        # Method 1: Check with Alpha Vantage (free tier)
        # Note: This would require API key in production
        # For demo, we'll simulate the validation logic

        # Method 2: Simple heuristic validation
        validation_result.update(heuristic_validation(symbol))

        return validation_result

    except Exception as e:
        validation_result.update({
            'status': 'ERROR',
            'reason': f'Validation failed: {str(e)}',
            'data_source': 'ERROR'
        })
        return validation_result

def heuristic_validation(symbol):
    """
    Use heuristic patterns to assess security status
    Based on symbol patterns and known delisting indicators
    """

    # Known delisted/acquired patterns
    known_delisted = ['MODG', 'TWTR', 'FB']  # Examples
    penny_stock_threshold = 0.05

    # Load our stale price data for context
    try:
        with open('/tmp/jays_website_deploy/trading/data/stale_price_report.json', 'r') as f:
            stale_data = json.load(f)

        # Find this symbol in stale data
        symbol_data = None
        for sec in stale_data.get('securities', []):
            if sec['symbol'] == symbol:
                symbol_data = sec
                break

        if not symbol_data:
            return {'status': 'ACTIVE', 'reason': 'Not in stale price list', 'data_source': 'Internal'}

        price = symbol_data['price']
        avg_volume = symbol_data['avg_volume']
        consecutive_days = symbol_data['consecutive_days']
        zero_vol_days = symbol_data['zero_volume_days']

        # Classification logic
        if symbol in known_delisted:
            return {
                'status': 'DELISTED',
                'reason': 'Known delisted security',
                'last_price': price,
                'volume': avg_volume,
                'data_source': 'Known List'
            }

        elif zero_vol_days >= 5:
            return {
                'status': 'SUSPENDED',
                'reason': f'{zero_vol_days} days with zero volume',
                'last_price': price,
                'volume': avg_volume,
                'data_source': 'Volume Analysis'
            }

        elif price < 0.001:
            return {
                'status': 'DELISTED',
                'reason': 'Extreme penny stock (< $0.001)',
                'last_price': price,
                'volume': avg_volume,
                'data_source': 'Price Analysis'
            }

        elif price < penny_stock_threshold and avg_volume < 10000:
            return {
                'status': 'AT_RISK',
                'reason': f'Penny stock with low volume (${price:.4f})',
                'last_price': price,
                'volume': avg_volume,
                'data_source': 'Risk Analysis'
            }

        elif consecutive_days >= 10:
            return {
                'status': 'SUSPICIOUS',
                'reason': f'{consecutive_days} days same price',
                'last_price': price,
                'volume': avg_volume,
                'data_source': 'Pattern Analysis'
            }

        else:
            return {
                'status': 'MONITOR',
                'reason': 'Stale price but appears active',
                'last_price': price,
                'volume': avg_volume,
                'data_source': 'Heuristic'
            }

    except Exception as e:
        return {
            'status': 'ERROR',
            'reason': f'Heuristic validation failed: {str(e)}',
            'data_source': 'Error'
        }

def batch_validate_securities():
    """Validate all securities flagged with stale prices"""

    # Load stale price report
    try:
        with open('/tmp/jays_website_deploy/trading/data/stale_price_report.json', 'r') as f:
            stale_data = json.load(f)
    except FileNotFoundError:
        print("❌ No stale price report found. Run stale_price_detector.py first.")
        return []

    stale_securities = stale_data.get('securities', [])

    if not stale_securities:
        print("✅ No stale securities to validate")
        return []

    print(f"🔍 Validating {len(stale_securities)} securities with stale prices...")
    print("=" * 60)

    validation_results = []

    for security in stale_securities:
        symbol = security['symbol']
        print(f"Validating {symbol}...", end=" ")

        result = validate_security_status(symbol)
        validation_results.append(result)

        # Status emoji mapping
        status_emojis = {
            'DELISTED': '🔴',
            'SUSPENDED': '🟠',
            'AT_RISK': '🟡',
            'SUSPICIOUS': '🔵',
            'MONITOR': '🟢',
            'ACTIVE': '✅',
            'ERROR': '❌',
            'UNKNOWN': '❔'
        }

        emoji = status_emojis.get(result['status'], '❔')
        print(f"{emoji} {result['status']} - {result['reason']}")

        # Small delay to be respectful
        time.sleep(0.1)

    # Save validation results
    validation_report = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'total_validated': len(validation_results),
        'summary': generate_validation_summary(validation_results),
        'results': validation_results
    }

    with open('/tmp/jays_website_deploy/trading/data/security_validation.json', 'w') as f:
        json.dump(validation_report, f, indent=2)

    print(f"\n📊 VALIDATION SUMMARY")
    print("=" * 30)
    for status, count in validation_report['summary'].items():
        if count > 0:
            print(f"{status}: {count}")

    return validation_results

def generate_validation_summary(results):
    """Generate summary statistics from validation results"""

    summary = {}
    for result in results:
        status = result['status']
        summary[status] = summary.get(status, 0) + 1

    return summary

if __name__ == "__main__":
    batch_validate_securities()
    print(f"\nValidation report saved to: security_validation.json")