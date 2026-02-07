#!/usr/bin/env python3
"""
Stale Price Detection System for Trading Dashboard
Identifies securities with unchanged prices and validates their trading status
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import os

def detect_stale_prices():
    """Main stale price detection function"""

    # Load database
    df = pd.read_parquet('/home/j/trading-pipeline/data/daily.parquet')

    print(f"📊 Analyzing {len(df):,} records for {df['symbol'].nunique()} securities")

    # Get last 10 trading days
    latest_dates = sorted(df['Date'].unique())[-10:]
    recent_df = df[df['Date'].isin(latest_dates)].copy()

    stale_securities = []

    # Analyze each security for stale price patterns
    for symbol in recent_df['symbol'].unique():
        symbol_data = recent_df[recent_df['symbol'] == symbol].sort_values('Date')

        if len(symbol_data) >= 5:
            closes = symbol_data['Close'].tolist()
            volumes = symbol_data['Volume'].tolist()
            dates = symbol_data['Date'].tolist()

            # Check for 3+ consecutive days with same price
            consecutive_count = 1
            max_consecutive = 1
            stale_price = None
            stale_start_idx = 0

            for i in range(1, len(closes)):
                if abs(closes[i] - closes[i-1]) < 0.0001:  # Same price (accounting for floating point)
                    consecutive_count += 1
                    if consecutive_count > max_consecutive:
                        max_consecutive = consecutive_count
                        stale_price = closes[i]
                        stale_start_idx = i - consecutive_count + 1
                else:
                    consecutive_count = 1

            # Flag securities with 3+ days of same price
            if max_consecutive >= 3:
                stale_vol_window = volumes[stale_start_idx:stale_start_idx + max_consecutive]
                avg_volume = np.mean(stale_vol_window)
                zero_vol_days = sum(1 for v in stale_vol_window if v == 0)

                # Risk assessment
                risk_level = "LOW"
                if zero_vol_days >= 2:
                    risk_level = "HIGH"
                elif avg_volume < 1000:
                    risk_level = "MEDIUM"
                elif stale_price < 0.01:
                    risk_level = "HIGH"

                stale_securities.append({
                    'symbol': symbol,
                    'price': float(stale_price),
                    'consecutive_days': int(max_consecutive),
                    'avg_volume': float(avg_volume),
                    'zero_volume_days': int(zero_vol_days),
                    'risk_level': risk_level,
                    'last_date': str(dates[-1]),
                    'analysis_date': datetime.now().strftime('%Y-%m-%d %H:%M')
                })

    # Sort by risk level and volume
    risk_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    stale_securities.sort(key=lambda x: (risk_order[x['risk_level']], x['avg_volume']))

    return stale_securities

def generate_stale_price_report():
    """Generate comprehensive stale price analysis report"""

    stale_securities = detect_stale_prices()

    # Create summary statistics
    total_stale = len(stale_securities)
    high_risk = len([s for s in stale_securities if s['risk_level'] == 'HIGH'])
    medium_risk = len([s for s in stale_securities if s['risk_level'] == 'MEDIUM'])
    low_risk = len([s for s in stale_securities if s['risk_level'] == 'LOW'])

    # Generate HTML report data
    report_data = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'summary': {
            'total_stale': total_stale,
            'high_risk': high_risk,
            'medium_risk': medium_risk,
            'low_risk': low_risk
        },
        'securities': stale_securities[:50],  # Limit to top 50
        'recommendations': generate_cleanup_recommendations(stale_securities)
    }

    return report_data

def generate_cleanup_recommendations(stale_securities):
    """Generate database cleanup recommendations"""

    recommendations = []

    high_risk = [s for s in stale_securities if s['risk_level'] == 'HIGH']
    medium_risk = [s for s in stale_securities if s['risk_level'] == 'MEDIUM']

    if high_risk:
        recommendations.append({
            'action': 'IMMEDIATE_REVIEW',
            'description': f'Review {len(high_risk)} high-risk securities with zero/minimal volume',
            'symbols': [s['symbol'] for s in high_risk[:10]],
            'priority': 'HIGH'
        })

    if medium_risk:
        recommendations.append({
            'action': 'MONITOR',
            'description': f'Monitor {len(medium_risk)} medium-risk securities for delisting',
            'symbols': [s['symbol'] for s in medium_risk[:10]],
            'priority': 'MEDIUM'
        })

    # Specific recommendations for penny stocks
    penny_stocks = [s for s in stale_securities if s['price'] < 0.01]
    if penny_stocks:
        recommendations.append({
            'action': 'PENNY_STOCK_REVIEW',
            'description': f'Review {len(penny_stocks)} securities under $0.01',
            'symbols': [s['symbol'] for s in penny_stocks],
            'priority': 'MEDIUM'
        })

    return recommendations

if __name__ == "__main__":
    # Generate and save report
    report_data = generate_stale_price_report()

    # Save to JSON for dashboard consumption
    # Save to github_deploy for website deployment
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_path = os.path.join(script_dir, 'data', 'stale_price_report.json')
    with open(output_path, 'w') as f:
        json.dump(report_data, f, indent=2)

    # Print summary
    print("🔍 STALE PRICE DETECTION COMPLETE")
    print("=" * 50)
    print(f"Total securities with stale prices: {report_data['summary']['total_stale']}")
    print(f"High risk: {report_data['summary']['high_risk']}")
    print(f"Medium risk: {report_data['summary']['medium_risk']}")
    print(f"Low risk: {report_data['summary']['low_risk']}")

    if report_data['securities']:
        print(f"\nTop 10 concerns:")
        for i, sec in enumerate(report_data['securities'][:10], 1):
            print(f"{i:2d}. {sec['symbol']} - ${sec['price']:.4f} ({sec['consecutive_days']} days, {sec['risk_level']} risk)")

    print(f"\nReport saved to: stale_price_report.json")