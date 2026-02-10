#!/usr/bin/env python3
"""
Database Cleanup Recommendations System
Provides actionable recommendations for database maintenance based on stale price analysis
"""

import json
import os
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REMOVAL_LOG_PATH = os.path.join(SCRIPT_DIR, 'data', 'removal_log.json')


def load_removal_log():
    """Load existing removal log or create empty one"""
    if os.path.exists(REMOVAL_LOG_PATH):
        with open(REMOVAL_LOG_PATH) as f:
            return json.load(f)
    return {"last_updated": None, "removals": []}


def log_removal(symbol, reason, status, last_price=0.0, watchlist="my_main_512.txt"):
    """Append a removal entry to the persistent removal log"""
    log = load_removal_log()

    # Don't duplicate if already logged
    existing_symbols = {r['symbol'] for r in log['removals']}
    if symbol in existing_symbols:
        return

    log['removals'].append({
        'symbol': symbol,
        'date': datetime.now().strftime('%Y-%m-%d'),
        'reason': reason,
        'status': status,
        'last_price': last_price,
        'watchlist': watchlist
    })
    log['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    os.makedirs(os.path.dirname(REMOVAL_LOG_PATH), exist_ok=True)
    with open(REMOVAL_LOG_PATH, 'w') as f:
        json.dump(log, f, indent=2)


def log_removals_from_validation(validation_data):
    """Auto-log confirmed DELISTED securities from validation results"""
    delisted = [r for r in validation_data.get('results', []) if r['status'] == 'DELISTED']
    for sec in delisted:
        log_removal(
            symbol=sec['symbol'],
            reason=sec.get('reason', 'Confirmed delisted'),
            status='DELISTED',
            last_price=sec.get('last_price', 0.0)
        )

def generate_cleanup_recommendations():
    """Generate comprehensive database cleanup recommendations"""

    # Load analysis data
    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        with open(os.path.join(script_dir, 'data/stale_price_report.json'), 'r') as f:
            stale_data = json.load(f)
    except FileNotFoundError:
        print("Run stale_price_detector.py first")
        return

    try:
        with open(os.path.join(script_dir, 'data/security_validation.json'), 'r') as f:
            validation_data = json.load(f)
    except FileNotFoundError:
        print("Run security_validator.py first")
        return

    # Auto-log confirmed delisted securities
    log_removals_from_validation(validation_data)

    print("🔧 DATABASE CLEANUP RECOMMENDATIONS")
    print("=" * 60)

    recommendations = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'analysis_summary': {
            'total_securities': 847,  # From database
            'stale_securities': stale_data['summary']['total_flagged'],
            'validated_securities': validation_data['total_validated']
        },
        'cleanup_actions': [],
        'maintenance_schedule': [],
        'risk_assessment': {}
    }

    # Generate specific cleanup actions
    cleanup_actions = []

    # 1. Delisted securities cleanup
    delisted_securities = [r for r in validation_data['results'] if r['status'] == 'DELISTED']
    if delisted_securities:
        cleanup_actions.append({
            'priority': 'HIGH',
            'action': 'REMOVE_DELISTED',
            'description': f'Remove {len(delisted_securities)} confirmed delisted securities',
            'affected_symbols': [s['symbol'] for s in delisted_securities],
            'estimated_cleanup_time': '15 minutes',
            'data_savings': f'~{len(delisted_securities) * 200} records'
        })

    # 2. Suspended securities review
    suspended_securities = [r for r in validation_data['results'] if r['status'] == 'SUSPENDED']
    if suspended_securities:
        cleanup_actions.append({
            'priority': 'MEDIUM',
            'action': 'REVIEW_SUSPENDED',
            'description': f'Review {len(suspended_securities)} suspended securities',
            'affected_symbols': [s['symbol'] for s in suspended_securities],
            'estimated_cleanup_time': '30 minutes',
            'recommendation': 'Monitor for 30 days, then remove if still suspended'
        })

    # 3. Penny stock cleanup (under $1.00)
    penny_stocks = [r for r in validation_data['results'] if r['status'] in ('AT_RISK', 'PENNY_STOCK')]
    if penny_stocks:
        cleanup_actions.append({
            'priority': 'MEDIUM',
            'action': 'PENNY_STOCK_REVIEW',
            'description': f'Review {len(penny_stocks)} securities under $1.00',
            'affected_symbols': [s['symbol'] for s in penny_stocks],
            'estimated_cleanup_time': '45 minutes',
            'recommendation': 'Consider removing from watchlist if consistently under $1.00'
        })

    # 4. Data quality improvements
    cleanup_actions.append({
        'priority': 'MEDIUM',
        'action': 'DATA_QUALITY_CHECK',
        'description': 'Implement automated stale price monitoring',
        'estimated_cleanup_time': '2 hours',
        'recommendation': 'Add daily automated checks for stale prices'
    })

    # 5. General maintenance
    cleanup_actions.append({
        'priority': 'LOW',
        'action': 'GENERAL_MAINTENANCE',
        'description': 'Optimize database structure and indexing',
        'estimated_cleanup_time': '1 hour',
        'recommendation': 'Monthly database optimization'
    })

    recommendations['cleanup_actions'] = cleanup_actions

    # Generate maintenance schedule
    maintenance_schedule = [
        {
            'frequency': 'DAILY',
            'task': 'Run stale price detection',
            'automation': 'Automated',
            'estimated_time': '5 minutes'
        },
        {
            'frequency': 'WEEKLY',
            'task': 'Review flagged securities',
            'automation': 'Manual',
            'estimated_time': '30 minutes'
        },
        {
            'frequency': 'MONTHLY',
            'task': 'Database optimization and cleanup',
            'automation': 'Semi-automated',
            'estimated_time': '2 hours'
        },
        {
            'frequency': 'QUARTERLY',
            'task': 'Comprehensive data quality audit',
            'automation': 'Manual',
            'estimated_time': '4 hours'
        }
    ]

    recommendations['maintenance_schedule'] = maintenance_schedule

    # Risk assessment
    total_stale = stale_data['summary']['total_flagged']
    total_securities = 847

    risk_level = "LOW"
    if total_stale > 50:
        risk_level = "HIGH"
    elif total_stale > 10:
        risk_level = "MEDIUM"

    risk_assessment = {
        'overall_risk': risk_level,
        'stale_percentage': round((total_stale / total_securities) * 100, 2),
        'data_quality_score': max(0, 100 - (total_stale * 2)),  # Simple scoring
        'recommendations_urgency': get_urgency_level(cleanup_actions)
    }

    recommendations['risk_assessment'] = risk_assessment

    # Save recommendations
    with open(os.path.join(script_dir, 'data/cleanup_recommendations.json'), 'w') as f:
        json.dump(recommendations, f, indent=2)

    # Display summary
    display_recommendations_summary(recommendations)

    return recommendations

def get_urgency_level(cleanup_actions):
    """Determine overall urgency based on cleanup actions"""

    high_priority_count = len([a for a in cleanup_actions if a.get('priority') == 'HIGH'])

    if high_priority_count > 0:
        return 'IMMEDIATE'
    elif len(cleanup_actions) > 3:
        return 'MODERATE'
    else:
        return 'LOW'

def display_recommendations_summary(recommendations):
    """Display formatted recommendations summary"""

    print(f"\n📈 DATA QUALITY ASSESSMENT")
    print("-" * 40)
    risk = recommendations['risk_assessment']
    print(f"Overall Risk Level: {risk['overall_risk']}")
    print(f"Stale Price Percentage: {risk['stale_percentage']}%")
    print(f"Data Quality Score: {risk['data_quality_score']}/100")

    print(f"\n🎯 PRIORITY ACTIONS")
    print("-" * 40)

    high_priority = [a for a in recommendations['cleanup_actions'] if a.get('priority') == 'HIGH']
    medium_priority = [a for a in recommendations['cleanup_actions'] if a.get('priority') == 'MEDIUM']

    if high_priority:
        print("🔴 HIGH PRIORITY:")
        for action in high_priority:
            print(f"   • {action['description']}")
            if action.get('affected_symbols'):
                symbols = ', '.join(action['affected_symbols'][:5])
                if len(action['affected_symbols']) > 5:
                    symbols += f" (+{len(action['affected_symbols'])-5} more)"
                print(f"     Symbols: {symbols}")

    if medium_priority:
        print("\n🟡 MEDIUM PRIORITY:")
        for action in medium_priority:
            print(f"   • {action['description']}")

    print(f"\n⏰ RECOMMENDED MAINTENANCE SCHEDULE")
    print("-" * 40)
    for schedule in recommendations['maintenance_schedule']:
        print(f"{schedule['frequency']:<12}: {schedule['task']} ({schedule['estimated_time']})")

    print(f"\n💾 CLEANUP SCRIPT GENERATION")
    print("-" * 40)
    print("Run the following to generate cleanup scripts:")
    print("   python3 generate_cleanup_scripts.py")

def create_cleanup_sql_script():
    """Generate SQL cleanup script based on recommendations"""

    script_dir = os.path.dirname(os.path.abspath(__file__))
    try:
        with open(os.path.join(script_dir, 'data/security_validation.json'), 'r') as f:
            validation_data = json.load(f)
    except FileNotFoundError:
        print("❌ No validation data found")
        return

    # Generate SQL for delisted securities
    delisted = [r['symbol'] for r in validation_data['results'] if r['status'] == 'DELISTED']

    if delisted:
        sql_script = f"""-- Database Cleanup Script
-- Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
-- CAUTION: Review before executing

-- Backup delisted securities data before removal
CREATE TABLE IF NOT EXISTS deleted_securities_backup AS
SELECT * FROM daily_prices WHERE symbol IN ({', '.join([f"'{s}'" for s in delisted])});

-- Remove delisted securities from main table
-- DELETE FROM daily_prices WHERE symbol IN ({', '.join([f"'{s}'" for s in delisted])});

-- Note: Uncomment the DELETE statement above after reviewing the backup

-- Statistics after cleanup:
-- SELECT COUNT(*) as remaining_records FROM daily_prices;
-- SELECT COUNT(DISTINCT symbol) as remaining_securities FROM daily_prices;
"""

        with open(os.path.join(script_dir, 'data/cleanup_script.sql'), 'w') as f:
            f.write(sql_script)

        print(f"📄 SQL cleanup script saved to: cleanup_script.sql")

if __name__ == "__main__":
    generate_cleanup_recommendations()
    create_cleanup_sql_script()
    print(f"\n✅ Cleanup recommendations saved to: cleanup_recommendations.json")