-- Database Cleanup Script
-- Generated: 2026-04-17 07:00:03
-- CAUTION: Review before executing

-- Backup delisted securities data before removal
CREATE TABLE IF NOT EXISTS deleted_securities_backup AS
SELECT * FROM daily_prices WHERE symbol IN ('TIPS', 'VIX');

-- Remove delisted securities from main table
-- DELETE FROM daily_prices WHERE symbol IN ('TIPS', 'VIX');

-- Note: Uncomment the DELETE statement above after reviewing the backup

-- Statistics after cleanup:
-- SELECT COUNT(*) as remaining_records FROM daily_prices;
-- SELECT COUNT(DISTINCT symbol) as remaining_securities FROM daily_prices;
