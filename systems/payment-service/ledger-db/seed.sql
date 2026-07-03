-- Generated seed for ledger-db (source of truth: seeds.json). Idempotent: safe to re-run.
-- Mounted into the init dir AFTER init.sql, so a fresh `down -v` rebuild loads the
-- schema and then this chart of accounts automatically.

-- account: the chart of accounts the app resolves by (owner_id, name). ids are fixed
-- small bigints (the app otherwise assigns snowflake ids on insert); ON CONFLICT DO
-- NOTHING skips rows that already exist by primary key or the (owner_id, name) unique.
INSERT INTO account (id, name, type, owner_id, currency) VALUES
  (1001, 'cash',   'asset',     'platform', 'USD'),
  (1002, 'escrow', 'liability', 'platform', 'USD'),
  (1003, 'income', 'income',    'platform', 'USD'),
  (1004, NULL,     'asset',     'psp',      'USD'),
  (1005, NULL,     'liability', 'seller1',  'USD'),
  (1006, NULL,     'liability', 'payout',   'USD')
ON CONFLICT DO NOTHING;
