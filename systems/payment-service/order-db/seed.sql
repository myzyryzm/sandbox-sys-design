-- Generated seed for order-db (source of truth: seeds.json). Idempotent: safe to re-run.
-- Mounted into the init dir AFTER init.sql, so a fresh `down -v` rebuild loads the
-- schema and then these rows automatically. FK-safe order: order + item, then order_item.

INSERT INTO "order" (id) VALUES ('order1') ON CONFLICT DO NOTHING;

INSERT INTO item (id, seller_id, price, currency) VALUES
  ('item1', 'seller1', 100, 'USD')
ON CONFLICT DO NOTHING;

INSERT INTO order_item (order_id, item_id) VALUES
  ('order1', 'item1')
ON CONFLICT DO NOTHING;
