-- Meta-types to exclude on either side of a pair (§4).
-- Tuned from v1 experience; grow as noise is discovered.

INSERT INTO meta_type_blocklist (qid, reason) VALUES
  ('Q4167410',  'Wikimedia disambiguation page'),
  ('Q4167836',  'Wikimedia category'),
  ('Q13406463', 'Wikimedia list article'),
  ('Q11266439', 'Wikimedia template'),
  ('Q13442814', 'scholarly article'),
  ('Q15184295', 'Wikimedia module'),
  ('Q15407973', 'Wikimedia disambiguation category'),
  ('Q17633526', 'Wikinews article'),
  ('Q15647814', 'Wikimedia project page'),
  ('Q22808320', 'Wikimedia disambiguation page (draft)'),
  ('Q20010800', 'Wikimedia user language category'),
  ('Q20769160', 'Wikimedia set index article'),
  ('Q14204246', 'Wikimedia project'),
  ('Q17524420', 'aspect of history'),
  ('Q577',      'year'),
  ('Q3186692',  'calendar year'),
  ('Q41825',    'leap year'),
  ('Q29964144', 'year BC'),
  ('Q14795564', 'point in time with respect to recurrent timeframe'),
  ('Q1620908',  'historical period'),
  ('Q24575110', 'calendar date')
ON CONFLICT DO NOTHING;

-- source_blocklist / destination_blocklist are populated as noise is found.
-- Examples carried forward from v1:
INSERT INTO source_blocklist (qid, reason) VALUES
  ('Q5107',  'continent — too generic as src'),
  ('Q2221906','geographic region — too generic as src')
ON CONFLICT DO NOTHING;

INSERT INTO destination_blocklist (qid, reason) VALUES
  ('Q2221906','geographic region — too generic as dst')
ON CONFLICT DO NOTHING;
