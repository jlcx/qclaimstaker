-- Hand-curated inverse pairs for known-but-undeclared or ambiguous cases.
-- Each pair is stored both directions so lookups are symmetric.
-- `load-inverses` will additionally harvest P1696 claims from wd_properties.

INSERT INTO inverse_properties (pid_a, pid_b, source) VALUES
  ('P527', 'P361', 'seed'),   -- has part / part of
  ('P361', 'P527', 'seed'),
  ('P40',  'P22',  'seed'),   -- child / father
  ('P22',  'P40',  'seed'),
  ('P40',  'P25',  'seed'),   -- child / mother
  ('P25',  'P40',  'seed'),
  ('P1038','P1038','seed'),   -- relative (symmetric)
  ('P26',  'P26',  'seed'),   -- spouse
  ('P3373','P3373','seed'),   -- sibling
  ('P451', 'P451', 'seed'),   -- unmarried partner
  ('P460', 'P460', 'seed'),   -- said to be the same as
  ('P1889','P1889','seed'),   -- different from
  ('P1365','P1366','seed'),   -- replaces / replaced by
  ('P1366','P1365','seed'),
  ('P155', 'P156', 'seed'),   -- follows / followed by
  ('P156', 'P155', 'seed')
ON CONFLICT DO NOTHING;
