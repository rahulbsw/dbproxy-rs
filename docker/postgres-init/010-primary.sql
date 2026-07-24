INSERT INTO backend_identity(id, marker)
VALUES (1, 'primary')
ON CONFLICT (id) DO UPDATE SET marker = EXCLUDED.marker;
