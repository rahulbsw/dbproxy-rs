INSERT INTO backend_identity(id, marker)
VALUES (1, 'replica')
ON CONFLICT (id) DO UPDATE SET marker = EXCLUDED.marker;
