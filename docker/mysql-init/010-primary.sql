USE app;
INSERT INTO backend_identity(id, marker)
VALUES (1, 'primary')
ON DUPLICATE KEY UPDATE marker = VALUES(marker);
