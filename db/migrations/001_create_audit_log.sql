-- Migration: 001_create_audit_log
-- Date: 2026-06-30
-- Description: Create append-only audit_log table for security event recording
-- Note: This table is also auto-created by AuditLogger.ensure_table() on startup

CREATE TABLE IF NOT EXISTS centry.audit_log (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(36) NOT NULL UNIQUE,
    timestamp TIMESTAMPTZ NOT NULL,
    actor VARCHAR(255) NOT NULL,
    action VARCHAR(100) NOT NULL,
    resource VARCHAR(500) NOT NULL,
    details JSONB,
    ip_address VARCHAR(45),
    service VARCHAR(100),
    request_id VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS ix_audit_log_timestamp ON centry.audit_log (timestamp);
CREATE INDEX IF NOT EXISTS ix_audit_log_actor ON centry.audit_log (actor);
CREATE INDEX IF NOT EXISTS ix_audit_log_action ON centry.audit_log (action);
CREATE INDEX IF NOT EXISTS ix_audit_log_actor_timestamp ON centry.audit_log (actor, timestamp);
CREATE INDEX IF NOT EXISTS ix_audit_log_action_timestamp ON centry.audit_log (action, timestamp);

-- Revoke UPDATE and DELETE from application role to enforce append-only
-- (Uncomment and adjust role name for production)
-- REVOKE UPDATE, DELETE ON centry.audit_log FROM centry;
-- GRANT SELECT, INSERT ON centry.audit_log TO centry;

COMMENT ON TABLE centry.audit_log IS 'Append-only audit trail for security-relevant actions. Retention: 90 days, archived to S3.';
