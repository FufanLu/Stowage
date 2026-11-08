# Security: failed logins and permission errors

**Alert:** `StowageAuthFailures`: failed logins (`28P01`) or permission errors
(`42501`) more than once every five seconds, for five minutes.

## What it means

Either a client is misconfigured (a rotated password, a new service without
grants) or someone is guessing passwords or probing tables.

## Steps

1. Read the audit log (`stowd --audit-log`, one JSON line per event):
   ```text
   docker compose exec n1 sh -c 'grep -E "login_failed|permission_denied" /data/audit.log | tail -20'
   {"ts_us":…,"event":"login_failed","user":"app","detail":"SCRAM-SHA-256"}
   ```
   Many `login_failed` for one user from a known service: its password changed.
   Failed logins for users that do not exist, or many different users: guessing.
2. Permission errors name the user and the table. Grant what the service needs,
   no more:
   ```sql
   GRANT SELECT, INSERT, UPDATE ON orders TO app;
   ```
3. Guessing: block the source address at the network layer, and require TLS
   (`--pg-require-tls`) so passwords are not sent over plain TCP. SCRAM never
   sends the password itself, but TLS also hides user names and queries.
4. Rotate a password: `ALTER USER app WITH PASSWORD '…'`. Open sessions keep
   working; new logins need the new password.
5. Cap a user's sessions so a leaked credential cannot exhaust connections:
   `ALTER USER app WITH CONNECTION LIMIT 50`.

Audit events: `login`, `login_failed`, `permission_denied`, `ddl` (including role
changes and ANALYZE), `grant`, `revoke`. The log is append-only; ship it to central storage.
