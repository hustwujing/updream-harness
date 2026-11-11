# Authentication and Ingress

## Applies to

Required for anonymous identity, account login, cookies, owner IDs, nginx/FRPC
entry points or account management.

## Unified principal model

Nginx overwrites `X-Updream-Ingress` with `direct` on the ordinary listener and
`frpc` on the dedicated PROXY Protocol listener. The API is loopback-only and
uses that trusted marker as follows:

- a valid account Session always resolves to its stable account `owner_id`;
- direct ingress without an account retains the signed anonymous browser owner;
- FRPC ingress without an account may load static assets, `/api/session` and the
  login endpoint, but other APIs return `authentication_required`.

Changing username/password never changes `owner_id`. Direct users may log in
voluntarily and then see the same account-owned Threads as FRPC users.
Network access is decided for each new Turn from its submission ingress:
authenticated FRPC submissions receive public HTTP/S access, while direct and
all other submissions remain on the configured restricted allowlist. Switching
entry points later does not change an existing or queued Turn.

## Credentials and Sessions

Passwords are Argon2id hashes. `users` stores stable identity; `auth_sessions`
stores only SHA-256 hashes of random Cookie tokens. Cookies are HttpOnly and
SameSite=Lax. Password change keeps the current Session and revokes the rest;
disable/reset revokes all Sessions.

The current FRPC endpoint is HTTP by explicit deployment choice. This provides
no transport confidentiality for password, Cookie or content; hashing protects
stored passwords, not traffic interception.

## Ingress boundary

FRPC sends PROXY Protocol v2 to a dedicated loopback nginx port. That listener
sets `frpc` and forwards `$proxy_protocol_addr`; the normal port sets `direct`.
Never enable `proxy_protocol` on the shared direct listener. Never trust a
caller-supplied `X-Forwarded-For` or ingress header without nginx overwrite.

## Account administration

Use `sandbox-harness-user create|list|rename|reset-password|disable|enable` for
administration. Direct-ingress users may also register from the browser account
panel. FRPC users may log in, log out and change passwords, but cannot register
or rename accounts; both UI and API enforce this boundary.
Passwords must contain 8-256 characters. Direct anonymous registration moves
the current owner's Threads, uploads and media records to a new account owner
in one transaction, and clears the anonymous Cookie. Registration is rejected
while that owner has active work. FRPC registration starts with an empty owner.
The CLI reads new passwords from a mode-0600 file so credentials do not appear
in command arguments or logs. Password files must be mode 0600 and are not tracked.

## Required checks

- Direct anonymous Cookie behavior remains unchanged.
- FRPC denies owner-scoped APIs before login.
- Account Cookie resolves to the same owner on both ingress paths.
- Invalid login is generic and rate limited; logs contain no password/Cookie.
- Registration enforces username uniqueness and preserves direct anonymous data without leaving it reachable through the old anonymous owner.
- Username change preserves owner; password change revokes other Sessions.
- Wrong-owner account operations return 404 at resource boundaries.
- Nginx direct/FRPC configs and PROXY v2 behavior are tested separately.
- Turn network access follows the trusted ingress and remains frozen on replay/recovery.
