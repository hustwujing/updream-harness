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

Use `sandbox-harness-user create|list|rename|reset-password|disable|enable`.
Password files must be mode 0600 and are not tracked. Public self-registration
is not implemented.

## Required checks

- Direct anonymous Cookie behavior remains unchanged.
- FRPC denies owner-scoped APIs before login.
- Account Cookie resolves to the same owner on both ingress paths.
- Invalid login is generic and rate limited; logs contain no password/Cookie.
- Username change preserves owner; password change revokes other Sessions.
- Wrong-owner account operations return 404 at resource boundaries.
- Nginx direct/FRPC configs and PROXY v2 behavior are tested separately.
