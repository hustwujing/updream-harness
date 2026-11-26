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
Network access is decided for each new Turn from the authenticated principal:
any valid account Session receives public HTTP/S access, while anonymous direct
submissions remain on the configured restricted allowlist. Switching login or
entry point later does not change an existing or queued Turn.

## Credentials and Sessions

Passwords are Argon2id hashes. `users` stores stable identity; `auth_sessions`
stores only SHA-256 hashes of random Cookie tokens. Cookies are HttpOnly and
SameSite=Lax. Password change keeps the current Session and revokes the rest;
disable/reset revokes all Sessions. A transparent Argon2 parameter rehash only
updates `password_hash`; it is maintenance, not password rotation, and must not
revoke any Session.

Login throttling uses the same NFKC, trim and case-fold canonical username as
authentication, combined with the trusted source address. The current limiter
is deliberately process-local: failures expire after five minutes, and an API
process restart clears them. Deploy exactly one public API process unless the
limiter is moved to shared storage; multiple API workers would otherwise create
independent buckets and weaken brute-force protection.

The current FRPC endpoint is HTTP by explicit deployment choice. This provides
no transport confidentiality for password, Cookie or content; hashing protects
stored passwords, not traffic interception.

## Ingress boundary

FRPC sends PROXY Protocol v2 to a dedicated loopback nginx port. That listener
sets `frpc` and forwards `$proxy_protocol_addr`; the normal port sets `direct`.
Never enable `proxy_protocol` on the shared direct listener. Never trust a
caller-supplied `X-Forwarded-For` or ingress header without nginx overwrite.
The public `8090` deployment shares one PROXY v2 listener by URL prefix. Its
versioned location is `deploy/nginx-frpc-ali-shared-location.conf`; it
must be included inside that server rather than exposed as a second ordinary
HTTP listener.

## Account administration

Use `sandbox-harness-user create|list|rename|reset-password|disable|enable` for
administration. Direct-ingress users may also register from the browser account
panel. FRPC users may log in, log out and change passwords, but cannot register
or change profile identity/avatar; both UI and API enforce this boundary.
Passwords must contain 8-256 characters. Direct anonymous registration moves
every ORM table bearing `owner_id` (except the new `users` row itself) to the
account owner in the same transaction that creates the first Auth Session, and
clears the anonymous Cookie. A metadata test fails when a newly added
owner-bearing table is absent from this migration set. Registration is rejected
while that owner has an active Turn, generic Job, pending reference upload,
Sandbox takeover, pause/resume saga, media Job or site deployment. Replaying the
same username and password after response loss logs into the account that was
already committed instead of returning a false `username_taken`, but only when
the retry carries the same signed anonymous owner. This binding is a bounded
24-hour registration intent; submitting credentials for an unrelated existing
account remains `username_taken` and preserves the current anonymous Cookie and
data. FRPC registration starts with an empty owner.
The CLI reads new passwords from a mode-0600 file so credentials do not appear
in command arguments or logs. Password files must be mode 0600 and are not tracked.

Direct authenticated users may upload a PNG, JPEG, WebP or GIF avatar up to
5 MB. The immutable bytes live in the configured object store; `users` keeps
only the object reference, detected MIME and a monotonically increasing version.
Preview reads are account-scoped and never expose object-store URLs.

## Required checks

- Direct anonymous Cookie behavior remains unchanged.
- FRPC denies owner-scoped APIs before login.
- Account Cookie resolves to the same owner on both ingress paths.
- Invalid login is generic and rate limited; logs contain no password/Cookie.
- Registration enforces username uniqueness and preserves direct anonymous data without leaving it reachable through the old anonymous owner.
- Username change preserves owner; password change revokes other Sessions.
- Wrong-owner account operations return 404 at resource boundaries.
- Nginx direct/FRPC configs and PROXY v2 behavior are tested separately.
- Turn network access follows the authenticated principal and remains frozen on replay/recovery.
