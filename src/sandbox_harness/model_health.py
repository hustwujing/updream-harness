"""Probe observations are evidence, not a per-request circuit breaker."""
from typing import Any


def classify_response(response) -> tuple[str, str | None]:
    try:
        value = response.json()
    except ValueError:
        value = None
    error = value.get('error') if isinstance(value, dict) else None
    error = error if isinstance(error, dict) else {}
    code = str(error.get('code') or '').lower()
    message = str(error.get('message') or '').lower()
    if response.status_code in {401, 403}:
        return 'unavailable', 'access_denied'
    if code in {'model_not_found', 'unsupported_model', 'model_not_supported'}:
        return 'unavailable', 'model_not_supported'
    if (str(error.get('type') or '').lower() == 'not_found_error' and 'model' in message) or 'unknown model' in message:
        return 'unavailable', 'model_not_supported'
    if 'no cluster found for model' in message:
        return 'unavailable', 'model_route_missing'
    if 'not supported when using codex with a chatgpt account' in message:
        return 'unavailable', 'model_not_supported'
    if 200 <= response.status_code < 300:
        if isinstance(value, dict) and not value.get('error') and value.get('status') != 'failed' and value.get('type') != 'error':
            return 'available', None
        return 'busy', 'invalid_probe_response'
    return 'busy', f'http_{response.status_code}'


def reduce_health(previous: dict[str, Any], observation: dict[str, Any], *, failures: int = 3,
                  successes: int = 2) -> dict[str, Any]:
    outcome = observation['status']
    stable = previous.get('status', 'unknown')
    good = previous.get('success_streak', 0) + 1 if outcome == 'available' else 0
    bad = previous.get('failure_streak', 0) + 1 if outcome == 'busy' else 0
    if outcome == 'unavailable':
        stable = 'unavailable'
    elif outcome == 'available':
        if stable in {'unknown', 'available'} or good >= successes:
            stable = 'available'
    elif stable != 'unavailable' and bad >= failures:
        stable = 'busy'
    # An explicit unsupported-route diagnosis is not cleared by another transient
    # failure. Two successful probes are needed to make it selectable again.
    blocking_reason = observation.get('error_code') if outcome == 'unavailable' else previous.get('blocking_reason')
    if stable != 'unavailable':
        blocking_reason = None
    checked_at = observation['checked_at']
    return {**observation, 'status': stable, 'available': stable != 'unavailable',
            'last_probe_status': outcome, 'success_streak': good, 'failure_streak': bad,
            'blocking_reason': blocking_reason,
            'last_success_at': checked_at if outcome == 'available' else previous.get('last_success_at'),
            'state_changed_at': checked_at if stable != previous.get('status') else previous.get('state_changed_at', checked_at)}
