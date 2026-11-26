from sandbox_harness.services.api.sessions import (Sessions)


def test_signed_browser_session_is_stable_and_tampering_rotates_identity():
    sessions = Sessions("test-session-signing-key")
    owner, token, created = sessions.resolve(None)
    assert created and len(owner) == 64
    assert sessions.resolve(token) == (owner, token, False)
    replacement_owner, replacement, created = sessions.resolve(token[:-1] + ("0" if token[-1] != "0" else "1"))
    assert created and replacement_owner != owner and replacement != token
