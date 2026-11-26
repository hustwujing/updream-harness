import importlib.util
from pathlib import Path
from urllib.parse import urlsplit


ROOT=Path(__file__).resolve().parents[1]


def provisioner():
    path=ROOT/"deploy/ali/provision-domain-databases.py"
    spec=importlib.util.spec_from_file_location("domain_database_provisioner",path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_domain_urls_have_distinct_roles_and_databases_without_secret_output():
    module=provisioner();admin={"host":"127.0.0.1","port":28433}
    values={domain:module._domain_url(admin,domain,"secret /+ value") for domain in module.DOMAINS}
    parsed=[urlsplit(value.replace("postgresql+asyncpg://","postgresql://",1)) for value in values.values()]
    assert len({value.username for value in parsed})==4
    assert len({value.path for value in parsed})==4
    assert all(value.hostname=="127.0.0.1" and value.port==28433 for value in parsed)
    assert all("secret /+ value" not in value for value in values.values())


def test_platform_environment_update_is_idempotent_and_preserves_other_settings():
    module=provisioner();source=("SANDBOX_HARNESS_BACKEND=aliyun\n"
        "SANDBOX_HARNESS_DATABASE_URL=legacy\nSANDBOX_HARNESS_CORE_DATABASE_URL=old\n")
    values={domain:f"postgresql+asyncpg://{domain}:secret@127.0.0.1:28433/{domain}"
            for domain in module.DOMAINS}
    first=module._updated_environment(source,values)
    second=module._updated_environment(first,values)
    assert first==second and "SANDBOX_HARNESS_BACKEND=aliyun" in first
    assert all(first.count(f"SANDBOX_HARNESS_{domain.upper()}_DATABASE_URL=")==1
               for domain in module.DOMAINS)
    assert "SANDBOX_HARNESS_DATABASE_URL=" not in first


def test_provisioner_revokes_public_connect_and_uses_non_privileged_roles():
    source=(ROOT/"deploy/ali/provision-domain-databases.py").read_text()
    assert 'SANDBOX_HARNESS_ADMIN_DATABASE_URL' in source
    assert 'configured.get("SANDBOX_HARNESS_DATABASE_URL"' not in source
    assert "REVOKE ALL ON DATABASE" in source
    assert "NOSUPERUSER NOCREATEDB NOCREATEROLE" in source
    assert "os.replace" in source and "0o600" in source
    assert 'pwd.getpwnam("sandbox-harness")' in source and "os.chown" in source
