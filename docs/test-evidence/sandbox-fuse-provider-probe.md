# Sandbox FUSE Provider Probe

## Scope

The probe used isolated sandboxes and deleted them afterward. It distinguishes
platform-managed volume support from permission for an arbitrary process inside
the sandbox to mount its own FUSE filesystem.

## Ali Agent Sandbox

The current template exposes `/dev/fuse`; UID 10002 can open it, the kernel lists
FUSE, and `/usr/bin/fusermount3` is root-owned setuid mode 4755. The process has
no effective Linux capabilities. A temporary fusepy/libfuse3 daemon established
a real mountpoint and `fusermount3 -u` removed it successfully. The fusepy probe
did not provide correct file operations with libfuse3 (`readdir` unsupported and
read returned `EROFS`), so this proves mount plumbing rather than production
daemon compatibility. Ali officially supports managed JuiceFS mounts for a
second-generation custom MicroVM template with the storage clients installed.

## OpenSandbox

The current OpenSandbox image also exposes an openable `/dev/fuse` and kernel
FUSE, but UID 10001 has no effective capabilities and the image has no
`fusermount3`. Ordinary mount is denied. An image containing libfuse3 and a
setuid fusermount helper still needs a real mount test because the runtime may
apply `nosuid` or `no_new_privs`.

OpenSandbox's official Docker runtime supports a safer host-side OSSFS volume
model: the server host enables FUSE and mounts OSS, then injects the volume into
the sandbox. This does not require arbitrary in-sandbox mount privileges.

## Decision

- Keep CAS manifest/chunk restore as the portable authority and fallback.
- Evaluate Ali managed JuiceFS and OpenSandbox host-side FUSE volume injection
  as provider acceleration layers.
- Build an OpenSandbox FUSE experiment image before claiming arbitrary
  in-sandbox FUSE support.
- Do not put long-lived OSS credentials inside a sandbox.

References:

- <https://help.aliyun.com/en/agent-sandbox/append-mount-juicefs>
- <https://github.com/opensandbox-group/OpenSandbox/blob/main/docs/examples/docker-ossfs-volume-mount.md>
- <https://juicefs.com/docs/community/architecture/>
