# Nodes exit after a fatal internal error

## Bug Fix

When an internal error forced a node to shut down, the node could stop serving
requests without ever exiting. A later `SIGTERM` did not stop it either, so the
process only ended when it was killed.

Such a node now finishes its shutdown, at the latest after `shutdown-timeout`, and
exits with a non-zero exit code. If a shutdown still does not complete, the server
exits on its own once twice the `shutdown-timeout` has passed. Supervisors such as
Kubernetes or systemd then restart it. No configuration changes are required.
