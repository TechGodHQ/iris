#!/bin/sh
# Iris reads native IRIS_* configuration directly. Keep this entrypoint a
# transparent exec so containers need neither a writable config path nor TOML
# generation at startup.
set -eu
exec "$@"
