#!/bin/bash
set -e

cat <<EOF > /var/lib/postgresql/data/pg_hba.conf
local all all trust
host all all 0.0.0.0/0 md5
EOF