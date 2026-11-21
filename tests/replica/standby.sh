#!/bin/bash
# Replica fixture (compose profile `replica`): a physical streaming standby of PRIMARY_HOST.
# Idempotent: the base backup runs only on an empty data directory; the physical slot
# upstream (primary_slot_name) is created by primary-init.sh. Then postgres runs as a hot
# standby with hot_standby_feedback and wal_level=logical (replica capture's prerequisites).
set -euo pipefail
export PGPASSWORD=phase0-local-only
if [ ! -s "$PGDATA/PG_VERSION" ]; then
  until pg_isready -h "$PRIMARY_HOST" -U replicator -d postgres -q; do sleep 1; done
  pg_basebackup -h "$PRIMARY_HOST" -U replicator -D "$PGDATA" -X stream -R -S standby_slot
  chmod 700 "$PGDATA"
  # In postgresql.conf, not on the command line: a test can then toggle it with ALTER
  # SYSTEM + pg_reload_conf() (the command line would override postgresql.auto.conf).
  echo "hot_standby_feedback = on" >> "$PGDATA/postgresql.conf"
fi
exec postgres -c hot_standby=on -c wal_level=logical \
  -c max_replication_slots=40 -c max_wal_senders=40
