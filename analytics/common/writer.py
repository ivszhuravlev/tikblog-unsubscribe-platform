"""Local critical section for Silver timestamps and Gold snapshots in one driver."""

from threading import RLock

# Gold must not publish a snapshot while an older-timestamped Silver write is
# still uncommitted. This is an in-process lock, not a distributed lock/service.
silver_gold_lock = RLock()
