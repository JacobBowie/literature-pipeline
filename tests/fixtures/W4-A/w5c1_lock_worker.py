"""A multiprocessing worker for the lock-file races (W5-C1): it imports only litpipe.lockfile."""
from litpipe import lockfile


def race(root, barrier, out, run_id):
    """Wait at the barrier, try to take the project's lock, report ("took" | "held", run id, the other
    record's run id), then hold a taken lock until every worker has tried."""
    barrier.wait(timeout=60)
    lk = None
    try:
        lk = lockfile.Lock(root, tool="race", run_id=run_id, heartbeat=False).acquire()
        out.put(("took", run_id, (lk.broke or {}).get("run_id")))
    except lockfile.LockHeld as e:
        out.put(("held", run_id, (e.record or {}).get("run_id")))
    barrier.wait(timeout=60)
    if lk is not None:
        lk.release()
