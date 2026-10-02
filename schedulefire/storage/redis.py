"""Redis job storage (requires ``redis``: ``pip install "schedulefire[redis]"``).

The fastest backend: fetching and state changes are single atomic Lua scripts, and idle
workers in every process are woken instantly through pub/sub. Works with Redis >= 5
(standalone or Sentinel). All keys share the ``{schedulefire}`` hash tag, so they land
in one slot on Redis Cluster too.

Configure Redis for durability (AOF ``appendfsync everysec`` or better) and a
``maxmemory-policy`` of ``noeviction``: an evicting Redis silently loses jobs.
"""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence

from .. import _serialization
from ..states import ALL_STATES, DELETED, ENQUEUED, FAILED, SCHEDULED, SUCCEEDED, State
from .base import (
    FetchedJob,
    JobRecord,
    JobStorage,
    QueueInfo,
    RecurringJobRecord,
    ServerRecord,
    StateRecord,
    UnsupportedDatabaseVersionError,
    merge_request,
    version_tuple,
)

logger = logging.getLogger("schedulefire")

# KEYS[1] is only used to route the call on Redis Cluster; ARGV[1] is the key prefix.
_SET_STATE = """
local p, id = ARGV[1], ARGV[2]
local jk = p .. ':job:' .. id
if redis.call('EXISTS', jk) == 0 then return 0 end
local cur = redis.call('HGET', jk, 'state') or ''
if ARGV[3] ~= '' then
  local ok = false
  for _, s in ipairs(cjson.decode(ARGV[3])) do if s == cur then ok = true break end end
  if not ok then return 0 end
end
local new = ARGV[4]
if cur ~= '' then redis.call('ZREM', p .. ':state:' .. cur, id) end
redis.call('ZREM', p .. ':schedule', id)
redis.call('ZADD', p .. ':state:' .. new, tonumber(id), id)
redis.call('HSET', jk, 'state', new, 'state_data', ARGV[6])
if ARGV[14] == '1' then redis.call('HSET', jk, 'state_reason', ARGV[5]) else redis.call('HDEL', jk, 'state_reason') end
if new == 'Scheduled' then redis.call('ZADD', p .. ':schedule', ARGV[7], id) end
if ARGV[8] ~= '' then
  redis.call('HSET', jk, 'expire_at', ARGV[8]); redis.call('ZADD', p .. ':expiring', ARGV[8], id)
else
  redis.call('HDEL', jk, 'expire_at'); redis.call('ZREM', p .. ':expiring', id)
end
redis.call('LPUSH', jk .. ':history', ARGV[11])
if new ~= 'Processing' then
  if redis.call('HGET', p .. ':fetched-queue', id) then
    redis.call('ZREM', p .. ':fetched', id); redis.call('HDEL', p .. ':fetched-queue', id)
  elseif cur == 'Enqueued' then
    local oq = redis.call('HGET', jk, 'enqueued_to')
    if oq then redis.call('LREM', p .. ':queue:' .. oq, 0, id) end
  end
end
if new == 'Enqueued' then
  redis.call('LPUSH', p .. ':queue:' .. ARGV[9], id)
  redis.call('HSET', jk, 'enqueued_to', ARGV[9])
  redis.call('SADD', p .. ':queues', ARGV[9])
  redis.call('PUBLISH', p .. ':notify', ARGV[9])
end
if new == 'Succeeded' or new == 'Failed' or new == 'Deleted' then
  local n = string.lower(new)
  redis.call('INCR', p .. ':stats:' .. n)
  local dk = p .. ':stats:' .. n .. ':' .. ARGV[12]
  redis.call('INCR', dk); redis.call('EXPIRE', dk, 7776000)
  local hk = p .. ':stats:' .. n .. ':h:' .. ARGV[13]
  redis.call('INCR', hk); redis.call('EXPIRE', hk, 691200)
end
return 1
"""

# Requeue jobs whose worker stopped heart-beating, then pop from the first non-empty queue.
_FETCH = """
local p, now = ARGV[1], ARGV[2]
local stale = redis.call('ZRANGEBYSCORE', p .. ':fetched', '-inf', '(' .. ARGV[3], 'LIMIT', 0, 100)
for _, id in ipairs(stale) do
  local q = redis.call('HGET', p .. ':fetched-queue', id)
  redis.call('ZREM', p .. ':fetched', id); redis.call('HDEL', p .. ':fetched-queue', id)
  if q then redis.call('RPUSH', p .. ':queue:' .. q, id) end
end
for i = 4, #ARGV do
  local q = ARGV[i]
  local id = redis.call('RPOP', p .. ':queue:' .. q)
  if id then
    redis.call('ZADD', p .. ':fetched', now, id); redis.call('HSET', p .. ':fetched-queue', id, q)
    return {id, q}
  end
end
return false
"""

_RELEASE_LOCK = """
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
"""

_DELETE_EXPIRED = """
local p = ARGV[1]
local ids = redis.call('ZRANGEBYSCORE', p .. ':expiring', '-inf', '(' .. ARGV[2], 'LIMIT', 0, tonumber(ARGV[3]))
for _, id in ipairs(ids) do
  local jk = p .. ':job:' .. id
  local st = redis.call('HGET', jk, 'state')
  if st then redis.call('ZREM', p .. ':state:' .. st, id) end
  if redis.call('HGET', p .. ':fetched-queue', id) then
    redis.call('ZREM', p .. ':fetched', id); redis.call('HDEL', p .. ':fetched-queue', id)
  elseif st == 'Enqueued' then
    local oq = redis.call('HGET', jk, 'enqueued_to')
    if oq then redis.call('LREM', p .. ':queue:' .. oq, 0, id) end
  end
  redis.call('ZREM', p .. ':schedule', id)
  redis.call('ZREM', p .. ':expiring', id)
  redis.call('DEL', jk, jk .. ':history', jk .. ':params', jk .. ':continuations')
end
return #ids
"""

_RECURRING_FIELDS = {
    "invocation", "queue", "cron", "time_zone", "next_execution",
    "last_execution", "last_job_id", "error",
}


def _day(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def _hour(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H")


def _float(value) -> Optional[float]:
    return None if value in (None, "") else float(value)


class RedisStorage(JobStorage):
    def __init__(
        self,
        url: str = "redis://localhost:6379/0",
        *,
        client: Any = None,
        prefix: str = "{schedulefire}",
        invisibility_timeout: float = 30 * 60,
        job_expiration: float = 24 * 3600,
        use_notify: bool = True,
    ):
        """
        :param url: Redis URL, e.g. ``redis://:password@host:6379/0`` or ``rediss://`` for TLS.
        :param client: an existing ``redis.Redis`` created with ``decode_responses=True``
            (instead of ``url``) - e.g. a Sentinel master client.
        :param prefix: key prefix; keep the ``{...}`` hash tag for Redis Cluster.
        """
        try:
            import redis
        except ImportError:
            raise ImportError('RedisStorage requires redis-py: pip install "schedulefire[redis]"') from None
        super().__init__(job_expiration=job_expiration)
        self._redis = redis
        if client is None:
            try:  # RESP2 works on every Redis version; redis-py >= 8 would default to RESP3 (Redis 6+)
                client = redis.Redis.from_url(url, decode_responses=True, protocol=2)
            except TypeError:  # redis-py < 5 has no `protocol` option and always speaks RESP2
                client = redis.Redis.from_url(url, decode_responses=True)
        self.r = client
        self._check_version()
        self.prefix = prefix
        self.invisibility_timeout = invisibility_timeout
        self.use_notify = use_notify
        self._set_state = self.r.register_script(_SET_STATE)
        self._fetch = self.r.register_script(_FETCH)
        self._release = self.r.register_script(_RELEASE_LOCK)
        self._delete_expired = self.r.register_script(_DELETE_EXPIRED)
        self._listener: Optional[threading.Thread] = None
        self._listener_lock = threading.Lock()
        self._disposed = threading.Event()

    def _check_version(self) -> None:
        try:
            version = self.r.info("server").get("redis_version", "")
        except self._redis.ResponseError:
            return  # INFO disabled by the provider: skip the check
        if version and version_tuple(version) < (4, 0, 0):
            raise UnsupportedDatabaseVersionError(f"RedisStorage needs Redis 4.0 or newer; the server is {version}.")

    def _k(self, *parts: Any) -> str:
        return ":".join([self.prefix, *map(str, parts)])

    @staticmethod
    def _valid_id(job_id: Any) -> Optional[str]:
        text = str(job_id)
        return text if text.isdigit() else None

    def dispose(self) -> None:
        self._disposed.set()
        self.r.close()

    # -- cross-process wake-up ----------------------------------------------------

    def wait_for_jobs(self, timeout: float) -> None:
        if self.use_notify:
            self._ensure_listener()
        super().wait_for_jobs(timeout)

    def _ensure_listener(self) -> None:
        with self._listener_lock:
            if self._listener is not None and self._listener.is_alive():
                return
            self._listener = threading.Thread(target=self._listen, name="schedulefire-redis-listener", daemon=True)
            self._listener.start()

    def _listen(self) -> None:
        while not self._disposed.is_set():
            try:
                pubsub = self.r.pubsub(ignore_subscribe_messages=True)
                pubsub.subscribe(self._k("notify"))
                try:
                    while not self._disposed.is_set():
                        if pubsub.get_message(timeout=1.0):
                            self.notify_new_jobs()
                finally:
                    pubsub.close()
            except Exception:
                if self._disposed.is_set():
                    return  # connection closed by dispose()
                logger.warning("Redis pub/sub connection lost; retrying in 5s", exc_info=True)
                self._disposed.wait(5)

    # -- locks --------------------------------------------------------------------

    def _try_acquire_lock(self, resource: str, owner: str, ttl: float) -> bool:
        return bool(self.r.set(self._k("lock", resource), owner, nx=True, px=max(1, int(ttl * 1000))))

    def _release_lock(self, resource: str, owner: str) -> None:
        self._release(keys=[self._k("lock", resource)], args=[owner])

    # -- jobs ---------------------------------------------------------------------

    def create_job(self, invocation: dict, queue: str, parameters: Dict[str, Any], expire_in: float) -> str:
        now = time.time()
        job_id = str(self.r.incr(self._k("job-id")))
        key = self._k("job", job_id)
        pipe = self.r.pipeline()
        pipe.hset(key, mapping={
            "invocation": _serialization.dumps(invocation), "queue": queue,
            "created_at": repr(now), "expire_at": repr(now + expire_in),
        })
        if parameters:
            pipe.hset(key + ":params", mapping={k: _serialization.dumps(v) for k, v in parameters.items()})
        pipe.zadd(self._k("expiring"), {job_id: now + expire_in})
        pipe.execute()
        return job_id

    def _record(self, job_id: str, row: dict, params: dict) -> JobRecord:
        return JobRecord(
            id=job_id,
            invocation=_serialization.loads(row["invocation"]),
            queue=row["queue"],
            state_name=row.get("state") or None,
            state_reason=row.get("state_reason"),
            state_data=_serialization.loads(row["state_data"]) if row.get("state_data") else {},
            parameters={k: _serialization.loads(v) for k, v in params.items()},
            created_at=float(row["created_at"]),
            expire_at=_float(row.get("expire_at")),
        )

    def _get_jobs(self, ids: Sequence[str]) -> List[JobRecord]:
        pipe = self.r.pipeline()
        for job_id in ids:
            pipe.hgetall(self._k("job", job_id))
            pipe.hgetall(self._k("job", job_id, "params"))
        results = pipe.execute()
        records = []
        for i, job_id in enumerate(ids):
            row, params = results[2 * i], results[2 * i + 1]
            if row:
                records.append(self._record(str(job_id), row, params))
        return records

    def get_job(self, job_id: str) -> Optional[JobRecord]:
        job_id = self._valid_id(job_id)
        if job_id is None:
            return None
        records = self._get_jobs([job_id])
        return records[0] if records else None

    def set_state(self, job_id, state: State, *, expected_states=None, expire_at=None) -> bool:
        job_id = self._valid_id(job_id)
        if job_id is None:
            return False
        now = time.time()
        data = state.serialize()
        history = _serialization.dumps({"name": state.name, "reason": state.reason, "data": data, "created_at": now})
        expected = "" if expected_states is None else json.dumps([s or "" for s in expected_states])
        enqueue_at = getattr(state, "enqueue_at", None) if state.name == SCHEDULED else None
        ok = self._set_state(keys=[self._k("job", job_id)], args=[
            self.prefix, job_id, expected, state.name, state.reason or "", _serialization.dumps(data),
            "" if enqueue_at is None else repr(enqueue_at), "" if expire_at is None else repr(expire_at),
            getattr(state, "queue", None) or "", repr(now), history, _day(now), _hour(now),
            "1" if state.reason is not None else "0",
        ])
        if ok and state.name == ENQUEUED:
            self.notify_new_jobs()
        return bool(ok)

    def get_job_parameter(self, job_id: str, name: str) -> Any:
        job_id = self._valid_id(job_id)
        value = None if job_id is None else self.r.hget(self._k("job", job_id, "params"), name)
        return None if value is None else _serialization.loads(value)

    def set_job_parameter(self, job_id: str, name: str, value: Any) -> None:
        job_id = self._valid_id(job_id)
        if job_id is not None and self.r.exists(self._k("job", job_id)):
            self.r.hset(self._k("job", job_id, "params"), name, _serialization.dumps(value))

    def get_state_history(self, job_id: str) -> List[StateRecord]:
        job_id = self._valid_id(job_id)
        if job_id is None:
            return []
        entries = [_serialization.loads(e) for e in self.r.lrange(self._k("job", job_id, "history"), 0, -1)]
        return [StateRecord(e["name"], e["reason"], e["data"], e["created_at"]) for e in entries]

    # -- queues -------------------------------------------------------------------

    def fetch_next(self, queues: Sequence[str]) -> Optional[FetchedJob]:
        if not queues:
            return None
        now = time.time()
        result = self._fetch(keys=[self._k("fetched")],
                             args=[self.prefix, repr(now), repr(now - self.invisibility_timeout), *queues])
        if not result:
            return None
        job_id, queue = result
        return FetchedJob(job_id=str(job_id), queue=queue, token=str(job_id))

    def remove_from_queue(self, fetched: FetchedJob) -> None:
        pipe = self.r.pipeline()
        pipe.zrem(self._k("fetched"), fetched.token)
        pipe.hdel(self._k("fetched-queue"), fetched.token)
        pipe.lrem(self._k("queue", fetched.queue), 0, fetched.job_id)
        pipe.execute()

    def heartbeat_fetched(self, tokens: Sequence[Any]) -> None:
        tokens = list(tokens)
        if tokens:
            now = time.time()
            self.r.zadd(self._k("fetched"), {t: now for t in tokens}, xx=True)

    def get_scheduled_due(self, now: float, limit: int) -> List[str]:
        return list(self.r.zrangebyscore(self._k("schedule"), "-inf", now, start=0, num=limit))

    # -- continuations ------------------------------------------------------------

    def add_continuation(self, parent_id: str, child_id: str) -> None:
        self.r.sadd(self._k("job", parent_id, "continuations"), str(child_id))

    def get_continuations(self, parent_id: str) -> List[str]:
        return sorted(self.r.smembers(self._k("job", parent_id, "continuations")), key=int)

    # -- recurring jobs -----------------------------------------------------------

    def _recurring_record(self, row: dict) -> RecurringJobRecord:
        return RecurringJobRecord(
            id=row["id"], invocation=_serialization.loads(row["invocation"]), queue=row["queue"],
            cron=row["cron"], time_zone=row["time_zone"], next_execution=_float(row.get("next_execution")),
            last_execution=_float(row.get("last_execution")), last_job_id=row.get("last_job_id") or None,
            created_at=float(row["created_at"]), error=row.get("error") or None,
        )

    @staticmethod
    def _encode_recurring_value(name: str, value: Any) -> Optional[str]:
        if value is None:
            return None
        if name == "invocation":
            return _serialization.dumps(value)
        if name in ("next_execution", "last_execution", "created_at"):
            return repr(float(value))
        return str(value)

    def _write_recurring_fields(self, pipe, recurring_id: str, fields: Dict[str, Any]) -> None:
        key = self._k("recurring", recurring_id)
        values = {n: self._encode_recurring_value(n, v) for n, v in fields.items()}
        present = {n: v for n, v in values.items() if v is not None}
        missing = [n for n, v in values.items() if v is None]
        if present:
            pipe.hset(key, mapping=present)
        if missing:
            pipe.hdel(key, *missing)
        if "next_execution" in fields:
            if fields["next_execution"] is None:
                pipe.zrem(self._k("recurring-schedule"), recurring_id)
            else:
                pipe.zadd(self._k("recurring-schedule"), {recurring_id: float(fields["next_execution"])})

    def upsert_recurring(self, record: RecurringJobRecord) -> None:
        pipe = self.r.pipeline()
        pipe.hsetnx(self._k("recurring", record.id), "created_at", repr(float(record.created_at)))
        pipe.hset(self._k("recurring", record.id), "id", record.id)
        self._write_recurring_fields(pipe, record.id, {
            "invocation": record.invocation, "queue": record.queue, "cron": record.cron,
            "time_zone": record.time_zone, "next_execution": record.next_execution,
            "last_execution": record.last_execution, "last_job_id": record.last_job_id, "error": record.error,
        })
        pipe.sadd(self._k("recurring-ids"), record.id)
        pipe.execute()

    def _get_recurring_many(self, ids: Sequence[str]) -> List[RecurringJobRecord]:
        pipe = self.r.pipeline()
        for rid in ids:
            pipe.hgetall(self._k("recurring", rid))
        return [self._recurring_record(row) for row in pipe.execute() if row]

    def get_recurring(self, recurring_id: str) -> Optional[RecurringJobRecord]:
        records = self._get_recurring_many([recurring_id])
        return records[0] if records else None

    def get_recurring_jobs(self) -> List[RecurringJobRecord]:
        return self._get_recurring_many(sorted(self.r.smembers(self._k("recurring-ids"))))

    def get_due_recurring(self, now: float, limit: int) -> List[RecurringJobRecord]:
        ids = self.r.zrangebyscore(self._k("recurring-schedule"), "-inf", now, start=0, num=limit)
        return self._get_recurring_many(ids)

    def update_recurring(self, recurring_id: str, **fields: Any) -> None:
        unknown = set(fields) - _RECURRING_FIELDS
        if unknown:
            raise ValueError(f"Unknown recurring job fields: {sorted(unknown)}")
        if not fields or not self.r.exists(self._k("recurring", recurring_id)):
            return
        pipe = self.r.pipeline()
        self._write_recurring_fields(pipe, recurring_id, fields)
        pipe.execute()

    def remove_recurring(self, recurring_id: str) -> bool:
        pipe = self.r.pipeline()
        pipe.srem(self._k("recurring-ids"), recurring_id)
        pipe.delete(self._k("recurring", recurring_id))
        pipe.zrem(self._k("recurring-schedule"), recurring_id)
        return bool(pipe.execute()[0])

    # -- servers ------------------------------------------------------------------

    def announce_server(self, server_id: str, name: str, queues: Sequence[str], worker_count: int) -> None:
        now = time.time()
        pipe = self.r.pipeline()
        pipe.hset(self._k("server", server_id), mapping={
            "name": name, "queues": json.dumps(list(queues)), "worker_count": worker_count, "started_at": repr(now)})
        pipe.zadd(self._k("servers"), {server_id: now})
        pipe.execute()

    def server_heartbeat(self, server_id: str) -> None:
        self.r.zadd(self._k("servers"), {server_id: time.time()}, xx=True)

    def remove_server(self, server_id: str) -> None:
        pipe = self.r.pipeline()
        pipe.zrem(self._k("servers"), server_id)
        pipe.delete(self._k("server", server_id))
        pipe.execute()

    def remove_timed_out_servers(self, timeout: float) -> int:
        dead = self.r.zrangebyscore(self._k("servers"), "-inf", time.time() - timeout)
        for server_id in dead:
            self.remove_server(server_id)
        return len(dead)

    @staticmethod
    def _server_record(server_id: str, heartbeat: float, row: dict) -> ServerRecord:
        return ServerRecord(id=server_id, name=row["name"], queues=json.loads(row["queues"]),
                            worker_count=int(row["worker_count"]), started_at=float(row["started_at"]),
                            heartbeat=heartbeat,
                            requested=json.loads(row["requested"]) if row.get("requested") else None)

    def get_servers(self) -> List[ServerRecord]:
        entries = self.r.zrange(self._k("servers"), 0, -1, withscores=True)
        pipe = self.r.pipeline()
        for server_id, _ in entries:
            pipe.hgetall(self._k("server", server_id))
        result = [self._server_record(server_id, heartbeat, row)
                  for (server_id, heartbeat), row in zip(entries, pipe.execute()) if row]
        return sorted(result, key=lambda s: s.started_at)

    def get_server(self, server_id: str) -> Optional[ServerRecord]:
        pipe = self.r.pipeline()
        pipe.zscore(self._k("servers"), server_id)
        pipe.hgetall(self._k("server", server_id))
        heartbeat, row = pipe.execute()
        return self._server_record(server_id, heartbeat, row) if heartbeat is not None and row else None

    def request_server_change(self, server_id, worker_count=None, queues=None) -> bool:
        key = self._k("server", server_id)
        with self.r.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    if not pipe.exists(key):
                        return False
                    current = pipe.hget(key, "requested")
                    request = merge_request(json.loads(current) if current else None, worker_count, queues)
                    pipe.multi()
                    pipe.hset(key, "requested", json.dumps(request))
                    pipe.execute()
                    return True
                except self._redis.WatchError:
                    continue

    def update_server_state(self, server_id, worker_count, queues, applied_request=None) -> None:
        key = self._k("server", server_id)
        with self.r.pipeline() as pipe:
            while True:
                try:
                    pipe.watch(key)
                    if not pipe.exists(key):
                        return
                    current = pipe.hget(key, "requested")
                    pipe.multi()
                    pipe.hset(key, mapping={"worker_count": worker_count, "queues": json.dumps(list(queues))})
                    if applied_request is not None and current and json.loads(current) == applied_request:
                        pipe.hdel(key, "requested")
                    pipe.execute()
                    return
                except self._redis.WatchError:
                    continue

    # -- maintenance --------------------------------------------------------------

    def delete_expired_jobs(self, now: float, batch_size: int = 1000) -> int:
        # Locks and hourly counters expire on their own through Redis TTLs.
        return int(self._delete_expired(keys=[self._k("expiring")], args=[self.prefix, repr(now), batch_size]))

    # -- monitoring ---------------------------------------------------------------

    def get_statistics(self) -> Dict[str, int]:
        pipe = self.r.pipeline()
        for name in ALL_STATES:
            pipe.zcard(self._k("state", name))
        pipe.get(self._k("stats", "succeeded"))
        pipe.get(self._k("stats", "deleted"))
        pipe.zcard(self._k("servers"))
        pipe.scard(self._k("recurring-ids"))
        values = pipe.execute()
        stats = {name.lower(): int(values[i]) for i, name in enumerate(ALL_STATES)}
        n = len(ALL_STATES)
        stats["succeeded"] = int(values[n] or 0)
        stats["deleted"] = int(values[n + 1] or 0)
        stats["servers"] = int(values[n + 2])
        stats["recurring"] = int(values[n + 3])
        stats["queues"] = len(self.get_queues())
        return stats

    def get_jobs(self, state: str, offset: int = 0, limit: int = 20) -> List[JobRecord]:
        end = offset + limit - 1
        if state == SCHEDULED:
            ids = self.r.zrange(self._k("schedule"), offset, end)
        elif state == ENQUEUED:
            ids = self.r.zrange(self._k("state", state), offset, end)
        else:
            ids = self.r.zrevrange(self._k("state", state), offset, end)
        return self._get_jobs(ids)

    def count_jobs(self, state: str) -> int:
        return int(self.r.zcard(self._k("state", state)))

    def get_queues(self) -> List[QueueInfo]:
        names = sorted(self.r.smembers(self._k("queues")))
        pipe = self.r.pipeline()
        for name in names:
            pipe.llen(self._k("queue", name))
        lengths = pipe.execute()
        fetched: Dict[str, int] = {}
        for queue in self.r.hvals(self._k("fetched-queue")):
            fetched[queue] = fetched.get(queue, 0) + 1
        return [QueueInfo(name, int(length), fetched.get(name, 0))
                for name, length in zip(names, lengths) if length or fetched.get(name)]

    def _counters(self, keys: Sequence[str]) -> List[int]:
        return [int(v or 0) for v in self.r.mget([self._k(k) for k in keys])] if keys else []

    def get_daily_history(self, days: int = 7) -> List[Dict[str, Any]]:
        today = datetime.now(timezone.utc).date()
        dates = [(today - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]
        values = self._counters([f"stats:{s}:{d}" for d in dates for s in ("succeeded", "failed")])
        return [{"date": d, "succeeded": values[2 * i], "failed": values[2 * i + 1]} for i, d in enumerate(dates)]

    def get_hourly_history(self, hours: int = 24) -> List[Dict[str, Any]]:
        now = time.time()
        stamps = [_hour(now - i * 3600) for i in range(hours - 1, -1, -1)]
        values = self._counters([f"stats:{s}:h:{hr}" for hr in stamps for s in ("succeeded", "failed")])
        return [{"hour": hr, "succeeded": values[2 * i], "failed": values[2 * i + 1]} for i, hr in enumerate(stamps)]
