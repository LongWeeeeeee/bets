"""Bounded, opt-in archive of the exact inputs used for Winline polling.

The poller only samples and enqueues immutable inputs. Compression, hashing and
all filesystem work belong to one writer. Existing evidence is never deleted.
"""
from __future__ import annotations

import atexit
from collections import OrderedDict
import copy
import fcntl
import gzip
import hashlib
import json
import logging
import math
import os
from pathlib import Path
import queue
import threading
import time
import uuid


LOG = logging.getLogger(__name__)
MAX_INPUT_CHARS = 4_500_000
STATUS_RESERVE_BYTES = 65536
V1_SCHEMA = "winline_dom_history.v1"
V2_SCHEMA = "winline_dom_history.v2"


def enabled() -> bool:
    return os.getenv("WINLINE_DOM_HISTORY_ENABLED", "0").strip().lower() in {
        "1", "true", "yes", "on",
    }


def _number(name: str, default: float, minimum: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return max(minimum, value) if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _archive_file(root, name):
    """Return a file below *root* without accepting a traversal from an index."""
    root = Path(root).resolve()
    target = (root / str(name)).resolve()
    if os.path.commonpath((str(root), str(target))) != str(root):
        raise ValueError("archive reference escapes root")
    return target


def read_record(root, index):
    """Read a v1 or v2 index entry and return a replay-ready full record.

    v2 keeps the raw HTML in a content-addressed gzip blob.  The returned
    record deliberately has the same ``inputs['html']`` shape as v1 so replay
    callers need no format branch.  A missing or altered referenced blob raises
    a normal file/decompression error or ``ValueError`` rather than replaying
    unverified input.
    """
    if not isinstance(index, dict) or not index.get("file"):
        raise ValueError("archive index entry has no record file")
    packed = _archive_file(root, index["file"]).read_bytes()
    expected = index.get("sha256")
    if expected and hashlib.sha256(packed).hexdigest() != expected:
        raise ValueError("archive record SHA-256 mismatch")
    try:
        record = json.loads(gzip.decompress(packed))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("archive record is unreadable") from exc
    if not isinstance(record, dict):
        raise ValueError("archive record is not an object")

    if record.get("schema") == V2_SCHEMA:
        ref = record.get("html_ref")
        if not isinstance(ref, dict) or not ref.get("file") or not ref.get("sha256"):
            raise ValueError("archive v2 record has no valid HTML reference")
        blob = _archive_file(root, ref["file"]).read_bytes()
        if ref.get("gzip_sha256") and hashlib.sha256(blob).hexdigest() != ref["gzip_sha256"]:
            raise ValueError("archive HTML blob gzip SHA-256 mismatch")
        try:
            html_bytes = gzip.decompress(blob)
            html = html_bytes.decode("utf-8")
        except (OSError, UnicodeDecodeError) as exc:
            raise ValueError("archive HTML blob is unreadable") from exc
        if hashlib.sha256(html_bytes).hexdigest() != ref["sha256"]:
            raise ValueError("archive HTML blob SHA-256 mismatch")
        inputs = record.get("inputs")
        if not isinstance(inputs, dict):
            raise ValueError("archive v2 record has no inputs")
        record["inputs"] = dict(inputs, html=html)
        return record

    if record.get("schema") != V1_SCHEMA:
        raise ValueError("unsupported archive record schema")
    inputs = record.get("inputs")
    if not isinstance(inputs, dict) or not isinstance(inputs.get("html"), str):
        raise ValueError("archive v1 record has no HTML")
    expected_html = index.get("html_sha256") or record.get("html_sha256")
    actual_html = hashlib.sha256(inputs["html"].encode("utf-8")).hexdigest()
    if expected_html and actual_html != expected_html:
        raise ValueError("archive v1 HTML SHA-256 mismatch")
    return record


class WinlineDOMHistory:
    def __init__(self, root, *, interval_s=15.0, max_bytes=512 * 1024 * 1024,
                 queue_size=8):
        self.root = Path(root)
        self.interval_s = max(0.0, float(interval_s))
        self.max_bytes = max(0, int(max_bytes))
        self._queue = queue.Queue(maxsize=max(1, int(queue_size)))
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None
        self._samples = OrderedDict()
        self._disabled = False
        self._stats = dict(queued=0, written=0, skipped=0, dropped=0, errors=0,
                           oversize=0, used_bytes=0, last_error=None,
                           budget_exhausted=False, last_written_id=None)

    def submit(self, attempt, inputs, result):
        """Return a queued ID, not a durability receipt; never wait for disk."""
        if not isinstance(inputs, dict) or not isinstance(inputs.get("html"), str):
            return None
        if sum(len(inputs.get(k) or "") for k in ("html", "body_text", "visible_text")) > MAX_INPUT_CHARS:
            with self._lock:
                self._stats["oversize"] += 1
            return None
        key = str(attempt.get("canonical_key") or "")
        stamp = time.monotonic()
        state = tuple(result.get(k) for k in (
            "p1_odds", "p2_odds", "market_status", "odds_bettable",
            "source", "error", "acquisition_error",
        ))
        with self._lock:
            if self._stop.is_set() or self._disabled:
                self._stats["dropped"] += 1
                return None
            previous = self._samples.get(key)
            if (previous and previous[0] == state
                    and stamp - previous[1] < self.interval_s
                    and not attempt.get("reload_attempted")):
                self._stats["skipped"] += 1
                return None
            capture_id = uuid.uuid4().hex
            record = {
                "schema": V2_SCHEMA, "capture_id": capture_id,
                "queued_wall": time.time(), "producer_pid": os.getpid(),
                "attempt": copy.deepcopy(attempt), "inputs": dict(inputs),
                "result": copy.deepcopy({k: v for k, v in result.items()
                                         if not k.startswith("_dom_history")}),
            }
            try:
                self._queue.put_nowait(record)
            except queue.Full:
                self._stats["dropped"] += 1
                return None
            self._stats["queued"] += 1
            self._samples[key] = (state, stamp, capture_id)
            self._samples.move_to_end(key)
            while len(self._samples) > 256:
                self._samples.popitem(last=False)
            if self._thread is None:
                self._thread = threading.Thread(target=self._run,
                                                name="winline-dom-history", daemon=True)
                self._thread.start()
            return capture_id

    def close(self, timeout=1.0):
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout)
        return thread is None or not thread.is_alive()

    def status(self):
        with self._lock:
            return dict(self._stats, queue_depth=self._queue.qsize(),
                        producer_pid=os.getpid(), max_bytes=self.max_bytes,
                        interval_s=self.interval_s, disabled=self._disabled)

    def _write_status(self):
        payload = dict(self.status(), updated_wall=time.time())
        tmp = self.root / "status.json.tmp"
        tmp.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
        os.replace(tmp, self.root / "status.json")

    def _write_record(self, record):
        html_bytes = record["inputs"]["html"].encode("utf-8")
        html_sha256 = hashlib.sha256(html_bytes).hexdigest()
        blob_name = "blobs/" + html_sha256 + ".html.gz"
        blob = gzip.compress(html_bytes, compresslevel=1, mtime=0)
        blob_target = self.root / blob_name
        if blob_target.exists():
            # Never overwrite an existing evidence blob.  A collision or
            # corruption must be visible rather than silently changing history.
            existing = gzip.decompress(blob_target.read_bytes())
            if hashlib.sha256(existing).hexdigest() != html_sha256:
                raise ValueError("existing HTML blob SHA-256 mismatch")
            new_blob = b""
        else:
            new_blob = blob
        stored = dict(record)
        stored["inputs"] = dict(record["inputs"])
        stored["inputs"].pop("html", None)
        stored["html_ref"] = dict(file=blob_name, sha256=html_sha256,
                                  gzip_sha256=hashlib.sha256(blob).hexdigest(),
                                  bytes=len(blob))
        packed = gzip.compress(json.dumps(stored, ensure_ascii=False).encode("utf-8"),
                               compresslevel=1, mtime=0)
        name = record["capture_id"] + ".json.gz"
        index = dict(capture_id=record["capture_id"], file=name,
                     sha256=hashlib.sha256(packed).hexdigest(),
                     html_sha256=html_sha256, bytes=len(packed),
                     html_file=blob_name, html_bytes=len(blob),
                     capture_wall=record["inputs"].get("captured_wall"),
                     canonical_key=record["attempt"].get("canonical_key"),
                     attempt_index=record["attempt"].get("attempt_index"),
                     market_status=record["attempt"].get("market_status"))
        line = (json.dumps(index, ensure_ascii=False) + "\n").encode("utf-8")
        required = len(new_blob) + len(packed) + len(line)
        if self._stats["used_bytes"] + required + STATUS_RESERVE_BYTES > self.max_bytes:
            with self._lock:
                self._stats["budget_exhausted"] = True
                self._stats["dropped"] += 1
                self._disabled = True
            LOG.warning("Winline DOM history budget exhausted: %s", self.root)
            return
        # Reserve before I/O: a failed write can leave useful partial evidence.
        # Counting it conservatively prevents failed attempts evading the cap.
        with self._lock:
            self._stats["used_bytes"] += required
        target = self.root / name
        tmp = self.root / (name + ".tmp")
        if new_blob:
            blob_target.parent.mkdir(exist_ok=True)
            blob_tmp = blob_target.with_name(blob_target.name + ".tmp")
            with blob_tmp.open("xb") as fh:
                fh.write(new_blob)
            os.replace(blob_tmp, blob_target)
        with tmp.open("xb") as fh:
            fh.write(packed)
        os.replace(tmp, target)
        with (self.root / "index.jsonl").open("ab") as fh:
            fh.write(line)
        with self._lock:
            self._stats["written"] += 1
            self._stats["last_written_id"] = record["capture_id"]

    def _run(self):
        lock_file = None
        try:
            self.root.mkdir(parents=True, exist_ok=True)
            lock_file = (self.root / "writer.lock").open("a")
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            used = sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())
            with self._lock:
                self._stats["used_bytes"] = used
            while not self._stop.is_set() or not self._queue.empty():
                try:
                    record = self._queue.get(timeout=0.25)
                except queue.Empty:
                    continue
                status_changed = False
                try:
                    if not self._disabled:
                        self._write_record(record)
                        status_changed = True
                    else:
                        with self._lock:
                            self._stats["dropped"] += 1
                except Exception as exc:
                    with self._lock:
                        self._stats["errors"] += 1
                        self._stats["last_error"] = type(exc).__name__ + ": " + str(exc)[:200]
                        key = str(record["attempt"].get("canonical_key") or "")
                        sample = self._samples.get(key)
                        if sample and sample[2] == record["capture_id"]:
                            # A failed write must not debounce the next attempt.
                            # Do not invalidate a newer capture already enqueued.
                            self._samples.pop(key, None)
                    LOG.warning("Winline DOM history write failed: %s", exc)
                    status_changed = True
                finally:
                    self._queue.task_done()
                if status_changed:
                    try:
                        self._write_status()
                    except OSError as exc:
                        LOG.warning("Winline DOM history status write failed: %s", exc)
        except Exception as exc:
            with self._lock:
                self._disabled = True
                self._stats["errors"] += 1
                self._stats["last_error"] = type(exc).__name__ + ": " + str(exc)[:200]
            LOG.warning("Winline DOM history unavailable: %s", exc)
        finally:
            if lock_file is not None:
                lock_file.close()


_recorder = None
_recorder_lock = threading.Lock()


def record_attempt(attempt, result):
    """Observer injected into the poller; private raw inputs never enter evidence."""
    inputs = result.pop("_dom_history_payload", None)
    if not enabled():
        return None
    if inputs is None:
        # Browser failures have no DOM; retain the failed attempt explicitly.
        inputs = {"html": "", "body_text": "", "visible_text": "",
                  "captured_wall": None, "page_instance_id": None,
                  "parser_path": "unavailable"}
    global _recorder
    with _recorder_lock:
        if _recorder is None:
            root = os.getenv("WINLINE_DOM_HISTORY_DIR") or str(
                Path(__file__).resolve().parents[1] / "runtime" / "artifacts"
                / "odds-winline" / "dom-history")
            _recorder = WinlineDOMHistory(
                root, interval_s=_number("WINLINE_DOM_HISTORY_INTERVAL_S", 15.0, 0.0),
                max_bytes=int(_number("WINLINE_DOM_HISTORY_MAX_MIB", 512, 1) * 1024 * 1024),
            )
            atexit.register(_recorder.close)
        recorder = _recorder
    return recorder.submit(attempt, inputs, result)
