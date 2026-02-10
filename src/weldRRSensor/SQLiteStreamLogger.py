import os
import time
import json
import sqlite3
import threading
import queue
from dataclasses import dataclass
import numpy as np


@dataclass
class TopicInfo:
    """Topic configuration stored in-memory so we can re-register after rollover."""
    name: str
    dtype: str
    shape: tuple  # e.g. (H, W)


class SQLiteStreamLogger:
    """
    SQLite 'rosbag-like' logger for callback-based producers with rollover.

    Key properties:
      - Callback never blocks (drop policy when queue is full)
      - Background writer thread batches inserts and commits periodically
      - WAL mode for good performance
      - Rollover to a new .db3 file by time and/or size

    File layout:
      <out_dir>/<base_name>/
          metadata.json
          <base_name>_00000.db3
          <base_name>_00001.db3
          ...

    Notes:
      - Each segment has its own topics table and independent topic_id values.
      - metadata.json keeps a list of segment filenames and topic config for convenience.
    """

    def __init__(
        self,
        out_dir: str = None,
        base_name: str = "stream_bag",
        queue_max: int = 2000,
        batch_size: int = 256,
        commit_period_s: float = 0.05,
        drop_policy: str = "drop_newest",   # "drop_newest" or "drop_oldest"
        # Rollover controls:
        rollover_max_bytes: int | None = None,   # e.g. 2 * 1024**3 for ~2 GiB
        # Optional: force a SQLite WAL checkpoint on rollover (helps keep files tidy)
        checkpoint_on_rollover: bool = True,
        status_report_interval_s: float = 1.0,  # how often to print status reports to console
    ):
        self.out_dir = out_dir
        self.base_name = base_name
        self.queue_max = queue_max
        self.batch_size = batch_size
        self.commit_period_s = commit_period_s
        self.drop_policy = drop_policy
        self.rollover_max_bytes = rollover_max_bytes
        self.checkpoint_on_rollover = checkpoint_on_rollover
        self.status_report_interval_s = status_report_interval_s

        # Stable (global) topic registry (safe to read from callbacks)
        self._name_to_key: dict[str, int] = {}
        self._key_to_def: dict[int, TopicInfo] = {}
        self._next_key = 0

        # Segment state
        self._segment_idx = -1
        self._segment_dir = os.path.join(self.out_dir, self.base_name)
        self._current_db_path: str | None = None
        self._segment_start_perf = None

        # Threading/queue
        self._q = queue.Queue(maxsize=queue_max)
        self._stop = threading.Event()
        self._writer_thread = threading.Thread(target=self._writer_loop, daemon=True)

        # Stats
        self._dropped = 0
        self._enqueued = 0
        self._written = 0
        self._bytes_written = 0
        self._rollovers = 0
        self._stats_lock = threading.Lock()

        # Prepare folder + metadata
        os.makedirs(self._segment_dir, exist_ok=True)
        self._metadata_path = os.path.join(self._segment_dir, "metadata.json")
        self._metadata = {
            "base_name": self.base_name,
            "created_time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "segments": [],
            "topics": {},  # name -> {dtype, shape}
            "settings": {
                "queue_max": self.queue_max,
                "batch_size": self.batch_size,
                "commit_period_s": self.commit_period_s,
                "drop_policy": self.drop_policy,
                "enable_indexes": self.enable_indexes,
                "rollover_max_bytes": self.rollover_max_bytes,
                "checkpoint_on_rollover": self.checkpoint_on_rollover,
            },
        }
        self._write_metadata()

    # ----------------------------
    # Public API
    # ----------------------------

    def start_logging(self):
        """Start the writer thread (if not already started)."""
        if not self._writer_thread.is_alive():
            self._open_new_segment() # open first segment before starting thread (ensures DB ready for incoming data)
            self._writer_thread.start()
    
    def register_topic(self, name: str, dtype: str, shape: tuple):
        """
        Register a topic once. This will also register it in the *current* segment DB.
        Future rollover segments will automatically re-register all known topics.
        """
        if name in self._name_to_key:
            return self._name_to_key[name]
        
        key = self._next_key
        self._next_key += 1

        self._name_to_key[name] = key
        self._key_to_def[key] = TopicInfo(name=name, dtype=dtype, shape=tuple(shape))
        
        self._metadata["topics"][name] = {"dtype": dtype, "shape": list(shape)}
        self._write_metadata()

        return key

    def log_data(self, topic_name: str, data: np.ndarray, timestamp_ns: int | None = None) -> bool:
        """
        Called from callback. MUST be fast. Never blocks.
        Returns True if enqueued, False if dropped.
        """
        if not self._writer_thread.is_alive():
            raise RuntimeError("Writer thread not started. Call start_logging() before logging data.")

        if topic_name not in self._name_to_key:
            raise KeyError(f"Topic '{topic_name}' not registered (call register_topic first)")

        key = self._name_to_key[topic_name]  # stable forever
        info = self._key_to_def[key]
        if str(data.dtype) != info.dtype:
            raise ValueError(f"{topic_name}: expected dtype {info.dtype}, got {data.dtype}")
        if tuple(data.shape) != tuple(info.shape):
            raise ValueError(f"{topic_name}: expected shape {info.shape}, got {data.shape}")

        if timestamp_ns is None:
            timestamp_ns = time.time_ns()

        blob = data.tobytes(order="C")
        item = (key, timestamp_ns, blob)

        try:
            self._q.put(item, block=False)
            with self._stats_lock:
                self._enqueued += 1
            return True
        except queue.Full:
            if self.drop_policy == "drop_oldest":
                # Drop one old item to make room, then try insert once.
                try:
                    _ = self._q.get(block=False)
                except queue.Empty:
                    pass
                try:
                    self._q.put(item, block=False)
                    with self._stats_lock:
                        self._enqueued += 1
                        self._dropped += 1  # dropped an old item
                    return True
                except queue.Full:
                    with self._stats_lock:
                        self._dropped += 1
                    return False
            else:
                # drop_newest
                with self._stats_lock:
                    self._dropped += 1
                return False

    def get_stats(self) -> dict:
        with self._stats_lock:
            return {
                "segment_idx": self._segment_idx,
                "current_db": self._current_db_path,
                "queue_size": self._q.qsize(),
                "queue_max": self.queue_max,
                "enqueued": self._enqueued,
                "dropped": self._dropped,
                "written": self._written,
                "bytes_written": self._bytes_written,
                "rollovers": self._rollovers,
            }

    def close(self):
        """Stop writer after draining queue."""
        if not self._writer_thread.is_alive():
            raise RuntimeError("Writer thread not started. Call start_logging() before closing.")
        self._stop.set()
        self._writer_thread.join(timeout=10.0)
        # Writer thread closes DB connection.

    # ----------------------------
    # DB / Segment handling
    # ----------------------------

    def _segment_filename(self, idx: int) -> str:
        return f"{self.base_name}_{idx:05d}.db3"

    def _write_metadata(self):
        # Keep metadata writes small/atomic
        tmp = self._metadata_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._metadata, f, indent=2)
        os.replace(tmp, self._metadata_path)

    def _open_db(self, db_path: str):
        con = sqlite3.connect(db_path, timeout=30.0, check_same_thread=False)
        cur = con.cursor()

        # Pragmas for speed + reasonable safety
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA temp_store=MEMORY;")

        # Tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS topics (
            id INTEGER PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            dtype TEXT NOT NULL,
            shape TEXT NOT NULL
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            topic_id INTEGER NOT NULL,
            timestamp_ns INTEGER NOT NULL,
            data BLOB NOT NULL,
            FOREIGN KEY(topic_id) REFERENCES topics(id)
        );
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_topic_time ON messages(topic_id, timestamp_ns);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(timestamp_ns);")

        con.commit()
        return con, cur

    def _open_new_segment(self):
        """Close old DB (if any) and open a new segment DB; re-register topics."""
        # Close old connection
        if hasattr(self, "_con") and self._con is not None:
            try:
                # Optional checkpoint before close (keeps WAL from being separate/large)
                if self.checkpoint_on_rollover:
                    self._cur.execute("PRAGMA wal_checkpoint(FULL);")
                self._con.commit()
            finally:
                self._con.close()

        # New DB path
        self._segment_idx += 1
        fname = self._segment_filename(self._segment_idx)
        self._current_db_path = os.path.join(self._segment_dir, fname)

        # Open and init
        self._con, self._cur = self._open_db(self._current_db_path)
        self._segment_start_perf = time.perf_counter()

        # new segment, so clear topic_key -> topic_id cache (IDs are per-segment)
        self._seg_topic_id = {}  # stable topic_key -> segment topic_id (updated on rollover)

        # Update metadata segment list
        self._metadata["segments"].append(
            {
                "index": self._segment_idx,
                "filename": fname,
                "created_time_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            }
        )
        self._write_metadata()

    def _should_rollover(self) -> bool:
        """Check rollover conditions."""
        if self._current_db_path is None:
            return False

        # Size-based rollover (filesystem size)
        if self.rollover_max_bytes is not None:
            try:
                if os.path.getsize(self._current_db_path) >= self.rollover_max_bytes:
                    return True
            except FileNotFoundError:
                pass

        return False

    # ----------------------------
    # Writer loop
    # ----------------------------

    def _ensure_topic_id(self, topic_key: int) -> int:
        """
        Map stable topic_key to the SQLite topic_id in THIS database segment.
        Only writer thread calls this; safe and race-free.
        """
        if topic_key in self._seg_topic_id:
            return self._seg_topic_id[topic_key]

        tdef = self._key_to_def[topic_key]
        self._cur.execute(
            "INSERT OR IGNORE INTO topics(name, dtype, shape) VALUES (?, ?, ?);",
            (tdef.name, tdef.dtype, str(tdef.shape))
        )
        self._cur.execute("SELECT id FROM topics WHERE name=?;", (tdef.name,))
        tid = self._cur.fetchone()[0]
        self._seg_topic_id[topic_key] = tid
        return tid

    def _writer_loop(self):
        """
        The only thread touching SQLite writes.
        Also performs rollover checks right before committing a batch.
        """
        batch = []
        last_commit = time.perf_counter()
        last_report = time.perf_counter()

        # Ensure DB connection exists
        if not hasattr(self, "_con") or self._con is None:
            print("[logger] Warning: DB connection not found at writer start, opening new segment.")
            self._open_new_segment()

        while not (self._stop.is_set() and self._q.empty()):
            # Pull items quickly
            try:
                item = self._q.get(timeout=0.02)
                item_actual = (self._ensure_topic_id(item[0]), item[1], item[2])
                batch.append(item_actual)
            except queue.Empty:
                pass

            now = time.perf_counter()
            # commit if batch full
            # or if commit_period_s elapsed since last commit (and we have something to commit)
            should_commit = (len(batch) >= self.batch_size) or ((now - last_commit) >= self.commit_period_s and batch)

            if should_commit:
                # Write batch
                self._cur.executemany(
                    "INSERT INTO messages(topic_id, timestamp_ns, data) VALUES (?, ?, ?);",
                    batch
                )
                self._con.commit()

                # Update stats
                bytes_this = sum(len(x[2]) for x in batch)
                with self._stats_lock:
                    self._written += len(batch)
                    self._bytes_written += bytes_this

                batch.clear()

                # Rollover BEFORE appending the next batch if conditions met
                # (keeps batches whole and boundaries clean)
                if self._should_rollover():
                    self._open_new_segment()
                    with self._stats_lock:
                        self._rollovers += 1

                last_commit = now

            # Optional periodic console report
            if (now - last_report) >= self.status_report_interval_s:
                st = self.get_stats()
                mb = st["bytes_written"] / (1024 * 1024)
                print(
                    f"[logger] seg={st['segment_idx']} q={st['queue_size']}/{st['queue_max']} "
                    f"enq={st['enqueued']} drop={st['dropped']} written={st['written']} "
                    f"bytes={mb:.1f} MiB rollovers={st['rollovers']}"
                )
                last_report = now

        # Final flush
        if batch:
            if self._should_rollover():
                self._open_new_segment()
                with self._stats_lock:
                    self._rollovers += 1

            self._cur.executemany(
                "INSERT INTO messages(topic_id, timestamp_ns, data) VALUES (?, ?, ?);",
                batch
            )
            self._con.commit()
            bytes_this = sum(len(x[2]) for x in batch)
            with self._stats_lock:
                self._written += len(batch)
                self._bytes_written += bytes_this

        # Optional checkpoint on shutdown
        try:
            if self.checkpoint_on_rollover:
                self._cur.execute("PRAGMA wal_checkpoint(FULL);")
            self._con.commit()
        finally:
            self._con.close()
            self._con = None
            self._cur = None