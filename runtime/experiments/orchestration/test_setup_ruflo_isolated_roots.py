
import sys as _sys, pathlib as _pathlib
_sys.path.insert(0, str(_pathlib.Path(__file__).resolve().parent))  # соседи по каталогу эксперимента
import json
import os
import sqlite3
import stat
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import setup_ruflo_isolated_roots as isolated_roots


def create_agentdb(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        for table in isolated_roots.REQUIRED_AGENTDB_TABLES:
            connection.execute(f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY)')
        connection.commit()
    finally:
        connection.close()


class IsolatedRootsTests(unittest.TestCase):
    def test_setup_initializes_distinct_agentdb_files(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)

            def fake_run(command, cwd, check):
                self.assertTrue(check)
                self.assertTrue(Path(cwd).is_relative_to(base))
                create_agentdb(Path(command[command.index("--path") + 1]))

            with mock.patch.object(isolated_roots, "BASE", base), mock.patch.object(
                isolated_roots,
                "SLOTS",
                {"orchestration1": 3301, "orchestration2": 3302},
            ), mock.patch.object(isolated_roots.shutil, "which", return_value="ruflo"), mock.patch.object(
                isolated_roots.subprocess, "run", side_effect=fake_run
            ):
                isolated_roots.setup_roots()
                report = isolated_roots.verify_roots()

            self.assertEqual(len(report), 2)
            inodes = set()
            for slot in ("orchestration1", "orchestration2"):
                root = base / slot
                memory = root / ".swarm" / "memory.db"
                board = root / ".claude-flow" / "swarm-tasks.json"
                config = json.loads((root / "claude-flow.config.json").read_text())
                inodes.add(memory.stat().st_ino)
                self.assertEqual(stat.S_IMODE(memory.stat().st_mode), 0o600)
                self.assertEqual(stat.S_IMODE(board.stat().st_mode), 0o600)
                self.assertEqual(config["autopilot"]["timeoutMinutes"], 30)
            self.assertEqual(len(inodes), 2)

    def test_setup_preserves_existing_agentdb(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            root = base / "orchestration1"
            memory = root / ".swarm" / "memory.db"
            memory.parent.mkdir(parents=True)
            create_agentdb(memory)
            original_inode = memory.stat().st_ino

            with mock.patch.object(isolated_roots, "BASE", base), mock.patch.object(
                isolated_roots, "SLOTS", {"orchestration1": 3301}
            ), mock.patch.object(isolated_roots.subprocess, "run") as run:
                isolated_roots.setup_roots()
                isolated_roots.verify_roots()

            run.assert_not_called()
            self.assertEqual(memory.stat().st_ino, original_inode)

    def test_repair_preserves_invalid_database_backup(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            root = base / "orchestration1"
            memory = root / ".swarm" / "memory.db"
            memory.parent.mkdir(parents=True)
            connection = sqlite3.connect(memory)
            connection.execute("CREATE TABLE metadata (id INTEGER PRIMARY KEY)")
            connection.commit()
            connection.close()

            def fake_run(command, cwd, check):
                create_agentdb(Path(command[command.index("--path") + 1]))

            with mock.patch.object(isolated_roots, "BASE", base), mock.patch.object(
                isolated_roots, "SLOTS", {"orchestration1": 3301}
            ), mock.patch.object(isolated_roots.shutil, "which", return_value="ruflo"), mock.patch.object(
                isolated_roots.subprocess, "run", side_effect=fake_run
            ):
                isolated_roots.setup_roots(repair_invalid=True)
                isolated_roots.verify_roots()

            backups = list((root / ".swarm" / "backups").glob("invalid-memory-*.db"))
            self.assertEqual(len(backups), 1)
            connection = sqlite3.connect(backups[0])
            try:
                tables = {
                    row[0]
                    for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
            finally:
                connection.close()
            self.assertEqual(tables, {"metadata"})


if __name__ == "__main__":
    unittest.main()
