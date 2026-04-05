from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from aiteams.review_policy_migration import migrate_review_policies_in_connection


def backup_suffix() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def create_backup(db_path: Path) -> Path:
    backup_path = db_path.with_name(f"{db_path.stem}.review-policies-backup.{backup_suffix()}{db_path.suffix}")
    shutil.copy2(db_path, backup_path)
    return backup_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate legacy review_policies rows to the new rules schema.")
    parser.add_argument("db_path", nargs="?", default="data/platform.db", help="Path to the platform metadata sqlite db.")
    parser.add_argument("--dry-run", action="store_true", help="Scan and report changes without writing to the database.")
    parser.add_argument("--backup", action="store_true", help="Create a sqlite backup file before writing changes.")
    parser.add_argument("--verbose", action="store_true", help="Include changed row ids and names in the output.")
    args = parser.parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    if not db_path.exists():
        print(json.dumps({"ok": False, "error": "database_not_found", "db_path": str(db_path)}, ensure_ascii=False))
        return 1

    backup_path: Path | None = None
    if args.backup and not args.dry_run:
        backup_path = create_backup(db_path)

    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    try:
        result = migrate_review_policies_in_connection(
            connection,
            dry_run=args.dry_run,
            commit=not args.dry_run,
        )
    finally:
        connection.close()

    payload = {
        "ok": True,
        "db_path": str(db_path),
        "dry_run": bool(args.dry_run),
        "backup_path": str(backup_path) if backup_path else None,
        "scanned": int(result.get("scanned") or 0),
        "migrated": int(result.get("migrated") or 0),
        "error_count": len(result.get("errors") or []),
    }
    if args.verbose:
        payload["changed_items"] = list(result.get("changed_items") or [])
        payload["errors"] = list(result.get("errors") or [])
    print(json.dumps(payload, ensure_ascii=False, indent=2 if args.verbose else None))
    return 0 if not result.get("errors") else 2


if __name__ == "__main__":
    raise SystemExit(main())
