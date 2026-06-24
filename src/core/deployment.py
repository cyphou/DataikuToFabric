"""Phase 12 — Deployment idempotency: content hashing and deployment manifest.

Provides:
- ``content_hash(data)`` — SHA-256 of serialised artifact content.
- ``DeploymentManifest`` — per-run record of what was deployed and when.
- ``DeploymentManifestStore`` — persist and reload manifests from disk.
- ``IdempotencyChecker`` — compare current artifact hashes against the last
  known manifest to decide which assets are stale and need re-deployment.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.logger import get_logger

logger = get_logger(__name__)


# ── Hash helpers ──────────────────────────────────────────────────────────────


def content_hash(data: str | bytes | dict | list) -> str:
    """Return a stable SHA-256 hex digest for any serialisable artifact.

    Args:
        data: Raw string/bytes content or a JSON-serialisable object.

    Returns:
        64-character lowercase hex string.
    """
    if isinstance(data, (dict, list)):
        raw = json.dumps(data, sort_keys=True, ensure_ascii=False).encode()
    elif isinstance(data, str):
        raw = data.encode()
    else:
        raw = data  # already bytes
    return hashlib.sha256(raw).hexdigest()


# ── Manifest model ────────────────────────────────────────────────────────────


class DeploymentEntry:
    """Record for a single deployed asset."""

    __slots__ = ("asset_id", "asset_type", "asset_name", "content_hash_value",
                 "deployed_at", "fabric_item_id", "skipped")

    def __init__(
        self,
        asset_id: str,
        asset_type: str,
        asset_name: str,
        content_hash_value: str,
        *,
        deployed_at: str | None = None,
        fabric_item_id: str | None = None,
        skipped: bool = False,
    ) -> None:
        self.asset_id = asset_id
        self.asset_type = asset_type
        self.asset_name = asset_name
        self.content_hash_value = content_hash_value
        self.deployed_at = deployed_at or datetime.now(timezone.utc).isoformat()
        self.fabric_item_id = fabric_item_id
        self.skipped = skipped

    def to_dict(self) -> dict[str, Any]:
        return {
            "asset_id": self.asset_id,
            "asset_type": self.asset_type,
            "asset_name": self.asset_name,
            "content_hash": self.content_hash_value,
            "deployed_at": self.deployed_at,
            "fabric_item_id": self.fabric_item_id,
            "skipped": self.skipped,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DeploymentEntry":
        return cls(
            asset_id=d["asset_id"],
            asset_type=d["asset_type"],
            asset_name=d["asset_name"],
            content_hash_value=d["content_hash"],
            deployed_at=d.get("deployed_at"),
            fabric_item_id=d.get("fabric_item_id"),
            skipped=d.get("skipped", False),
        )


class DeploymentManifest:
    """Complete record of a single deployment run.

    Attributes:
        run_id: Unique identifier for this deployment run.
        project_key: Dataiku project key being migrated.
        started_at: ISO-8601 UTC timestamp when the run started.
        finished_at: ISO-8601 UTC timestamp when the run finished (None if in progress).
        entries: Mapping of asset_id → DeploymentEntry.
    """

    def __init__(
        self,
        run_id: str,
        project_key: str,
        *,
        started_at: str | None = None,
        finished_at: str | None = None,
    ) -> None:
        self.run_id = run_id
        self.project_key = project_key
        self.started_at = started_at or datetime.now(timezone.utc).isoformat()
        self.finished_at = finished_at
        self.entries: dict[str, DeploymentEntry] = {}

    # ── Entry management ──────────────────────────────────────

    def add_entry(self, entry: DeploymentEntry) -> None:
        """Register a deployed (or skipped) asset entry."""
        self.entries[entry.asset_id] = entry

    def get_entry(self, asset_id: str) -> DeploymentEntry | None:
        """Retrieve a specific entry by asset ID."""
        return self.entries.get(asset_id)

    def mark_finished(self) -> None:
        """Record the completion timestamp."""
        self.finished_at = datetime.now(timezone.utc).isoformat()

    # ── Statistics ────────────────────────────────────────────

    @property
    def deployed_count(self) -> int:
        return sum(1 for e in self.entries.values() if not e.skipped)

    @property
    def skipped_count(self) -> int:
        return sum(1 for e in self.entries.values() if e.skipped)

    @property
    def total_count(self) -> int:
        return len(self.entries)

    # ── Serialisation ─────────────────────────────────────────

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "project_key": self.project_key,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "deployed_count": self.deployed_count,
            "skipped_count": self.skipped_count,
            "total_count": self.total_count,
            "entries": {aid: e.to_dict() for aid, e in self.entries.items()},
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DeploymentManifest":
        manifest = cls(
            run_id=d["run_id"],
            project_key=d["project_key"],
            started_at=d.get("started_at"),
            finished_at=d.get("finished_at"),
        )
        for entry_dict in d.get("entries", {}).values():
            manifest.add_entry(DeploymentEntry.from_dict(entry_dict))
        return manifest


# ── Manifest store ────────────────────────────────────────────────────────────


class DeploymentManifestStore:
    """Persist and reload deployment manifests from a directory.

    Layout::

        <manifest_dir>/
            deployment_<run_id>.json
            latest.json   ← symlink / copy of the most recent run
    """

    LATEST_FILENAME = "latest.json"

    def __init__(self, manifest_dir: Path) -> None:
        self.manifest_dir = Path(manifest_dir)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)

    def save(self, manifest: DeploymentManifest) -> Path:
        """Write manifest to disk. Returns the written file path."""
        path = self.manifest_dir / f"deployment_{manifest.run_id}.json"
        data = json.dumps(manifest.to_dict(), indent=2)
        path.write_text(data, encoding="utf-8")

        # Keep a 'latest' pointer
        latest_path = self.manifest_dir / self.LATEST_FILENAME
        latest_path.write_text(data, encoding="utf-8")

        logger.info(
            "manifest_saved",
            run_id=manifest.run_id,
            path=str(path),
            deployed=manifest.deployed_count,
            skipped=manifest.skipped_count,
        )
        return path

    def load(self, run_id: str) -> DeploymentManifest | None:
        """Load a manifest by run ID. Returns None if not found."""
        path = self.manifest_dir / f"deployment_{run_id}.json"
        if not path.exists():
            return None
        return DeploymentManifest.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def load_latest(self) -> DeploymentManifest | None:
        """Load the most recent manifest. Returns None if no manifest exists."""
        latest_path = self.manifest_dir / self.LATEST_FILENAME
        if not latest_path.exists():
            return None
        return DeploymentManifest.from_dict(
            json.loads(latest_path.read_text(encoding="utf-8"))
        )

    def list_runs(self) -> list[str]:
        """Return sorted list of run IDs with saved manifests (oldest first)."""
        run_ids = []
        for p in self.manifest_dir.glob("deployment_*.json"):
            run_ids.append(p.stem.removeprefix("deployment_"))
        return sorted(run_ids)


# ── Idempotency checker ───────────────────────────────────────────────────────


class IdempotencyChecker:
    """Decide whether an artifact needs re-deployment by comparing content hashes.

    Usage::

        checker = IdempotencyChecker(store)
        checker.load_previous()

        if checker.needs_deploy(asset_id, current_content):
            ... deploy ...
            checker.record_deployed(asset_id, asset_type, name, current_content, fabric_item_id)
        else:
            checker.record_skipped(asset_id, asset_type, name, current_content)
    """

    def __init__(self, store: DeploymentManifestStore, run_id: str, project_key: str) -> None:
        self.store = store
        self.run_id = run_id
        self.project_key = project_key
        self._previous: dict[str, str] = {}  # asset_id → previous content hash
        self.current_manifest = DeploymentManifest(run_id, project_key)

    def load_previous(self) -> int:
        """Load previous hashes from the latest manifest. Returns number loaded."""
        manifest = self.store.load_latest()
        if not manifest:
            logger.info("no_previous_manifest", run_id=self.run_id)
            return 0
        self._previous = {
            aid: entry.content_hash_value
            for aid, entry in manifest.entries.items()
            if not entry.skipped
        }
        logger.info("previous_manifest_loaded", run_id=manifest.run_id,
                    assets_known=len(self._previous))
        return len(self._previous)

    def needs_deploy(self, asset_id: str, content: str | bytes | dict | list) -> bool:
        """Return True if the artifact content has changed since last deployment."""
        current = content_hash(content)
        previous = self._previous.get(asset_id)
        if previous is None:
            logger.debug("needs_deploy_new_asset", asset_id=asset_id)
            return True
        changed = current != previous
        if not changed:
            logger.debug("needs_deploy_unchanged", asset_id=asset_id)
        return changed

    def record_deployed(
        self,
        asset_id: str,
        asset_type: str,
        asset_name: str,
        content: str | bytes | dict | list,
        fabric_item_id: str | None = None,
    ) -> None:
        """Record a successful deployment in the current manifest."""
        entry = DeploymentEntry(
            asset_id=asset_id,
            asset_type=asset_type,
            asset_name=asset_name,
            content_hash_value=content_hash(content),
            fabric_item_id=fabric_item_id,
            skipped=False,
        )
        self.current_manifest.add_entry(entry)

    def record_skipped(
        self,
        asset_id: str,
        asset_type: str,
        asset_name: str,
        content: str | bytes | dict | list,
    ) -> None:
        """Record an asset that was skipped because it was unchanged."""
        entry = DeploymentEntry(
            asset_id=asset_id,
            asset_type=asset_type,
            asset_name=asset_name,
            content_hash_value=content_hash(content),
            skipped=True,
        )
        self.current_manifest.add_entry(entry)

    def finalize(self) -> Path:
        """Mark the run finished and persist the manifest. Returns saved path."""
        self.current_manifest.mark_finished()
        return self.store.save(self.current_manifest)
