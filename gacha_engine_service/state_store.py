"""Shared contracts and errors for authoritative gacha runtime state."""

from __future__ import annotations


class GachaStateStoreError(Exception):
    """Raised when authoritative gacha state cannot be read or committed."""


class PityVersionConflict(Exception):
    """Raised when another pull updated a pity snapshot first."""

    def __init__(self, current_version: int) -> None:
        super().__init__("pity state version conflict")
        self.current_version = current_version


class PullOperationOwnershipLost(Exception):
    """Raised when a newer worker owns the processing lease."""

    def __init__(self) -> None:
        super().__init__("pull operation processing ownership was lost")
