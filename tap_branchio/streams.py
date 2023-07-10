"""Stream type classes for tap-branchio."""

from __future__ import annotations

from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_branchio.client import branchioStream

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class EoBranchCtaView(branchioStream):
    """Define custom stream."""

    name = "eo_branch_cta_view"


class EoClick(branchioStream):
    """Define custom stream."""

    name = "eo_click"


class EoCommerceEvent(branchioStream):
    """Define custom stream."""

    name = "eo_commerce_event"


class EoContentEvent(branchioStream):
    """Define custom stream."""

    name = "eo_content_event"


class EoInstall(branchioStream):
    """Define custom stream."""

    name = "eo_install"


class EoOpen(branchioStream):
    """Define custom stream."""

    name = "eo_open"


class EoReinstall(branchioStream):
    """Define custom stream."""

    name = "eo_reinstall"


class EoUserLifecycleEvent(branchioStream):
    """Define custom stream."""

    name = "eo_user_lifecycle_event"
