"""branchio tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_branchio import streams


class Tapbranchio(Tap):
    """branchio tap class."""

    name = "tap-branchio"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "branch_key",
            th.StringType,
            required=True,
        ),
        th.Property(
            "branch_secret",
            th.StringType,
            required=True,
            secret=True,  
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),

    ).to_dict()

    def discover_streams(self) -> list[streams.branchioStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.EoBranchCtaView(self),
            streams.EoClick(self),
            streams.EoCommerceEvent(self),
            streams.EoContentEvent(self),
            streams.EoInstall(self),
            streams.EoOpen(self),
            streams.EoReinstall(self),
            streams.EoUserLifecycleEvent(self),
        ]


if __name__ == "__main__":
    Tapbranchio.cli()
