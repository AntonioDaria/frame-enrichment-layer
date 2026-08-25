"""Runtime settings: the topic names and consumer group the worker runs against."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Config:
    """The few things that differ between environments, and nothing else.

    Frozen because these are read once at startup and never change; a worker that
    could be reconfigured mid-flight would be a source of bugs rather than
    flexibility.

    Plain stdlib rather than pydantic-settings. There are four strings here, all
    with defaults, and none of them needs validation beyond being a string, so a
    settings library would be a dependency bought for nothing.
    """

    frames_topic: str = "frames"
    colours_topic: str = "colours"
    enriched_topic: str = "enriched-frames"
    group_id: str = "enrichment-workers"

    @classmethod
    def from_env(cls) -> "Config":
        """Build the config from the environment, falling back to the defaults.

        The defaults are the ones the in-memory demo runs on, so the service
        starts with no environment set at all. A real deployment overrides the
        topic names per environment.

        group_id is what a real broker uses to spread partitions across the
        horizontally scaled workers and to remember their offsets. The in-memory
        stub has a single consumer and no offsets, so it ignores the value; it is
        carried here because the deployed service needs it and leaving it out
        would misrepresent what the service is configured by.
        """
        return cls(
            frames_topic=os.environ.get("FRAMES_TOPIC", cls.frames_topic),
            colours_topic=os.environ.get("COLOURS_TOPIC", cls.colours_topic),
            enriched_topic=os.environ.get("ENRICHED_TOPIC", cls.enriched_topic),
            group_id=os.environ.get("GROUP_ID", cls.group_id),
        )
