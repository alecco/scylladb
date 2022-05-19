#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
from typing import Protocol
from typing import Callable, Awaitable, List, Dict, Optional
import asyncio
import logging

Artifact = Awaitable


class Suite(Protocol):
    suite_key: str


class ArtifactRegistry:
    """ A global to all tests registry of all external
    resources and artifacts, such as open ports, directories with temporary
    files or running auxiliary processes. Contains a map of all glboal
    resources, and as soon as the resource is taken by the test it is
    reprsented in the artifact registry. """

    def __init__(self) -> None:
        self.suite_artifacts: Dict[str, List[Artifact]] = {}
        self.exit_artifacts: Dict[str, List[Artifact]] = {}

    async def cleanup_before_exit(self) -> None:
        logging.info("Cleaning up before exit...")
        for key in self.suite_artifacts:
            for artifact in self.suite_artifacts[key]:
                artifact.close()  # type: ignore
            await asyncio.gather(*self.suite_artifacts[key], return_exceptions=True)
        self.suite_artifacts = {}
        for key in self.exit_artifacts:
            await asyncio.gather(*self.exit_artifacts[key], return_exceptions=True)
        self.exit_artifacts = {}
        logging.info("Done cleaning up before exit...")

    async def cleanup_after_suite(self, suite: Suite, failed: bool) -> None:
        """At the end of the suite, delete all suite artifacts, if the suite
        succeeds, and, in all cases, delete all exit artifacts produced by
        the suite. Executing exit artifacts right away is a good idea
        because it kills running processes and frees their resources
        early."""
        key = suite.suite_key
        logging.info("Cleaning up after suite %s...", key)
        # Only drop suite artifacts if the suite executed successfully.
        if not failed and key in self.suite_artifacts:
            await asyncio.gather(*self.suite_artifacts[key])
            del self.suite_artifacts[key]
        if key in self.exit_artifacts:
            await asyncio.gather(*self.exit_artifacts[key])
            del self.exit_artifacts[key]
        logging.info("Done cleaning up after suite %s...", key)

    def add_suite_artifact(self, suite: Suite, artifact: Callable[[], Artifact]) -> None:
        key = suite.suite_key
        self.suite_artifacts.setdefault(key, []).append(artifact())

    def add_exit_artifact(self, suite: Optional[Suite], artifact: Callable[[], Artifact]):
        key = suite.suite_key if suite else "__exit__"
        self.exit_artifacts.setdefault(key, []).append(artifact())
