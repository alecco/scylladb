#
# Copyright (C) 2022-present ScyllaDB
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
import asyncio
import logging


class ArtifactRegistry:
    """ A global to all tests registry of all external
    resources and artifacts, such as open ports, directories with temporary
    files or running auxiliary processes. Contains a map of all glboal
    resources, and as soon as the resource is taken by the test it is
    reprsented in the artifact registry. """

    def __init__(self):
        self.suite_artifacts = {}
        self.exit_artifacts = []

    async def cleanup_before_exit(self):
        logging.critical("Cleaning up before exit...")
        await asyncio.gather(*self.exit_artifacts)
        self.exit_artifacts = None
        logging.critical("Done cleaning up before exit...")

    async def cleanup_after_suite(self, suite):
        logging.critical("Cleaning up after suite %s...", suite.name)
        if suite in self.suite_artifacts:
            await asyncio.gather(*self.suite_artifacts[suite])
            del self.suite_artifacts[suite]
        logging.critical("Done cleaning up after suite %s...", suite.name)

    def add_suite_artifact(self, suite, artifact):
        if suite in self.suite_artifacts:
            self.suite_artifacts[suite].append(artifact())
        else:
            self.suite_artifacts[suite] = [artifact()]

    def add_exit_artifact(self, artifact):
        self.exit_artifacts.append(artifact())
