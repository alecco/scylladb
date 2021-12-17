import asyncio
import logging

class ArtefactRegistry:
    """ A global to all tests registry of all external
    resources and artefacts, such as open ports, directories with temporary
    files or running auxiliary processes. Contains a map of all glboal
    resources, and as soon as the resource is taken by the test it is
    reprsented in the artefact registry. """

    def __init__(self):
        self.suite_artefacts = {}
        self.exit_artefacts = []

    async def cleanup_before_exit(self):
        logging.critical("Cleaning up before exit...")
        await asyncio.gather(*self.exit_artefacts)
        self.exit_artefacts = None
        logging.critical("Done cleaning up before exit...")

    async def cleanup_after_suite(self, suite):
        logging.critical("Cleaning up after suite %s...", suite.name)
        if suite in self.suite_artefacts:
            await asyncio.gather(*self.suite_artefacts[suite])
            del self.suite_artefacts[suite]
        logging.critical("Done cleaning up after suite %s...", suite.name)


    def add_suite_artefact(self, suite, artefact):
        if suite in self.suite_artefacts:
            self.suite_artefacts[suite].append(artefact())
        else:
            self.suite_artefacts[suite] = [artefact()]

    def add_exit_artefact(self, artefact):
        self.exit_artefacts.append(artefact())

