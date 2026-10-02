"""Small helpers so the tests run on every supported Python version (3.7+)."""

import shutil
import sys
import tempfile


class TemporaryDirectory(tempfile.TemporaryDirectory):
    """tempfile.TemporaryDirectory that never fails on cleanup (Windows may still hold a
    SQLite file open for a moment). Python 3.10+ has ``ignore_cleanup_errors`` for this."""

    def __init__(self):
        if sys.version_info >= (3, 10):
            super().__init__(ignore_cleanup_errors=True)
        else:
            super().__init__()

    def cleanup(self):
        if sys.version_info >= (3, 10):
            return super().cleanup()
        self._finalizer.detach()
        shutil.rmtree(self.name, ignore_errors=True)
