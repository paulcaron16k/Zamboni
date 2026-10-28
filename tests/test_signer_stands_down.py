# SPDX-License-Identifier: Apache-2.0
"""The S3 signer mirror stands down where the library has the fix (#241).

`tests/test_signer.py` exercises the mirror and skips, as a module, wherever
the installed PyIceberg already signs once per signer -- which is the fork this
repository pins. So on the pinned build nothing there runs, and nothing showed
that `install()` leaves the library alone. Proving each mirror stands down on
the library form of its fix is what the development pin is for, so this is the
half that runs there, and skips on stock where the other half runs.
"""

from __future__ import annotations

import pytest

from zamboni import signer


@pytest.mark.skipif(
    not signer.library_signs_once(),
    reason="stock PyIceberg: the mirror installs here, tests/test_signer.py covers it",
)
def test_install_leaves_the_library_alone_where_it_signs_once_itself():
    from pyiceberg.io import fsspec

    before = dict(fsspec.SCHEME_TO_FS)
    signer.install()
    assert before == fsspec.SCHEME_TO_FS
    assert all(factory is not signer._s3_signed_once for factory in fsspec.SCHEME_TO_FS.values())
