"""Run gitupdater to check for and install updates to uvbekutils and bekgoogle."""
import os
import sys

try:
    import gitupdater
except ImportError:
    sys.path.append(os.path.expanduser("~/Dropbox/Postcard Files/"))
    import gitupdater
