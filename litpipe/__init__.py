"""litpipe: the shared core of the literature pipeline (strangler package, plan DEC-01).

New network, state, integrity and runner code lives here. The flat scripts at the repo root stay
the CLIs and are rewired onto this package stage by stage; their public names do not move.

Import rule: the flat scripts put the repo root on sys.path (it is sys.path[0] when a script runs),
so `import litpipe.outcomes` works from any of them without installing anything. Submodules
import nothing from the flat scripts except lit_util, so a script can import litpipe without a
cycle.
"""

__version__ = "0.2.0.dev0"
