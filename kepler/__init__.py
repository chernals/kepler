# Create a global lock for multithread caching
from threading import Lock
_thread_lock = Lock()

# Switch for the cache prefectching
_cache_flag = False
def cache(flag = True):
    global _cache_flag
    _cache_flag = flag

# Expose as little as possible for the autocompletion
__dict__ = {}
from kepler.md import MD