import threading
_thread_lock = threading.Lock()
_cache_flag = False
def cache(flag = True):
    global _cache_flag
    _cache_flag = flag

from kepler.md import MD