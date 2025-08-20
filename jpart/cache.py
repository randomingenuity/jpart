import os
import logging

import jpart.utility

_MAX_CACHED_RESOURCES = 100

_LOGGER = logging.getLogger(__name__)


def default_fault_handler(output_path, name):

    filepath = os.path.join(output_path, name)
    output_path = os.path.dirname(filepath)
    filename = os.path.basename(filepath)

    _LOGGER.info("Opening: [{}]".format(filename))

    if os.path.exists(output_path) is False:
        os.makedirs(output_path)

    f = jpart.utility.RESOURCE_APPEND_OPENER(filepath)
    return f


class CachedResources(object):
    def __init__(self, fault_cb):
        self._lru = []
        self._index = {}

        self._fault_cb = fault_cb

    @property
    def lru(self):
        return self._lru

    @property
    def index(self):
        return self._index

    def _dispose_oldest(self):

        if not self._lru:
            return False

        name, self._lru = self._lru[0], self._lru[1:]
        resource = self._index.pop(name)

        _LOGGER.info("Closing: [{}]".format(name))
        resource.close()

        return True

    def dispose(self):

        if not self._index:
            return False

        # Designed to avoid infinite looping
        for _ in range(_MAX_CACHED_RESOURCES):
            last_result = self._dispose_oldest()

            if last_result is False:
                break

        assert \
            not self._index, \
            "Not all resources were cleaned-up."

        return True

    def get_or_create(self, name):
        """Return a resource for the given name."""


        # Check if cached

        try:
            resource = self._index[name]

        except KeyError:
            pass

        else:
            return resource


        # Retrieve resource

        resource = self._fault_cb(name)

        # Register resource

        self._add(name, resource)


        return resource

    def _add(self, name, resource):

        assert \
            name not in self._index, \
            "A resource with name [{}] is already cached.".format(name)


        # Make sure there's space

        if len(self._index) >= _MAX_CACHED_RESOURCES:
            self._dispose_oldest()


        # Add to cache

        self._lru.append(name)
        self._index[name] = resource
