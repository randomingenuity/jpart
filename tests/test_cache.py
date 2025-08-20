import os
import io
import uuid

import riu.utility

import jpart.cache


class Test(object):
    def test_add(self):

        # Register first resource

        fault_cb = None
        cache = jpart.cache.CachedResources(fault_cb)

        filename1 = 'test-file1'

        # The resources should implement a `close()` method
        resource1 = io.StringIO()

        cache._add(filename1, resource1)


        # Register second resource

        filename2 = 'test-file2'

        # The resources should implement a `close()` method
        resource2 = io.StringIO()

        cache._add(filename2, resource2)


        # Check registered

        len_ = len(cache._lru)

        assert \
            len_ == 2, \
            "Expected two entries in LRU: ({})".format(len_)

        len_ = len(cache._index)

        assert \
            len_ == 2, \
            "Expected two entries in index: ({})".format(len_)

        actual_keys = sorted(cache._index.keys())
        expected_keys = sorted([filename1, filename2])

        assert \
            actual_keys == expected_keys, \
            "Cached filenames not correct:\n" \
                "{} != {}".format(actual_keys, expected_keys)


        # Fill all remaining slots (we've already added two)

        expected_index = {
            filename1: resource1,
            filename2: resource2,
        }

        for _ in range(jpart.cache._MAX_CACHED_RESOURCES - 2):
            u = str(uuid.uuid4())
            r = io.StringIO()

            expected_keys.append(u)
            expected_index[u] = r

            cache._add(u, r)


        assert \
            cache._index == expected_index, \
            "Index not correct."

        assert \
            cache._lru == expected_keys, \
            "LRU not correct."


        # Push one more

        filename3 = str(uuid.uuid4())
        expected_keys.append(filename3)

        resource3 = io.StringIO()
        expected_index[filename3] = resource3

        cache._add(filename3, resource3)


        # Make sure the oldest item was disposed but that nothing else was
        # touched

        del expected_keys[0]
        del expected_index[filename1]

        assert \
            cache._index == expected_index, \
            "Index not correct (after add)."

        assert \
            cache._lru == expected_keys, \
            "LRU not correct (after add)."


        # Dispose one

        cache._dispose_oldest()

        del expected_keys[0]
        del expected_index[filename2]

        assert \
            cache._index == expected_index, \
            "Index not correct (after dispose-one)."

        assert \
            cache._lru == expected_keys, \
            "LRU not correct (after dispose-one)."


        # Dispose all

        cache.dispose()

        assert \
            not cache._index, \
            "Index not empty (after dispose-one)."

        assert \
            not cache._lru, \
            "LRU not empty (after dispose-one)."


    test_dispose_oldest = test_add
    test_dispose = test_add


    def test_get_or_create(self):

        requested = {}
        def fault_cb(name):
            s = io.StringIO()
            requested[name] = s

            return s


        cache = jpart.cache.CachedResources(fault_cb)

        resource1 = cache.get_or_create('resource1')
        resource2 = cache.get_or_create('resource2')

        expected = {
            'resource1': resource1,
            'resource2': resource2,
        }

        assert \
            requested == expected, \
            "Requested resources not accurate:\n{}".format(requested)

    def test_default_fault_handler(self):

        with riu.utility.temp_path() as temp_path:

            filename = 'testfile'
            f = jpart.cache.default_fault_handler(temp_path, filename)

            try:
                test_content = "xyz"

                f.write(test_content)
                f.close()


                # Check output path

                entries = os.walk(temp_path)
                entries = list(entries)

                expected = [
                    (
                        temp_path,
                        [],
                        [
                            filename,
                        ]
                    )
                ]

                assert \
                    entries == expected, \
                    "Written files not correct:\n" \
                        "ACTUAL:\n" \
                        "{}\n\n" \
                        "EXPECTED:\n" \
                        "{}".format(
                        entries, expected)


                # Check content

                with open(filename) as f:
                    actual = f.read()

                assert \
                    actual == test_content, \
                    "File content not correct: [{}]".format(actual)

            finally:
                os.unlink(filename)
