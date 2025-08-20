
import jpart.filter


class TestBaseFilter(object):
    def test_get_value(self):
        bf = jpart.filter.BaseFilter()

        data = {
            'aa': 11,
            'bb': 22,
            'cc': 33,
        }

        actual = bf.get_value('aa', data)

        assert \
            actual == 11, \
            "Expected value not correct (1): {}".format(actual)

        actual = bf.get_value('bb', data)

        assert \
            actual == 22, \
            "Expected value not correct (2): {}".format(actual)

        actual = bf.get_value('cc', data)

        assert \
            actual == 33, \
            "Expected value not correct (3): {}".format(actual)

