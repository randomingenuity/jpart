
import jpart.utility


class Test(object):
    def test_construct_output_filename(self):
        parts = ['aa', 'bb', 'cc']
        filename = jpart.utility.construct_output_filename(parts)

        expected = 'aa-bb-cc.jsonl'

        assert \
            filename == expected, \
            "Filename not correct: [{}]".format(filename)
