import os


RESOURCE_APPEND_OPENER = lambda mode: open(mode, 'a')


_OUTPUT_SUFFIX = '.jsonl'

def construct_output_filename(phrases):

    filename = '-'.join(phrases) + _OUTPUT_SUFFIX
    return filename
