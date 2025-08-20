import os
import io
import json
import functools

import riu.journal
import riu.utility

import jpart.rule
import jpart.filter


class _MaskedRule(jpart.rule.Rule):
    def __init__(self):
        """Prevents the default constructor from running and invoking
        the method that we're trying to test.
        """

        pass


class Test(object):
    def test_process_parts(self):

        rule = _MaskedRule()

        filter_mappings = {}

        rule_raw = [
            'field1',
            'field2',
        ]

        materialized_parts = rule._process_parts(filter_mappings, rule_raw)

        assert \
            materialized_parts == rule_raw, \
            "Expected no net change in the processed parts:\n{}".format(
                materialized_parts)

    def test_process_parts__filters(self):

        # Process one rule part

        class _TestFilter(jpart.filter.BaseFilter):
            pass

        filter_mappings = {
            'test_filter': _TestFilter,
        }

        rule_raw = [
            'field1',
            ('field2', '!test_filter'),
        ]

        rule = _MaskedRule()
        materialized_parts = rule._process_parts(filter_mappings, rule_raw)

        test_filter = _TestFilter()

        assert \
            len(materialized_parts) == 2, \
            "Expected two parts:\n{}".format(materialized_parts)

        assert \
            len(materialized_parts[1]) == 2, \
            "Expected 2-tuple in second part:\n{}".format(materialized_parts[1])

        expected = [
            'field1',
            ('field2', materialized_parts[1][1]),
        ]

        assert \
            materialized_parts == expected, \
            "Expected no net change in the processed parts:\n{}".format(
                materialized_parts)

    def test_get_value_with_rule_part(self):

        filter_mappings = {}

        rule_raw = [
            'field1',
            'field2',
        ]

        rule_name = 'test rule'
        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)

        record = {
            'field1': 'aa',
            'field2': 'bb',
            'field3': 'cc',
        }

        part = 'field2'

        value = rule._get_value_with_rule_part(part, record)

        assert \
            value == 'bb', \
            "Value not expected: [{}]".format(value)

    def test_get_value_with_rule_part__filter__no_override(self):

        # Process one rule part

        class _TestFilter(jpart.filter.BaseFilter):
            pass

        filter_mappings = {
            'test_filter': _TestFilter,
        }

        rule_raw = [
            'field1',
            ('field2', '!test_filter'),
        ]

        rule_name = 'test rule'
        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)

        record = {
            'field1': 'aa',
            'field2': 'bb',
            'field3': 'cc',
        }

        part = 'field2'

        value = rule._get_value_with_rule_part(part, record)

        assert \
            value == 'bb', \
            "Value not expected: [{}]".format(value)

    def test_get_value_with_rule_part__filter__get_value(self):

        # Process one rule part

        class _TestFilter(jpart.filter.BaseFilter):
            def get_value(self, name, record):
                return '{}:xyz'.format(name)

        filter_mappings = {
            'test_filter': _TestFilter,
        }

        rule_raw = [
            'field1',
            ('field2', '!test_filter'),
        ]

        rule_name = 'test rule'
        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)

        test_filter = _TestFilter()
        part = ('field2', test_filter)

        record = {}
        value = rule._get_value_with_rule_part(part, record)

        assert \
            value == 'field2:xyz', \
            "Value not expected: [{}]".format(value)


    def test_get_value_with_rule_part__filter__does_qualify(self):

        # Process one rule part

        class _TestFilter(jpart.filter.BaseFilter):
            def does_qualify(self, name, value):
                if value == 'ee':
                    return True
                else:
                    return False

        filter_mappings = {
            'test_filter': _TestFilter,
        }

        rule_raw = [
            'field1',
            ('field2', '!test_filter'),
        ]

        rule_name = 'test rule'
        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)

        test_filter = _TestFilter()
        part = ('field2', test_filter)


        # Try first record (miss)

        record1 = {
            'field1': 'aa',
            'field2': 'bb',
            'field3': 'cc',
        }

        try:
            rule._get_value_with_rule_part(part, record1)

        except jpart.filter.SkipRuleException as e:
            if str(e) != "Skipping rule on account of value or lack of value for [field2]: filter: not qualified":
                raise

        else:
            raise Exception("Expected not-qualified exception (1).")


        # Try second record (hit)

        record2 = {
            'field1': 'dd',
            'field2': 'ee',
            'field3': 'ff',
        }

        value = rule._get_value_with_rule_part(part, record2)

        assert \
            value == 'ee', \
            "Value not expected (2): [{}]".format(value)


        # Try third record (miss)

        record3 = {
            'field1': 'gg',
            'field2': 'hh',
            'field3': 'ii',
        }

        try:
            rule._get_value_with_rule_part(part, record3)

        except jpart.filter.SkipRuleException as e:
            if str(e) != "Skipping rule on account of value or lack of value for [field2]: filter: not qualified":
                raise

        else:
            raise Exception("Expected not-qualified exception (3).")

    def test_apply(self):

        # Hit

        filter_mappings = {}

        rule_raw = [
            'field1',
            'field2',
        ]

        rule_name = 'test rule'
        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)

        record1 = {
            'field1': 'aa',
            'field2': 'bb',
            'field3': 'cc',
        }

        values = rule.apply(record1)

        expected = [
            'aa',
            'bb',
        ]

        assert \
            values == expected, \
            "Apply output values not correct:\n" \
                "{}".format(values)


        # Miss

        rule_raw = [
            'field1',
            'field2',
        ]

        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)

        record1 = {
            'field1': 'aa',
            'field3': 'cc',
        }

        values = rule.apply(record1)

        assert \
            values is None, \
            "Rule did not return None for irrelevant record as expected:\n" \
                "{}".format(values)

    def test_write_record__inner(self):

        # Construct

        filter_mappings = {}

        rule_raw = [
            'field1',
        ]

        rule_name = 'test rule'
        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)


        # Write

        record = {
            'field1': 'aa',
        }

        s = io.StringIO()
        rule._write_record__inner(s, record)


        # Check stream output

        expected = """\
{"field1": "aa"}
"""

        actual = s.getvalue()

        assert \
            actual == expected, \
            "Stream output not expected:\n" \
                "==========\n" \
                "{}\n" \
                "==========".format(
                actual)

    def test_write_record(self):

        # Construct

        filter_mappings = {}

        rule_raw = [
            'field1',
            'field2',
        ]

        rule_name = 'test rule'
        rule = jpart.rule.Rule(filter_mappings, rule_name, rule_raw)


        with riu.utility.temp_path() as temp_path:

            # Write first

            record1 = {
                'field1': 'aa',
                'field2': 'bb',
                'field3': 'cc',
            }

            values1 = ['aa', 'bb']

            rule.write_record(temp_path, rule.name, record1, values1)


            # Write second (same output)

            record2 = {
                'field1': 'aa',
                'field2': 'bb',
                'field3': 'dd',
            }

            rule.write_record(temp_path, rule.name, record2, values1)


            # Write second (same output)

            record3 = {
                'field1': 'cc',
                'field2': 'dd',
                'field3': 'ee',
            }

            values2 = ['cc', 'dd']

            rule.write_record(temp_path, rule.name, record3, values2)


            # Check outputs

            outputs = os.walk(temp_path)
            outputs = list(outputs)

            expected = [
                (
                    temp_path,
                    [rule.name],
                    [],
                ),
                (
                    os.path.join(temp_path, rule.name),
                    [],
                    [
                        'cc-dd.jsonl',
                        'aa-bb.jsonl',
                    ],
                ),
            ]

            for output in outputs:
                output[2].sort()

            for record in expected:
                record[2].sort()

            assert \
                outputs == expected, \
                "Outputs not correct:\n{}".format(outputs)


            # Check output stream (1)

            with open(os.path.join(rule.name, 'aa-bb.jsonl')) as f:
                actual = riu.journal.parse_journal_stream_gen(f)
                actual = list(actual)

            expected = [
                {'field1': 'aa', 'field2': 'bb', 'field3': 'cc'},
                {'field1': 'aa', 'field2': 'bb', 'field3': 'dd'},
            ]

            assert \
                actual == expected, \
                "aa-bb.jsonl output file not correct:\n{}".format(actual)


            # Check output stream (2)

            with open(os.path.join(rule.name, 'cc-dd.jsonl')) as f:
                actual = riu.journal.parse_journal_stream_gen(f)
                actual = list(actual)

            expected = [
                {'field1': 'cc', 'field2': 'dd', 'field3': 'ee'},
            ]

            assert \
                actual == expected, \
                "cc-dd.jsonl output file not correct:\n{}".format(actual)

    def test_apply_rules_to_input_data_with_rules(self):

        with riu.utility.temp_path() as module_path:
            with riu.utility.temp_path() as output_path:

                rule_name = 'rule1'

                config = {
                    'rules': {
                        'rule1': ['field1', 'field2'],
                    },
                }

                fault_cb = \
                    functools.partial(
                        jpart.cache.default_fault_handler,
                        output_path)

                cached_resources = jpart.cache.CachedResources(fault_cb)

                rules = \
                    jpart.rule._build_rules_with_config(
                        module_path,
                        config,
                        cached_resources)

                input_data = """\
{ "field1": "aa", "field2": "bb", "field3": "cc" }
{ "field1": "aa", "field2": "bb", "field3": "dd" }
{ "field1": "ee", "field2": "ff", "field3": "gg" }
"""

                s = io.StringIO(input_data)

                jpart.rule.apply_rules_to_input_data_with_rules(
                    output_path,
                    rules,
                    s)


                # Check outputs

                outputs = os.walk(output_path)
                outputs = list(outputs)

                expected = [
                    (
                        output_path,
                        [
                            rule_name,
                        ],
                        [],
                    ),
                    (
                        os.path.join(output_path, rule_name),
                        [],
                        [
                            'aa-bb.jsonl',
                            'ee-ff.jsonl',
                        ],
                    ),
                ]

                for rule in outputs:
                    rule[2].sort()

                for rule in expected:
                    rule[2].sort()

                assert \
                    outputs == expected, \
                    "Outputs not correct:\n{}".format(outputs)

    def test_build_rules_with_config(self):

        config = {
            'rules': {
                'rule1': ['field1', 'field2'],
                'rule2': ['field2', 'field3'],
            },
        }

        # Irrelevant since not used
        fault_cb = None

        cached_resources = jpart.cache.CachedResources(fault_cb)

        # Irrelevant since no filter mappings
        module_path = None

        rules = \
            jpart.rule._build_rules_with_config(
                module_path,
                config,
                cached_resources)

        rule1, rule2 = rules

        actual = [
            (rule1.name, rule1._parts),
            (rule2.name, rule2._parts),
        ]

        expected = [
            ('rule1', ['field1', 'field2']),
            ('rule2', ['field2', 'field3']),
        ]

        assert \
            actual == expected, \
            "Rules not correct:\nACTUAL:\n{}\n\nEXPECTED:\n{}".format(actual, expected)

    def test_load_rules_and_apply_to_input_data_with_config(self):

        with riu.utility.temp_path() as module_path:
            with riu.utility.temp_path() as output_path:

                rule_name = 'rule1'

                config = {
                    'rules': {
                        'rule1': ['field1', 'field2'],
                        'rule2': ['field2', 'field3'],
                    },
                }

                fault_cb = \
                    functools.partial(
                        jpart.cache.default_fault_handler,
                        output_path)

                cached_resources = jpart.cache.CachedResources(fault_cb)


                # Load

                input_data = io.StringIO()
                riu.journal.journalize(input_data, field1='aa', field2='bb', field3='cc', field4='dd')
                riu.journal.journalize(input_data, field1='ee', field2='ff', field3='gg', field4='hh')
                riu.journal.journalize(input_data, field4='hh', field5='ii', field6='jj')

                input_data.seek(0)

                jpart.rule.load_rules_and_apply_to_input_data_with_config(
                    module_path,
                    output_path,
                    config,
                    input_data,
                    do_dispose=True)


                # Check written files

                entries = os.walk(output_path)
                entries = sorted(entries)

                expected = sorted([
                    (
                        output_path,
                        ['rule1', 'rule2'],
                        [],
                    ),
                    (
                        os.path.join(output_path, 'rule1'),
                        [],
                        [
                            'aa-bb.jsonl',
                            'ee-ff.jsonl',
                        ],
                    ),
                    (
                        os.path.join(output_path, 'rule2'),
                        [],
                        [
                            'bb-cc.jsonl',
                            'ff-gg.jsonl',
                        ],
                    ),
                ])

                for record in entries:
                    record[1].sort()
                    record[2].sort()

                for record in expected:
                    record[1].sort()
                    record[2].sort()

                assert \
                    entries == expected, \
                    "Written files not correct:\n" \
                        "ACTUAL:\n" \
                        "{}\n\n" \
                        "EXPECTED:\n" \
                        "{}".format(
                            entries, expected)
