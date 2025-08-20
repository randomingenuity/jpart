import logging
import os
import json
import functools
import re

import riu.plugin
import riu.journal
import riu.hierarchy
import riu.utility

import jpart.filter
import jpart.cache
import jpart.utility

SKIP_REASON_MODULE__NOT_QUALIFIED = 'filter: not qualified'
SKIP_REASON_DEFAULT__NOT_FOUND = 'default: not found'

_LOGGER = logging.getLogger(__name__)

_FILENAME_VALUE_RE = re.compile(r'^[a-zA-Z0-9\-_\. ]*$')


class Rule(object):
    def __init__(self, filter_mappings, name, rule_raw, cached_resources=None):
        rebuilt = self._process_parts(filter_mappings, rule_raw)

        self._name = name
        self._parts = rebuilt
        self._cached_resources = cached_resources

    @property
    def name(self):
        return self._name

    def __str__(self):
        return self.name

    def _process_parts(self, filter_mappings, rule_raw):

        materialized_parts = []
        for part in rule_raw:
            if isinstance(part, (list, tuple)) is True:
                field_name, filter_name = part

                # Filters will be more visually discernable if they're prefixed
                # with exclamation points
                assert \
                    filter_name[0] == '!', \
                    "The filter names in references must be prefixed with " \
                        "an exclamation point: [{}]".format(filter_name)

                filter_name = filter_name[1:]
                filter_cls = filter_mappings[filter_name]
                filter_ = filter_cls()
                part = (field_name, filter_)

            materialized_parts.append(part)


        return materialized_parts

    def _get_value_with_rule_part(self, part, record):

        # Get the name of the field that this part of the rule is concerned
        # with

        filter_module = None
        if isinstance(part, (list, tuple)) is True:
            name, filter_module = part
        else:
            name = part

        # Get the value produced by applying the one rule part


        if filter_module is not None:
            value = filter_module.get_value(name, record)
            does_qualify = filter_module.does_qualify(name, value)

            if does_qualify is False:
                raise \
                    jpart.filter.SkipRuleException(
                        name,
                        SKIP_REASON_MODULE__NOT_QUALIFIED)

        else:
            try:
                value = \
                    riu.hierarchy.get_value_from_hierarchy_with_string_reference(
                        record, name)

            except KeyError as ke:
                raise \
                    jpart.filter.SkipRuleException(
                        name,
                        SKIP_REASON_DEFAULT__NOT_FOUND)


        return value

    def apply(self, record):
        """Retrieve the values for each of the parts of the rule. If any are
        not present, return None.
        """

        values = []
        for part in self._parts:

            # Apply rule part

            try:
                value = self._get_value_with_rule_part(part, record)

            except jpart.filter.SkipRuleException:
                return None


            assert \
                isinstance(value, (str, int, float)) is True, \
                "Rule [{}] part [{}] yielded [non-simple] value that can't " \
                    "be used in a filename: [{}] {}\n" \
                    "RECORD:\n{}".format(
                    self.name, part, value.__class__.__name__, value,
                    riu.utility.get_pretty_json(record))

            # If not a string, make it a string
            value = str(value)

            assert \
                _FILENAME_VALUE_RE.match(value) is not None, \
                "Rule [{}] part [{}] yielded value that can't be used in a " \
                    "filename: [{}]\n" \
                    "RECORD:\n{}".format(
                    self.name, part, value,
                    riu.utility.get_pretty_json(record))


            # Capture

            values.append(value)


        return values

    def _write_record__inner(self, f, record):

        json.dump(record, f)
        f.write('\n')

    def write_record(self, output_path, rule_name, record, phrases):
        """Write the record in a certain partitioned output path. Values values
        will have already been stringified.
        """

        filename = jpart.utility.construct_output_filename(phrases)

        if self._cached_resources is None:
            rule_output_path = os.path.join(output_path, rule_name)

            if os.path.exists(rule_output_path) is False:
                os.makedirs(rule_output_path)

            filepath = os.path.join(rule_output_path, filename)

            with jpart.utility.RESOURCE_APPEND_OPENER(filepath) as f:
                self._write_record__inner(f, record)

        else:
            # The cache logic is context agnostic. Therefore, the "name" we're
            # looking up must be relative to the root output path (so that it's
            # both absolute and unique against any other identical filename).
            rel_filepath = os.path.join(rule_name, filename)

            f = self._cached_resources.get_or_create(rel_filepath)
            self._write_record__inner(f, record)


def _build_rules_with_config(
        root_module_import_path, config, cached_resources):

    filter_mappings_raw = config.get('filter_mappings', {})
    rules_index_raw = config['rules']


    # Load custom filters

# TODO(dustin): Add test for filter mappings
    filter_mappings = {}
    for name, reference in filter_mappings_raw.items():

        _LOGGER.debug("Processing [{}] [{}].".format(name, reference))

        cls_ = riu.plugin.get_module_symbol_with_reference(
                root_module_import_path,
                reference)

        assert \
            issubclass(cls_, jpart.filter.BaseFilter) is True, \
            "Class does not inherit jpart.filter.BaseFilter: [{}]".format(
                reference)

        filter_ = cls_()

        filter_mappings[name] = filter_


    # Initialize filters

    rules = []
    for name, rule_raw in rules_index_raw.items():

        rule = Rule(
                filter_mappings,
                name,
                rule_raw,
                cached_resources=cached_resources)

        rules.append(rule)


    return rules


def apply_rules_to_input_data_with_rules(output_path, rules, f):

    j = riu.journal.parse_journal_stream_gen(f)
    for i, record in enumerate(j):

        for rule in rules:
            values = rule.apply(record)
            if values is None:
                continue

            try:
                rule.write_record(output_path, rule.name, record, values)

            except:
                _LOGGER.exception("Could not write record via rule: {}".format(
                                  rule))

                raise


        if i % 100 == 0 and i > 0:
            _LOGGER.info("Processed ({}) records.".format(i + 1))


def load_rules_and_apply_to_input_data_with_config(
        module_path, output_path, config, f, cached_resources=None,
        do_dispose=True):

    # Initialize cache

    if cached_resources is None:

        fault_cb = \
            functools.partial(
                jpart.cache.default_fault_handler,
                output_path)

        cached_resources = jpart.cache.CachedResources(fault_cb)


    # Build rules

    rules = \
        _build_rules_with_config(
            module_path,
            config,
            cached_resources)


    # Process data

    try:
        apply_rules_to_input_data_with_rules(output_path, rules, f)

    finally:
        if do_dispose is True:
            cached_resources.dispose()
