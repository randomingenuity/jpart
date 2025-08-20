
import riu.hierarchy


class SkipRuleException(Exception):
    def __init__(self, field_name, reason):
        self._field_name = field_name
        self._reason = reason

        message = \
            "Skipping rule on account of value or lack of value for [{}]: " \
                "{}".format(
                self._field_name, reason)

        super().__init__(message)

    @property
    def field_name(self):
        return self._field_name

    @property
    def reason(self):
        return self._reason


class BaseFilter(object):
    def get_value(self, name, record):
        """Value extractor if needed to be overridden with something more
        elaborate.
        """

        value = \
            riu.hierarchy.get_value_from_hierarchy_with_string_reference(
                record, name)

        return value

    def does_qualify(self, name, value):
        """Whether to skip this record for this rule."""

        return True
