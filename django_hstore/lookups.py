from __future__ import unicode_literals, absolute_import

from django.utils import six
from django.db.models.lookups import (
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Contains,
    IContains,
    IsNull,
    Exact
)
from django.db.models import  Lookup, Func

from django_hstore.utils import get_cast_for_param, get_value_annotations
import json
import collections

__all__ = [
    'HStoreComparisonLookupMixin',
    'HStoreGreaterThan',
    'HStoreGreaterThanOrEqual',
    'HStoreLessThan',
    'HStoreLessThanOrEqual',
    'HStoreContains',
    'HStoreIContains',
    'HStoreIsNull'
]


class HStoreLookupMixin(object):
    def __init__(self, lhs, rhs, *args, **kwargs):
        # We need to record the types of the rhs parameters before they are converted to strings
        if isinstance(rhs, dict):
            self.value_annot = get_value_annotations(rhs)
        super(HStoreLookupMixin, self).__init__(lhs, rhs)


class HStoreComparisonLookupMixin(HStoreLookupMixin):
    """
    Mixin for hstore comparison custom lookups.
    """

    def as_postgresql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        rhs, rhs_params = self.process_rhs(compiler, connection)
        if len(rhs_params) == 1 and isinstance(rhs_params[0], dict):
            param = rhs_params[0]
            sign = (self.lookup_name[0] == 'g' and '>%s' or '<%s') % (self.lookup_name[-1] == 'e' and '=' or '')
            param_keys = list(param.keys())
            conditions = []

            for key in param_keys:
                cast = get_cast_for_param(self.value_annot, key)
                conditions.append('(%s->\'%s\')%s %s %%s' % (lhs, key, cast, sign))

            return (" AND ".join(conditions), param.values())

        raise ValueError('invalid value')


class HStoreGreaterThan(HStoreComparisonLookupMixin, GreaterThan):
    pass


class HStoreGreaterThanOrEqual(HStoreComparisonLookupMixin, GreaterThanOrEqual):
    pass


class HStoreLessThan(HStoreComparisonLookupMixin, LessThan):
    pass


class HStoreLessThanOrEqual(HStoreComparisonLookupMixin, LessThanOrEqual):
    pass


class HStoreContains(HStoreLookupMixin, Contains):

    def as_postgresql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)
        # FIXME: ::text cast is added by ``django.db.backends.postgresql_psycopg2.DatabaseOperations.lookup_cast``;
        # maybe there's a cleaner way to fix the cast for hstore columns
        if lhs.endswith('::text'):
            lhs = '{0}{1}'.format(lhs[:-4], 'hstore')
        param = self.rhs

        if isinstance(param, dict):
            values = list(param.values())
            keys = list(param.keys())
            if len(values) == 1 and isinstance(values[0], (list, tuple)):
                # Can't cast here because the list could contain multiple types
                return '%s->\'%s\' = ANY(%%s)' % (lhs, keys[0]), [[str(x) for x in values[0]]]
            elif len(keys) == 1 and len(values) == 1:
                # Retrieve key and compare to param instead of using '@>' in order to cast hstore value
                cast = get_cast_for_param(self.value_annot, keys[0])
                return ('(%s->\'%s\')%s = %%s' % (lhs, keys[0], cast), [values[0]])
            return '%s @> %%s' % lhs, [param]
        elif isinstance(param, (list, tuple)):
            if len(param) == 0:
                raise ValueError('invalid value')
            if len(param) < 2:
                return '%s ? %%s' % lhs, [param[0]]
            if param:
                return '%s ?& %%s' % lhs, [param]
        elif isinstance(param, six.string_types):
            # if looking for a string perform the normal text lookup
            # that is: look for occurence of string in all the keys
            pass
        # needed for SerializedDictionaryField
        elif hasattr(self.lhs.target, 'serializer'):
            try:
                self.lhs.target._serialize_value(param)
                pass
            except Exception:
                raise ValueError('invalid value')
        else:
            raise ValueError('invalid value')
        return super(HStoreContains, self).as_sql(compiler, connection)


class HStoreIContains(IContains, HStoreContains):
    pass


class HStoreIsNull(IsNull):

    def as_postgresql(self, compiler, connection):
        lhs, lhs_params = self.process_lhs(compiler, connection)

        if isinstance(self.rhs, dict):
            param = self.rhs
            param_keys = list(param.keys())
            conditions = []

            for key in param_keys:
                op = 'IS NULL' if param[key] else 'IS NOT NULL'
                conditions.append('(%s->\'%s\') %s' % (lhs, key, op))

            return (" AND ".join(conditions), lhs_params)

        return super(HStoreIsNull, self).as_sql(compiler, connection)



# JSONField
class JSONValue(Func):
    function = 'CAST'
    template = '%(function)s(%(expressions)s AS JSON)'

    def __init__(self, expression):
        json_string = json.dumps(expression, allow_nan=False)
        super(JSONValue, self).__init__(Value(json_string))
        
class JSONLookupMixin(object):

    def get_prep_lookup(self):
        value = self.rhs
        if not hasattr(value, '_prepare') and value is not None:
            return JSONValue(value)
        return super(JSONLookupMixin, self).get_prep_lookup()


class JSONExact(JSONLookupMixin, Exact):
    pass


class JSONGreaterThan(JSONLookupMixin, GreaterThan):
    lookup_name = 'gt'


class JSONGreaterThanOrEqual(JSONLookupMixin, GreaterThanOrEqual):
    lookup_name = 'gte'


class JSONLessThan(JSONLookupMixin, LessThan):
    lookup_name = 'lt'


class JSONLessThanOrEqual(JSONLookupMixin, LessThanOrEqual):
    lookup_name = 'lte'


class JSONContainedBy(Lookup):
    lookup_name = 'contained_by'

    def as_sql(self, qn, connection):
        lhs, lhs_params = self.process_lhs(qn, connection)
        rhs, rhs_params = self.process_rhs(qn, connection)
        params = rhs_params + lhs_params
        return 'JSON_CONTAINS({}, {})'.format(rhs, lhs), params


class JSONContains(JSONLookupMixin, Lookup):
    lookup_name = 'contains'

    def as_sql(self, qn, connection):
        lhs, lhs_params = self.process_lhs(qn, connection)
        rhs, rhs_params = self.process_rhs(qn, connection)
        params = lhs_params + rhs_params
        return 'JSON_CONTAINS({}, {})'.format(lhs, rhs), params


class JSONHasKey(Lookup):
    lookup_name = 'has_key'

    def get_prep_lookup(self):
        if not isinstance(self.rhs, six.text_type):
            raise ValueError(
                "JSONField's 'has_key' lookup only works with {} values"
                .format(six.text_type.__name__)
            )
        return super(JSONHasKey, self).get_prep_lookup()

    def as_sql(self, qn, connection):
        lhs, lhs_params = self.process_lhs(qn, connection)
        key_name = self.rhs
        path = '$.{}'.format(json.dumps(key_name))
        params = lhs_params + [path]
        return "JSON_CONTAINS_PATH({}, 'one', %s)".format(lhs), params


class JSONSequencesMixin(object):
    def get_prep_lookup(self):
        if not isinstance(self.rhs, collections.Sequence):
            raise ValueError(
                "JSONField's '{}' lookup only works with Sequences"
                .format(self.lookup_name)
            )
        return self.rhs


class JSONHasKeys(JSONSequencesMixin, Lookup):
    lookup_name = 'has_keys'

    def as_sql(self, qn, connection):
        lhs, lhs_params = self.process_lhs(qn, connection)
        paths = [
            '$.{}'.format(json.dumps(key_name))
            for key_name in self.rhs
        ]
        params = lhs_params + paths

        sql = ['JSON_CONTAINS_PATH(', lhs, ", 'all', "]
        sql.append(', '.join('%s' for _ in paths))
        sql.append(')')
        return ''.join(sql), params


class JSONHasAnyKeys(JSONSequencesMixin, Lookup):
    lookup_name = 'has_any_keys'

    def as_sql(self, qn, connection):
        lhs, lhs_params = self.process_lhs(qn, connection)
        paths = [
            '$.{}'.format(json.dumps(key_name))
            for key_name in self.rhs
        ]
        params = lhs_params + paths

        sql = ['JSON_CONTAINS_PATH(', lhs, ", 'one', "]
        sql.append(', '.join('%s' for _ in paths))
        sql.append(')')
        return ''.join(sql), params

