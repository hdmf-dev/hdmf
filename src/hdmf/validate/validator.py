import re
from abc import ABCMeta, abstractmethod
from copy import copy
from itertools import chain
from collections import defaultdict

import numpy as np

from .errors import Error, DtypeError, MissingError, MissingDataType, ShapeError, IllegalLinkError, IncorrectDataType
from .errors import ExpectedArrayError, IncorrectQuantityError
from ..build import GroupBuilder, DatasetBuilder, LinkBuilder, ReferenceBuilder, RegionBuilder
from ..build.builders import BaseBuilder
from ..spec import Spec, AttributeSpec, GroupSpec, DatasetSpec, RefSpec, LinkSpec
from ..spec import SpecNamespace
from ..spec.spec import BaseStorageSpec, DtypeHelper
from ..utils import docval, getargs, call_docval_func, pystr, get_data_shape

__synonyms = DtypeHelper.primary_dtype_synonyms

__additional = {
    'float': ['double'],
    'int8': ['short', 'int', 'long'],
    'short': ['int', 'long'],
    'int': ['long'],
    'uint8': ['uint16', 'uint32', 'uint64'],
    'uint16': ['uint32', 'uint64'],
    'uint32': ['uint64'],
    'utf': ['ascii']
}

# if the spec dtype is a key in __allowable, then all types in __allowable[key] are valid
__allowable = dict()
for dt, dt_syn in __synonyms.items():
    allow = copy(dt_syn)
    if dt in __additional:
        for addl in __additional[dt]:
            allow.extend(__synonyms[addl])
    for syn in dt_syn:
        __allowable[syn] = allow
__allowable['numeric'] = set(chain.from_iterable(__allowable[k] for k in __allowable if 'int' in k or 'float' in k))


def check_type(expected, received):
    '''
    *expected* should come from the spec
    *received* should come from the data
    '''
    if isinstance(expected, list):
        if len(expected) > len(received):
            raise ValueError('compound type shorter than expected')
        for i, exp in enumerate(DtypeHelper.simplify_cpd_type(expected)):
            rec = received[i]
            if rec not in __allowable[exp]:
                return False
        return True
    else:
        if isinstance(received, np.dtype):
            if received.char == 'O':
                if 'vlen' in received.metadata:
                    received = received.metadata['vlen']
                else:
                    raise ValueError("Unrecognized type: '%s'" % received)
                received = 'utf' if received is str else 'ascii'
            elif received.char == 'U':
                received = 'utf'
            elif received.char == 'S':
                received = 'ascii'
            else:
                received = received.name
        elif isinstance(received, type):
            received = received.__name__
        if isinstance(expected, RefSpec):
            expected = expected.reftype
        elif isinstance(expected, type):
            expected = expected.__name__
        return received in __allowable[expected]


def get_iso8601_regex():
    isodate_re = (r'^(-?(?:[1-9][0-9]*)?[0-9]{4})-(1[0-2]|0[1-9])-(3[01]|0[1-9]|[12][0-9])T(2[0-3]|[01][0-9]):'
                  r'([0-5][0-9]):([0-5][0-9])(\.[0-9]+)?(Z|[+-](?:2[0-3]|[01][0-9]):[0-5][0-9])?$')
    return re.compile(isodate_re)


_iso_re = get_iso8601_regex()


def _check_isodatetime(s, default=None):
    try:
        if _iso_re.match(pystr(s)) is not None:
            return 'isodatetime'
    except Exception:
        pass
    return default


class EmptyArrayError(Exception):
    pass


def get_type(data):
    if isinstance(data, str):
        return _check_isodatetime(data, 'utf')
    elif isinstance(data, bytes):
        return _check_isodatetime(data, 'ascii')
    elif isinstance(data, RegionBuilder):
        return 'region'
    elif isinstance(data, ReferenceBuilder):
        return 'object'
    elif isinstance(data, np.ndarray):
        if data.size == 0:
            raise EmptyArrayError()
        return get_type(data[0])
    elif isinstance(data, np.bool_):
        return 'bool'
    if not hasattr(data, '__len__'):
        return type(data).__name__
    else:
        if hasattr(data, 'dtype'):
            if isinstance(data.dtype, list):
                return [get_type(data[0][i]) for i in range(len(data.dtype))]
            if data.dtype.metadata is not None and data.dtype.metadata.get('vlen') is not None:
                return get_type(data[0])
            return data.dtype
        if len(data) == 0:
            raise EmptyArrayError()
        return get_type(data[0])


def check_shape(expected, received):
    ret = False
    if expected is None:
        ret = True
    else:
        if isinstance(expected, (list, tuple)):
            if isinstance(expected[0], (list, tuple)):
                for sub in expected:
                    if check_shape(sub, received):
                        ret = True
                        break
            else:
                if len(expected) > 0 and received is None:
                    ret = False
                elif len(expected) == len(received):
                    ret = True
                    for e, r in zip(expected, received):
                        if not check_shape(e, r):
                            ret = False
                            break
        elif isinstance(expected, int):
            ret = expected == received
    return ret


class ValidatorMap:
    """A class for keeping track of Validator objects for all data types in a namespace"""

    @docval({'name': 'namespace', 'type': SpecNamespace, 'doc': 'the namespace to builder map for'})
    def __init__(self, **kwargs):
        ns = getargs('namespace', kwargs)
        self.__ns = ns
        tree = defaultdict(list)
        types = ns.get_registered_types()
        self.__type_key = ns.get_spec(types[0]).type_key()
        for dt in types:
            spec = ns.get_spec(dt)
            parent = spec.data_type_inc
            child = spec.data_type_def
            tree[child] = list()
            if parent is not None:
                tree[parent].append(child)
        for t in tree:
            self.__rec(tree, t)
        self.__valid_types = dict()
        self.__validators = dict()
        for dt, children in tree.items():
            _list = list()
            for t in children:
                spec = self.__ns.get_spec(t)
                if isinstance(spec, GroupSpec):
                    val = GroupValidator(spec, self)
                else:
                    val = DatasetValidator(spec, self)
                if t == dt:
                    self.__validators[t] = val
                _list.append(val)
            self.__valid_types[dt] = tuple(_list)

    def __rec(self, tree, node):
        if not isinstance(tree[node], tuple):
            sub_types = {node}
            for child in tree[node]:
                sub_types.update(self.__rec(tree, child))
            tree[node] = tuple(sub_types)
        return tree[node]

    @property
    def namespace(self):
        return self.__ns

    @docval({'name': 'spec', 'type': (Spec, str), 'doc': 'the specification to use to validate'},
            returns='all valid sub data types for the given spec', rtype=tuple)
    def valid_types(self, **kwargs):
        '''Get all valid types for a given data type'''
        spec = getargs('spec', kwargs)
        if isinstance(spec, Spec):
            spec = spec.data_type_def
        try:
            return self.__valid_types[spec]
        except KeyError:
            raise ValueError("no children for '%s'" % spec)

    @docval({'name': 'data_type', 'type': (BaseStorageSpec, str),
             'doc': 'the data type to get the validator for'},
            returns='the validator ``data_type``')
    def get_validator(self, **kwargs):
        """Return the validator for a given data type"""
        dt = getargs('data_type', kwargs)
        if isinstance(dt, BaseStorageSpec):
            dt_tmp = dt.data_type_def
            if dt_tmp is None:
                dt_tmp = dt.data_type_inc
            dt = dt_tmp
        try:
            return self.__validators[dt]
        except KeyError:
            msg = "data type '%s' not found in namespace %s" % (dt, self.__ns.name)
            raise ValueError(msg)

    @docval({'name': 'builder', 'type': BaseBuilder, 'doc': 'the builder to validate'},
            returns="a list of errors found", rtype=list)
    def validate(self, **kwargs):
        """Validate a builder against a Spec

        ``builder`` must have the attribute used to specifying data type
        by the namespace used to construct this ValidatorMap.
        """
        builder = getargs('builder', kwargs)
        dt = builder.attributes.get(self.__type_key)
        if dt is None:
            msg = "builder must have data type defined with attribute '%s'" % self.__type_key
            raise ValueError(msg)
        validator = self.get_validator(dt)
        return validator.validate(builder)


class Validator(metaclass=ABCMeta):
    '''A base class for classes that will be used to validate against Spec subclasses'''

    @docval({'name': 'spec', 'type': Spec, 'doc': 'the specification to use to validate'},
            {'name': 'validator_map', 'type': ValidatorMap, 'doc': 'the ValidatorMap to use during validation'})
    def __init__(self, **kwargs):
        self.__spec = getargs('spec', kwargs)
        self.__vmap = getargs('validator_map', kwargs)

    @property
    def spec(self):
        return self.__spec

    @property
    def vmap(self):
        return self.__vmap

    @abstractmethod
    @docval({'name': 'value', 'type': None, 'doc': 'either in the form of a value or a Builder'},
            returns='a list of Errors', rtype=list)
    def validate(self, **kwargs):
        pass

    @classmethod
    def get_spec_loc(cls, spec):
        return spec.path

    @classmethod
    def get_builder_loc(cls, builder):
        stack = list()
        tmp = builder
        while tmp is not None and tmp.name != 'root':
            stack.append(tmp.name)
            tmp = tmp.parent
        return "/".join(reversed(stack))


class AttributeValidator(Validator):
    '''A class for validating values against AttributeSpecs'''

    @docval({'name': 'spec', 'type': AttributeSpec, 'doc': 'the specification to use to validate'},
            {'name': 'validator_map', 'type': ValidatorMap, 'doc': 'the ValidatorMap to use during validation'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)

    @docval({'name': 'value', 'type': None, 'doc': 'the value to validate'},
            returns='a list of Errors', rtype=list)
    def validate(self, **kwargs):
        value = getargs('value', kwargs)
        ret = list()
        spec = self.spec
        if spec.required and value is None:
            ret.append(MissingError(self.get_spec_loc(spec)))
        else:
            if spec.dtype is None:
                ret.append(Error(self.get_spec_loc(spec)))
            elif isinstance(spec.dtype, RefSpec):
                if not isinstance(value, BaseBuilder):
                    expected = '%s reference' % spec.dtype.reftype
                    try:
                        value_type = get_type(value)
                        ret.append(DtypeError(self.get_spec_loc(spec), expected, value_type))
                    except EmptyArrayError:
                        # do not validate dtype of empty array. HDMF does not yet set dtype when writing a list/tuple
                        pass
                else:
                    target_spec = self.vmap.namespace.catalog.get_spec(spec.dtype.target_type)
                    data_type = value.attributes.get(target_spec.type_key())
                    hierarchy = self.vmap.namespace.catalog.get_hierarchy(data_type)
                    if spec.dtype.target_type not in hierarchy:
                        ret.append(IncorrectDataType(self.get_spec_loc(spec), spec.dtype.target_type, data_type))
            else:
                try:
                    dtype = get_type(value)
                    if not check_type(spec.dtype, dtype):
                        ret.append(DtypeError(self.get_spec_loc(spec), spec.dtype, dtype))
                except EmptyArrayError:
                    # do not validate dtype of empty array. HDMF does not yet set dtype when writing a list/tuple
                    pass
            shape = get_data_shape(value)
            if not check_shape(spec.shape, shape):
                if shape is None:
                    ret.append(ExpectedArrayError(self.get_spec_loc(self.spec), self.spec.shape, str(value)))
                else:
                    ret.append(ShapeError(self.get_spec_loc(spec), spec.shape, shape))
        return ret


class BaseStorageValidator(Validator):
    '''A base class for validating against Spec objects that have attributes i.e. BaseStorageSpec'''

    @docval({'name': 'spec', 'type': BaseStorageSpec, 'doc': 'the specification to use to validate'},
            {'name': 'validator_map', 'type': ValidatorMap, 'doc': 'the ValidatorMap to use during validation'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)
        self.__attribute_validators = dict()
        for attr in self.spec.attributes:
            self.__attribute_validators[attr.name] = AttributeValidator(attr, self.vmap)

    @docval({"name": "builder", "type": BaseBuilder, "doc": "the builder to validate"},
            returns='a list of Errors', rtype=list)
    def validate(self, **kwargs):
        builder = getargs('builder', kwargs)
        attributes = builder.attributes
        ret = list()
        for attr, validator in self.__attribute_validators.items():
            attr_val = attributes.get(attr)
            if attr_val is None:
                if validator.spec.required:
                    ret.append(MissingError(self.get_spec_loc(validator.spec),
                                            location=self.get_builder_loc(builder)))
            else:
                errors = validator.validate(attr_val)
                for err in errors:
                    err.location = self.get_builder_loc(builder) + ".%s" % validator.spec.name
                ret.extend(errors)
        return ret


class DatasetValidator(BaseStorageValidator):
    '''A class for validating DatasetBuilders against DatasetSpecs'''

    @docval({'name': 'spec', 'type': DatasetSpec, 'doc': 'the specification to use to validate'},
            {'name': 'validator_map', 'type': ValidatorMap, 'doc': 'the ValidatorMap to use during validation'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)

    @docval({"name": "builder", "type": DatasetBuilder, "doc": "the builder to validate"},
            returns='a list of Errors', rtype=list)
    def validate(self, **kwargs):
        builder = getargs('builder', kwargs)
        ret = super().validate(builder)
        data = builder.data
        if self.spec.dtype is not None:
            try:
                dtype = get_type(data)
                if not check_type(self.spec.dtype, dtype):
                    ret.append(DtypeError(self.get_spec_loc(self.spec), self.spec.dtype, dtype,
                                          location=self.get_builder_loc(builder)))
            except EmptyArrayError:
                # do not validate dtype of empty array. HDMF does not yet set dtype when writing a list/tuple
                pass
        shape = get_data_shape(data)
        if not check_shape(self.spec.shape, shape):
            if shape is None:
                ret.append(ExpectedArrayError(self.get_spec_loc(self.spec), self.spec.shape, str(data),
                                              location=self.get_builder_loc(builder)))
            else:
                ret.append(ShapeError(self.get_spec_loc(self.spec), self.spec.shape, shape,
                                      location=self.get_builder_loc(builder)))
        return ret


def _resolve_data_type(spec):
    if isinstance(spec, LinkSpec):
        return spec.target_type
    return spec.data_type


class GroupValidator(BaseStorageValidator):
    '''A class for validating GroupBuilders against GroupSpecs'''

    @docval({'name': 'spec', 'type': GroupSpec, 'doc': 'the specification to use to validate'},
            {'name': 'validator_map', 'type': ValidatorMap, 'doc': 'the ValidatorMap to use during validation'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)

    @docval({"name": "builder", "type": GroupBuilder, "doc": "the builder to validate"},  # noqa: C901
            returns='a list of Errors', rtype=list)
    def validate(self, **kwargs):  # noqa: C901
        builder = getargs('builder', kwargs)

        errors = super().validate(builder)
        errors.extend(self.__validate_children(builder))
        return errors

    def __validate_children(self, parent_builder):
        """Validates the children of the group builder against the children in the spec.

        Children are defined as datasets, groups, and links.

        Validation works by first assigning builder children to spec children
        in a many-to-one relationship using a SpecMatcher (this matching is
        non-trivial due to inheritance, which is why it is isolated in a
        separate class). Once the matching is complete, it is a
        straightforward procedure for validating the set of matching builders
        against each child spec.
        """
        spec_children = chain(self.spec.datasets, self.spec.groups, self.spec.links)
        matcher = SpecMatcher(self.vmap, spec_children)

        builder_children = chain(parent_builder.datasets.values(),
                                 parent_builder.groups.values(),
                                 parent_builder.links.values())
        matcher.assign_to_specs(builder_children)

        for child_spec, matched_builders in matcher.spec_matches:
            yield from self.__validate_presence_and_quantity(child_spec, len(matched_builders), parent_builder)
            for child_builder in matched_builders:
                yield from self.__validate_child_builder(child_spec, child_builder, parent_builder)

    def __validate_presence_and_quantity(self, child_spec, n_builders, parent_builder):
        """Validate that at least one matching builder exists if the spec is
        required and that the number of builders agrees with the spec quantity
        """
        if n_builders == 0 and child_spec.required:
            yield self.__construct_missing_child_error(child_spec, parent_builder)
        elif self.__incorrect_quantity(n_builders, child_spec):
            yield self.__construct_incorrect_quantity_error(child_spec, parent_builder, n_builders)

    def __construct_missing_child_error(self, child_spec, parent_builder):
        """Returns either a MissingDataType or a MissingError depending on
        whether or not a specific data type can be resolved from the spec
        """
        data_type = _resolve_data_type(child_spec)
        builder_loc = self.get_builder_loc(parent_builder)
        if data_type is not None:
            name_of_erroneous = self.get_spec_loc(self.spec)
            return MissingDataType(name_of_erroneous, data_type,
                                   location=builder_loc, missing_dt_name=child_spec.name)
        else:
            name_of_erroneous = self.get_spec_loc(child_spec)
            return MissingError(name_of_erroneous, location=builder_loc)

    @staticmethod
    def __incorrect_quantity(n_found, spec):
        """Returns a boolean indicating whether the number of builder elements matches the specified quantity"""
        if not spec.is_many() and n_found > 1:
            return True
        elif isinstance(spec.quantity, int) and n_found != spec.quantity:
            return True
        return False

    def __construct_incorrect_quantity_error(self, child_spec, parent_builder, n_builders):
        name_of_erroneous = self.get_spec_loc(self.spec)
        data_type = _resolve_data_type(child_spec)
        builder_loc = self.get_builder_loc(parent_builder)
        return IncorrectQuantityError(name_of_erroneous, data_type, expected=child_spec.quantity,
                                      received=n_builders, location=builder_loc)

    def __validate_child_builder(self, child_spec, child_builder, parent_builder):
        """Validate a child builder against a child spec considering links"""
        if isinstance(child_builder, LinkBuilder):
            if self.__cannot_be_link(child_spec):
                yield self.__construct_illegal_link_error(child_spec, parent_builder)
                return  # do not validate illegally linked objects
            child_builder = child_builder.builder
        child_validator = self.__get_child_validator(child_spec)
        yield from child_validator.validate(child_builder)

    def __construct_illegal_link_error(self, child_spec, parent_builder):
        name_of_erroneous = self.get_spec_loc(child_spec)
        builder_loc = self.get_builder_loc(parent_builder)
        return IllegalLinkError(name_of_erroneous, location=builder_loc)

    @staticmethod
    def __cannot_be_link(spec):
        return not isinstance(spec, LinkSpec) and not spec.linkable

    def __get_child_validator(self, spec):
        """Returns the appropriate validator for a child spec

        If a specific data type can be resolved, the validator is acquired from
        the ValidatorMap, otherwise a new Validator is created.
        """
        if _resolve_data_type(spec) is not None:
            return self.vmap.get_validator(_resolve_data_type(spec))
        elif isinstance(spec, GroupSpec):
            return GroupValidator(spec, self.vmap)
        elif isinstance(spec, DatasetSpec):
            return DatasetValidator(spec, self.vmap)
        else:
            msg = "Unable to resolve a validator for spec %s" % spec
            raise ValueError(msg)


class SpecMatches:
    """A utility class to hold a spec and the builders matched to it"""

    def __init__(self, spec):
        self.spec = spec
        self.builders = list()

    def add(self, builder):
        self.builders.append(builder)


class SpecMatcher:
    """Matches a set of builders against a set of specs

    This class is intended to isolate the task of choosing which spec a
    builder should be validated against from the task of performing that
    validation.
    """

    def __init__(self, vmap, specs):
        self.vmap = vmap
        self._spec_matches = [SpecMatches(spec) for spec in specs]
        self._unmatched_builders = SpecMatches(None)

    @property
    def unmatched_builders(self):
        """Returns the builders for which no matching spec was found

        These builders can be considered superfluous, and will generate a
        warning in the future.
        """
        return self._unmatched_builders.builders

    @property
    def spec_matches(self):
        """Returns a list of tuples of: (spec, assigned builders)"""
        return [(sm.spec, sm.builders) for sm in self._spec_matches]

    def assign_to_specs(self, builders):
        """Assigns a set of builders against a set of specs (many-to-one)

        In the case that no matching spec is found, a builder will be
        added to a list of unmatched builders.
        """
        for builder in builders:
            spec_match = self._best_matching_spec(builder)
            if spec_match is None:
                self._unmatched_builders.add(builder)
            else:
                spec_match.add(builder)

    def _best_matching_spec(self, builder):
        """Finds the best matching spec for builder

        The current algorithm is:
        1. filter specs which meet the minimum requirements of consistent name
            and data type
        2. if more than one candidate meets the minimum requirements, find the
            candidates which do not yet have a sufficient number of builders
            assigned (based on the spec quantity)
        3. return the first unsatisfied candidate if any, otherwise return the
            first candidate

        Note that the current algorithm will give different results depending
        on the order of the specs or builders, and also does not consider
        inheritance hierarchy. Future improvements to this matching algorithm
        should resolve these discrepancies.
        """
        candidates = self._filter_by_name(self._spec_matches, builder)
        candidates = self._filter_by_type(candidates, builder)
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0]
        else:
            unsatisfied_candidates = self._filter_by_unsatisfied(candidates)
            if len(unsatisfied_candidates) == 0:
                return candidates[0]
            else:
                return unsatisfied_candidates[0]

    def _filter_by_name(self, candidates, builder):
        """Returns the candidate specs that either have the same name as the
        builder or do not specify a name.
        """
        def name_is_consistent(spec_matches):
            spec = spec_matches.spec
            return spec.name is None or spec.name == builder.name

        return list(filter(name_is_consistent, candidates))

    def _filter_by_type(self, candidates, builder):
        """Returns the candidate specs which have a data type consistent with
        the builder's data type.
        """
        def compatible_type(spec_matches):
            spec = spec_matches.spec
            if isinstance(spec, LinkSpec):
                validator = self.vmap.get_validator(spec.target_type)
                spec = validator.spec
            if spec.data_type is None:
                return True
            valid_validators = self.vmap.valid_types(spec.data_type)
            valid_types = [v.spec.data_type for v in valid_validators]
            if isinstance(builder, LinkBuilder):
                dt = builder.builder.attributes.get(spec.type_key())
            else:
                dt = builder.attributes.get(spec.type_key())
            return dt in valid_types

        return list(filter(compatible_type, candidates))

    def _filter_by_unsatisfied(self, candidates):
        """Returns the candidate specs which are not yet matched against
        a number of builders which fulfils the quantity for the spec.
        """
        def is_unsatisfied(spec_matches):
            spec = spec_matches.spec
            n_match = len(spec_matches.builders)
            if spec.required and n_match == 0:
                return True
            if isinstance(spec.quantity, int) and n_match < spec.quantity:
                return True
            return False

        return list(filter(is_unsatisfied, candidates))
