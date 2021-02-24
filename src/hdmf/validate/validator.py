import re
from abc import ABCMeta, abstractmethod
from copy import copy
from itertools import chain
from collections import defaultdict, namedtuple

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

        # build tree that isn't really a tree
        # map(type, list of child types or self)
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

        # map(type, validators of child types or self)
        self.__valid_types = dict()
        # map(type, validator of self)
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
        # recursively go through subtypes and convert to tuple when complete
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

    def __validate_children(self, builder):
        children = self.spec.datasets + self.spec.groups + self.spec.links
        matcher = SpecMatcher(self.vmap, children)

        def _iter_builders(builder):
            # TODO: define sorting order!
            yield from builder.datasets.values()
            yield from builder.groups.values()
            yield from builder.links.values()

        for child_builder in _iter_builders(builder):
            spec = matcher.best_matching_spec(child_builder)
            if spec is None:
                # Superfluous
                continue
            matcher.assign_builder(child_builder, spec)

        for spec_matches in matcher.spec_matches:
            yield from self.__validate_child_spec(spec_matches.spec, spec_matches.builders, builder)

    def __validate_child_spec(self, child_spec, builders, parent_builder):
        n_builders = len(builders)
        resolved_spec = self.__resolve_links(child_spec)
        if n_builders == 0 and child_spec.required:
            if resolved_spec.data_type is not None:
                yield MissingDataType(self.get_spec_loc(self.spec), resolved_spec.data_type,
                                      location=self.get_builder_loc(parent_builder), missing_dt_name=resolved_spec.name)
            else:
                yield MissingError(self.get_spec_loc(child_spec), location=self.get_builder_loc(parent_builder))
        elif self.__incorrect_quantity(n_builders, child_spec):
            yield IncorrectQuantityError(self.get_spec_loc(self.spec), resolved_spec.data_type, child_spec.quantity,
                                         n_builders, location=self.get_builder_loc(parent_builder))

        for child_builder in builders:
            if isinstance(child_builder, LinkBuilder):
                if not isinstance(child_spec, LinkSpec) and not child_spec.linkable:
                    yield IllegalLinkError(self.get_spec_loc(child_spec), location=self.get_builder_loc(parent_builder))
                    continue  # do not validate illegally linked objects
                child_builder = child_builder.builder
            if resolved_spec.data_type is None:
                child_validator = self.__create_untyped_validator(resolved_spec)
            else:
                child_validator = self.vmap.get_validator(resolved_spec.data_type)
            yield from child_validator.validate(child_builder)

    def __resolve_links(self, spec):
        if isinstance(spec, LinkSpec):
            validator = self.vmap.get_validator(spec.target_type)
            return validator.spec
        return spec

    @staticmethod
    def __filter_by_name_if_required(builders, name):
        if name is None:
            return builders
        return filter(lambda x: x.name == name, builders)

    @staticmethod
    def __cannot_be_link(spec):
        return not isinstance(spec, LinkSpec) and not spec.linkable

    @staticmethod
    def __incorrect_quantity(n_found, spec):
        """Returns a boolean indicating whether the number of builder elements matches the specified quantity"""
        if not spec.is_many() and n_found > 1:
            return True
        elif isinstance(spec.quantity, int) and n_found != spec.quantity:
            return True
        return False

    def __create_untyped_validator(self, spec):
        if isinstance(spec, GroupSpec):
            return GroupValidator(spec, self.vmap)
        elif isinstance(spec, DatasetSpec):
            return DatasetValidator(spec, self.vmap)
        else:
            raise ValueError(spec)


SpecMatches = namedtuple('SpecMatches', ('spec', 'builders'))


class SpecMatcher:
    def __init__(self, vmap, specs):
        self.vmap = vmap
        self.spec_matches = [SpecMatches(spec, list()) for spec in specs]

    def best_matching_spec(self, builder):
        candidates = self._filter_by_name(self.spec_matches, builder)
        candidates = self._filter_by_type(candidates, builder)
        if len(candidates) == 0:
            return None
        elif len(candidates) == 1:
            return candidates[0].spec
        else:
            unsatisfied_candidates = self._filter_by_unsatisfied(candidates)
            if len(unsatisfied_candidates) == 0:
                return candidates[0].spec
            else:
                return unsatisfied_candidates[0].spec

    def _filter_by_name(self, candidates, builder):
        def same_name(spec_matches):
            spec = spec_matches.spec
            return spec.name is None or spec.name == builder.name
        return list(filter(same_name, candidates))

    def _filter_by_type(self, candidates, builder):
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
        def is_unsatisfied(spec_matches):
            spec = spec_matches.spec
            n_match = len(spec_matches.builders)
            if spec.required and n_match == 0:
                return True
            if isinstance(spec.quantity, int) and n_match < spec.quantity:
                return True
            return False
        return list(filter(is_unsatisfied, candidates))

    def assign_builder(self, child_builder, spec):
        for cm in self.spec_matches:
            if spec == cm.spec:
                cm.builders.append(child_builder)
                return
