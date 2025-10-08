from abc import ABC, abstractmethod
from enum import Enum
from itertools import chain
from typing import Any, ClassVar, Literal, Optional, Self, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator, PositiveInt, validate_call


# TODO: breaking change. specs are no longer dicts, no longer mutable, and no longer accessible via getitem
# no longer supports positional args. dims and shape are always converted to tuples if provided as lists
# TODO: error messages have changed
# DtypeHelper.check_dtype no longer returns the dtype, it just raises an error if invalid
# TODO: removed support for DatasetSpec.default_value
# TODO: removed support for BaseStorageSpec.linkable

class DtypeHelper:
    # Dict where the keys are the primary data type and the values are list of strings with synonyms for the dtype
    # make sure keys are consistent between hdmf.spec.spec.DtypeHelper.primary_dtype_synonyms,
    # hdmf.build.objectmapper.ObjectMapper.__dtypes, hdmf.build.manager.TypeMap._spec_dtype_map,
    # hdmf.validate.validator.__allowable, and backend dtype maps
    # see https://hdmf-schema-language.readthedocs.io/en/latest/description.html#dtype
    primary_dtype_synonyms = {
        'float': ["float", "float32"],
        'double': ["double", "float64"],
        'short': ["int16", "short"],
        'int': ["int32", "int"],
        'long': ["int64", "long"],
        'utf': ["text", "utf", "utf8", "utf-8"],
        'ascii': ["ascii", "bytes"],
        'bool': ["bool"],
        'int8': ["int8"],
        'uint8': ["uint8"],
        'uint16': ["uint16"],
        'uint32': ["uint32", "uint"],
        'uint64': ["uint64"],
        'object': ['object'],
        'numeric': ['numeric'],
        'isodatetime': ["isodatetime", "datetime", "date"]
    }

    # List of recommended primary dtype strings. These are the keys of primary_dtype_string_synonyms
    recommended_primary_dtypes = list(primary_dtype_synonyms.keys())

    # List of valid primary data type strings
    valid_primary_dtypes = set(list(primary_dtype_synonyms.keys()) +
                               [vi for v in primary_dtype_synonyms.values() for vi in v])

    # TODO: evaluate whether this is necessary
    @staticmethod
    def simplify_cpd_type(cpd_type: list['DtypeSpec']) -> list[str]:
        '''
        Transform a list of DtypeSpecs into a list of strings.
        Use for simple representation of compound type and validation.

        For example, a compound type with fields of dtype 'float' and 'int'
        will be transformed to ['float', 'int'], and a compound type with fields
        of dtype 'float' and RefSpec(target_type='MyType', reftype='object')
        will be transformed to ['float', 'object'].

        Parameters
        ----------
        cpd_type : list[DtypeSpec]
            The compound type to simplify

        Returns
        -------
        list[str]
            A list of strings representing the simplified compound type.

        '''
        ret = list()
        for exp in cpd_type:
            exp_key = exp.dtype
            if isinstance(exp_key, RefSpec):
                exp_key = exp_key.reftype
            ret.append(exp_key)
        return ret

    @staticmethod
    def check_dtype(dtype: Optional[Union[str, list['DtypeSpec'], 'RefSpec']]) -> None:
        """Check that the dtype string is a reference or a valid primary dtype."""
        if not isinstance(dtype, RefSpec) and dtype not in DtypeHelper.valid_primary_dtypes:
            raise ValueError("dtype '%s' is not a valid primary data type. Allowed dtypes: %s"
                             % (dtype, str(DtypeHelper.valid_primary_dtypes)))

    # all keys and values should be keys in primary_dtype_synonyms
    additional_allowed = {
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
    allowable = dict()
    for dt, dt_syn in primary_dtype_synonyms.items():
        allow = dt_syn.copy()
        if dt in additional_allowed:
            for addl in additional_allowed[dt]:
                allow.extend(primary_dtype_synonyms[addl])
        for syn in dt_syn:
            allowable[syn] = allow
    allowable['numeric'].extend(set(chain.from_iterable(v for k, v in allowable.items() if 'int' in k or 'float' in k)))

    @staticmethod
    def is_allowed_dtype(new: str, orig: str):
        if orig not in DtypeHelper.allowable:
            raise ValueError(f"Invalid dtype '{orig}'.")
        return new in DtypeHelper.allowable[orig]


class Spec(BaseModel, ABC):
    """
    A base specification class.
    """
    model_config = ConfigDict(extra='forbid', validate_assignment=True, validate_default=True)

    doc: str
    """Documentation on what this specification is specifying."""

    name: Optional[str] = None
    """The name of the object being specified."""

    _parent: Optional["Spec"] = None
    """The parent specification of this specification."""

    @property
    def parent(self) -> Optional["Spec"]:
        return self._parent

    @parent.setter
    def parent(self, value: "Spec") -> None:
        if self._parent is not None and self._parent != value:
            raise ValueError('Parent cannot be changed after being set.')
        self._parent = value

    # TODO: make hashable

    @property
    def path(self) -> str:
        """The full path of this specification in the hierarchy of specifications."""
        stack = list()
        tmp = self
        while tmp is not None:
            name = tmp.name or tmp.data_type_def or tmp.data_type_inc
            # name = tmp.name
            # if name is None:
            #     name = tmp.data_type_def
            #     if name is None:
            #         name = tmp.data_type_inc
            stack.append(name)
            tmp = tmp.parent
        return "/".join(reversed(stack))

Spec.parent = Spec.parent.setter(validate_call(config=dict(arbitrary_types_allowed=True))(Spec.parent.fset))


class RefSpec(BaseModel):
    """
    A specification that references another specification.
    """
    model_config = ConfigDict(extra='forbid', validate_assignment=True, validate_default=True)

    target_type: str
    """The type of object that this specification references (e.g., Group, Dataset, etc.)."""

    reftype: Literal["object"]
    """The type of reference. Only "object" is supported."""


class AttributeSpec(Spec):
    """
    A specification for an attribute.
    """

    required: Optional[bool] = True
    """Whether or not this specification is required. Default is True."""

    dtype: Union[str, RefSpec]
    """The data type of the attribute."""
    # NOTE: compound data types are not supported in attributes

    shape: Optional[tuple] = None
    """The shape of the attribute."""

    dims: Optional[tuple] = None
    """The dimensions of the attribute."""

    value: Optional[Any] = None
    """A constant value for this attribute."""

    default_value: Optional[Any] = None
    """A default value for this attribute."""

    @model_validator(mode="before")
    def set_dims_based_on_shape(cls, data: dict) -> dict:
        """If 'shape' is provided but 'dims' is not, set 'dims' to default dimension names."""
        if 'shape' in data and data['shape'] is not None and ('dims' not in data or data['dims'] is None):
            data['dims'] = tuple(f"dim_{i}" for i in range(len(data['shape'])))
        return data

    @model_validator(mode="before")
    def set_shape_based_on_dims(cls, data: dict) -> dict:
        """If 'dims' is provided but 'shape' is not, set 'shape' to (None,)*len(dims)."""
        if 'dims' in data and data['dims'] is not None and ('shape' not in data or data['shape'] is None):
            data['shape'] = tuple(None for _ in range(len(data['dims'])))
        return data

    @model_validator(mode="before")
    def set_optional_if_default_value(cls, data: dict) -> dict:
        """If 'default_value' is provided, set 'required' to False."""
        if 'default_value' in data and data['default_value'] is not None:
            data['required'] = False  # TODO handle not setting this so that it is not printed in model_dump
        return data

    @model_validator(mode="after")
    def check_dtype_valid(self) -> Self:
        DtypeHelper.check_dtype(self.dtype)
        return self

    @model_validator(mode="after")
    def check_not_both_value_and_default_value(self) -> Self:
        if (self.value is not None and self.default_value is not None):
            raise ValueError("Cannot specify both 'value' and 'default_value'.")
        return self

    @model_validator(mode="after")
    def check_not_both_value_and_optional(self) -> Self:
        if self.value is not None and self.required is False:
            raise ValueError("Cannot specify 'value' and 'required=False' at the same time.")
        return self

    @model_validator(mode="after")
    def check_dims_and_shape_same_length(self) -> Self:
        if self.dims is not None and self.shape is not None:
            if len(self.dims) != len(self.shape):
                raise ValueError("'dims' and 'shape' must have the same length.")
        return self

    @classmethod
    def build_spec(cls, spec_dict: dict[str, Any]) -> 'AttributeSpec':
        ''' Build a specification from a dictionary

        Parameters
        ----------
        spec_dict : dict[str, Any]
            The dictionary to build the specification from

        Returns
        -------
        AttributeSpec
            The constructed AttributeSpec object
        '''
        input_dict = spec_dict.copy()
        if isinstance(input_dict['dtype'], dict):
            input_dict['dtype'] = RefSpec(**input_dict['dtype'])
        return cls(**input_dict)


class QuantityEnum(str, Enum):
    """
    An enum for quantity values.

    The ZERO_OR_ONE value can be represented by either '?' or 'zero_or_one'.
    The ZERO_OR_MANY value can be represented by either '*' or 'zero_or_many'.
    The ONE_OR_MANY value can be represented by either '+' or 'one_or_many'.
    """
    ZERO_OR_ONE = '?'
    ZERO_OR_MANY = '*'
    ONE_OR_MANY = '+'

    @classmethod
    def _missing_(cls, value):
        """Allow string aliases to map to enum values."""
        if value == 'zero_or_one':
            return cls.ZERO_OR_ONE
        elif value == 'zero_or_many':
            return cls.ZERO_OR_MANY
        elif value == 'one_or_many':
            return cls.ONE_OR_MANY
        return None


class BaseStorageSpec(Spec, ABC):
    """
    A base specification for groups and datasets.
    """

    __inc_key: ClassVar[str] = 'data_type_inc'
    __def_key: ClassVar[str] = 'data_type_def'
    __type_key: ClassVar[str] = 'data_type'
    __id_key: ClassVar[str] = 'object_id'

    data_type_def: Optional[str] = None
    """The data type that this specification defines."""

    data_type_inc: Optional[str] = None
    """The data type that this specification extends."""

    default_name: Optional[str] = None
    """The default name of this specification."""

    quantity: Union[PositiveInt, QuantityEnum] = 1
    """The quantity of this specification. Can be a positive integer or one of '?' (zero or one), '*' (zero or more),
    '+' (one or more)."""

    linkable: bool = Field(default=False, exclude=True)
    """DEPRECATED and ignored. Maintained for backwards compatibility. Whether this specification can be linked to."""

    attributes: list[AttributeSpec] = list()
    """A list of attribute specifications for this specification."""

    _not_inherited_attributes: dict[str, AttributeSpec] = dict()
    """A dictionary of attribute specifications that are defined on this specification and not inherited from
    an included data type (data_type_inc)."""
    # TODO single or double underscore?
    # TODO allow these to be modified

    _overridden_attributes: dict[str, AttributeSpec] = dict()
    """A dictionary of attribute specifications that override attributes from an included data type
    (data_type_inc)."""

    __inc_spec_resolved: bool = False
    """Whether or not this specification has been fully resolved (i.e., fields from an included data type
    (data_type_inc) have been merged into this specification)."""

    __resolved: bool = False
    """Whether or not this specification has been fully resolved (i.e., fields from an included data type
    (data_type_inc) and any fields from subspecs with an included data type (data_type_inc) have been merged
    into this specification)."""

    def model_post_init(self, context: Any) -> None:
        """ Set parent on attribute specs after initialization """
        for attr in self.attributes:
            attr.parent = self

    @field_validator("name", mode="after")
    def check_valid_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and '/' in v:
            raise ValueError("Invalid character '/' in 'name'.")
        return v

    @field_validator("default_name", mode="after")
    def check_valid_default_name(cls, v: Optional[str]) -> Optional[str]:
        if v is not None and '/' in v:
            raise ValueError("Invalid character '/' in 'default_name'.")
        return v

    @model_validator(mode="after")
    def check_name_or_data_type_def_or_data_type_inc(self) -> Self:
        if self.name is None and self.data_type_def is None and self.data_type_inc is None:
            raise ValueError("At least one of 'name', 'data_type_def', or 'data_type_inc' must be specified.")
        return self

    @model_validator(mode="after")
    def check_not_both_name_and_default_name(self) -> Self:
        if self.name is not None and self.default_name is not None:
            raise ValueError("Cannot specify both 'name' and 'default_name'.")
        return self

    @model_validator(mode="after")
    def check_not_name_and_many_quantity(self) -> Self:
        if self.name is not None:
            if isinstance(self.quantity, int):
                if self.quantity != 1:
                    raise ValueError("Cannot specify 'name' with a quantity other than 1.")
            elif self.is_many():
                raise ValueError("Cannot specify 'name' on a spec that can exist multiple times.")
        return self

    @model_validator(mode="after")
    def check_data_type_def_not_equal_data_type_inc(self) -> Self:
        if self.data_type_def is not None and self.data_type_inc is not None:
            if self.data_type_def == self.data_type_inc:
                raise ValueError(
                    f"Cannot specify the same value for 'data_type_def' and 'data_type_inc': {self.data_type_def}"
                )
        return self

    def is_many(self) -> bool:
        return self.quantity not in (1, QuantityEnum.ZERO_OR_ONE)

    @property
    @validate_call
    def inc_spec_resolved(self) -> bool:
        return self.__inc_spec_resolved

    @property
    @validate_call
    def resolved(self) -> bool:
        return self.__resolved

    @resolved.setter
    @validate_call
    def resolved(self, val: bool) -> None:
        self.__resolved = val

    @property
    def required(self) -> bool:
        ''' Whether or not this specification is required. '''
        return self.quantity not in (QuantityEnum.ZERO_OR_ONE, QuantityEnum.ZERO_OR_MANY)

    # TODO add namespace: SpecNamespace hint
    @abstractmethod
    def resolve_inc_spec(self, inc_spec: 'BaseStorageSpec', namespace) -> None:
        pass

    # TODO determine whether we need to track inherited and overridden attributes

    @classmethod
    def id_key(cls) -> str:
        ''' Get the key used to store data ID on an instance

        Override this method to use a different name for 'object_id'
        '''
        return cls.__id_key

    @classmethod
    def type_key(cls) -> str:
        ''' Get the key used to store data type on an instance

        Override this method to use a different name for 'data_type'. HDMF supports combining schema
        that uses 'data_type' and at most one different name for 'data_type'.
        '''
        return cls.__type_key

    @classmethod
    def inc_key(cls) -> str:
        ''' Get the key used to define a data_type include.

        Override this method to use a different keyword for 'data_type_inc'. HDMF supports combining schema
        that uses 'data_type_inc' and at most one different name for 'data_type_inc'.
        '''
        return cls.__inc_key

    @classmethod
    def def_key(cls) -> str:
        ''' Get the key used to define a data_type definition.

        Override this method to use a different keyword for 'data_type_def' HDMF supports combining schema
        that uses 'data_type_def' and at most one different name for 'data_type_def'.
        '''
        return cls.__def_key

    @property
    def data_type(self) -> Optional[str]:
        ''' The data type of this specification '''
        return self.data_type_def or self.data_type_inc

    @validate_call
    def set_attribute(self, spec: AttributeSpec) -> None:
        ''' Add an attribute specification to this specification

        Parameters
        ----------
        spec : AttributeSpec
            The attribute specification to add

        Raises
        ------
        ValueError
            If an attribute with the same name already exists
        '''
        # NOTE: this method would be better named add_attribute. however, there was previously a method named
        # add_attribute that took the kwargs of AttributeSpec. this needed to be deprecated and removed to avoid
        # confusion.
        # TODO: copy logic from BaseStorageSpec.set_attribute
        # TODO: You can no longer set attributes with the same name as an existing attribute (to replace it).
        if spec.name in [attr.name for attr in self.attributes]:
            raise ValueError(f"Attribute '{spec.name}' already exists in spec '{self.name or self.data_type}'.")
        self.attributes.append(spec)
        spec.parent = self

    @validate_call
    def get_attribute(self, name: str) -> Optional[AttributeSpec]:
        ''' Get an attribute specification by name

        Parameters
        ----------
        name : str
            The name of the attribute specification to get

        Returns
        -------
        Optional[AttributeSpec]
            The attribute specification with the given name, or None if it does not exist
        '''
        return self.attributes.get(name, None)

    @classmethod
    def build_spec(cls, spec_dict: dict[str, Any]) -> 'BaseStorageSpec':
        ''' Build a specification from a dictionary

        Parameters
        ----------
        spec_dict : dict[str, Any]
            The dictionary to build the specification from

        Returns
        -------
        BaseStorageSpec
            The constructed BaseStorageSpec object
        '''
        input_dict = spec_dict.copy()
        if 'attributes' in input_dict:
            input_dict['attributes'] = [AttributeSpec.build_spec(sub_spec) for sub_spec in input_dict['attributes']]
        return cls(**input_dict)

BaseStorageSpec.resolve_inc_spec = validate_call(config=dict(arbitrary_types_allowed=True))(
    BaseStorageSpec.resolve_inc_spec
)
BaseStorageSpec.build_spec = validate_call(config=dict(arbitrary_types_allowed=True))(BaseStorageSpec.build_spec)

class DtypeSpec(BaseModel):
    """
    A specification for a field of a compound data type.
    """
    model_config = ConfigDict(extra='forbid', validate_assignment=True, validate_default=True)

    name: str
    """The name of this field in the compound data type."""

    doc: str
    """Documentation on what this field is specifying."""

    dtype: Union[str, RefSpec]
    """The data type of this field."""

    # TODO: breaking change assertValidDtype and is_ref are removed
    @model_validator(mode="after")
    def check_dtype_valid(self) -> Self:
        DtypeHelper.check_dtype(self.dtype)
        return self

    @classmethod
    def build_spec(cls, spec_dict: dict[str, Any]) -> 'DtypeSpec':
        ''' Build a specification from a dictionary

        Parameters
        ----------
        spec_dict : dict[str, Any]
            The dictionary to build the specification from

        Returns
        -------
        DtypeSpec
            The constructed DtypeSpec object
        '''
        input_dict = spec_dict.copy()
        # TODO why this transformation?
        # NOTE: Nesting DtypeSpecs is not supported
        # if isinstance(ret['dtype'], list):
        #     input_dict['dtype'] = list(map(cls.build_const_args, input_dict['dtype']))
        if isinstance(input_dict['dtype'], dict):
            input_dict['dtype'] = RefSpec(**input_dict['dtype'])
        return cls(**input_dict)


class DatasetSpec(BaseStorageSpec):
    """
    A specification for a dataset.
    """

    dtype: Optional[Union[str, list[DtypeSpec], RefSpec]] = None
    """The data type of the dataset. Use a list of DtypeSpecs to specify a compound data type.
    Use None for untyped datasets."""
    # Unlike AttributeSpec, compound data types are supported in DatasetSpec and dtype can be None for untyped datasets

    shape: Optional[tuple] = None
    """The shape of the dataset."""
    # TODO: specify the type of the tuple elements (int or None)

    dims: Optional[tuple] = None
    """The dimensions of the dataset."""

    value: Optional[Any] = None
    """A constant value for this dataset."""

    default_value: Optional[Any] = None
    """DEPRECATED and ignored. Maintained for backwards compatibility. A default value for this dataset."""

    @model_validator(mode="before")
    def set_dims_based_on_shape(cls, data: dict) -> dict:
        """If 'shape' is provided but 'dims' is not, set 'dims' to default dimension names."""
        if 'shape' in data and data['shape'] is not None and ('dims' not in data or data['dims'] is None):
            data['dims'] = tuple(f"dim_{i}" for i in range(len(data['shape'])))
        return data

    @model_validator(mode="before")
    def set_shape_based_on_dims(cls, data: dict) -> dict:
        """If 'dims' is provided but 'shape' is not, set 'shape' to (None,)*len(dims)."""
        if 'dims' in data and data['dims'] is not None and ('shape' not in data or data['shape'] is None):
            data['shape'] = tuple(None for _ in range(len(data['dims'])))
        return data

    @model_validator(mode="after")
    def check_dtype_valid(self) -> Self:
        if self.dtype is not None:
            if isinstance(self.dtype, list):
                for dt in self.dtype:
                    DtypeHelper.check_dtype(dt.dtype)
            else:
                DtypeHelper.check_dtype(self.dtype)
        return self

    @model_validator(mode="after")
    def check_not_both_value_and_optional(self) -> Self:
        if self.value is not None and self.required is False:
            raise ValueError("Cannot specify 'value' and 'required=False' at the same time.")
        return self

    @model_validator(mode="after")
    def check_dims_and_shape_same_length(self) -> Self:
        if self.dims is not None and self.shape is not None:
            if len(self.dims) != len(self.shape):
                raise ValueError("'dims' and 'shape' must have the same length.")
        return self

    # TODO add namespace: SpecNamespace hint
    def resolve_inc_spec(self, inc_spec: 'DatasetSpec', namespace) -> None:
        pass

    @classmethod
    def dtype_spec_cls(cls) -> type[DtypeSpec]:
        ''' The class to use when constructing DtypeSpec objects

            Override this if extending to use a class other than DtypeSpec to build
            dataset specifications
        '''
        return DtypeSpec

    @classmethod
    def build_spec(cls, spec_dict: dict[str, Any]) -> 'DatasetSpec':
        ''' Build a specification from a dictionary

        Parameters
        ----------
        spec_dict : dict[str, Any]
            The dictionary to build the specification from

        Returns
        -------
        DatasetSpec
            The constructed DatasetSpec object
        '''
        input_dict = spec_dict.copy()
        if 'attributes' in input_dict:
            input_dict['attributes'] = [AttributeSpec.build_spec(sub_spec) for sub_spec in input_dict['attributes']]
        if 'dtype' in input_dict:
            if isinstance(input_dict['dtype'], list):  # compound data type
                input_dict['dtype'] = list(map(cls.dtype_spec_cls().build_spec, input_dict['dtype']))
            elif isinstance(input_dict['dtype'], dict):  # reference to another spec
                input_dict['dtype'] = RefSpec(**input_dict['dtype'])
        return cls(**input_dict)

DatasetSpec.resolve_inc_spec = validate_call(config=dict(arbitrary_types_allowed=True))(DatasetSpec.resolve_inc_spec)


class LinkSpec(Spec):
    """
    A specification for a link.
    """

    target_type: str
    """The target type GroupSpec or DatasetSpec."""

    quantity: Union[PositiveInt, QuantityEnum] = 1
    """The quantity of this specification. Can be a positive integer or one of '?' (zero or one), '*' (zero or more),
    '+' (one or more)."""

    name: Optional[str] = None
    """The name of this link."""

    @model_validator(mode="after")
    def check_not_both_name_and_many_quantity(self) -> Self:
        if self.name is not None:
            if isinstance(self.quantity, int):
                if self.quantity != 1:
                    raise ValueError("Cannot specify 'name' with a quantity other than 1.")
            elif self.is_many():
                raise ValueError("Cannot specify 'name' on a spec that can exist multiple times.")
        return self

    def is_many(self) -> bool:
        return self.quantity not in (1, QuantityEnum.ZERO_OR_ONE)

    # TODO removed data_type_inc property

    # TODO can no longer set target_type to a BaseStorageSpec

    @property
    def required(self) -> bool:
        ''' Whether or not this specification is required. '''
        return self.quantity not in (QuantityEnum.ZERO_OR_ONE, QuantityEnum.ZERO_OR_MANY)


class GroupSpec(BaseStorageSpec):
    """
    A specification for a group.
    """

    # NOTE: Some groups, datasets, and links do not have names

    groups: list['GroupSpec'] = list()
    """A list of subgroup specifications for this group."""

    datasets: list[DatasetSpec] = list()
    """A list of dataset specifications for this group."""

    links: list[LinkSpec] = list()
    """A list of link specifications for this group."""

    def model_post_init(self, context: Any) -> None:
        """ Set parent on subspecs after initialization """
        for subspec in self.groups + self.datasets + self.links:
            subspec.parent = self
        super().model_post_init(context)

    @model_validator(mode="after")
    def check_no_name_conflicts_groups_datasets_links(self) -> Self:
        # Check for name conflicts between groups, datasets, and links.
        # Conflicts with attribute names are not checked because attributes are stored separately.
        all_names = set()
        for spec in self.groups + self.datasets + self.links:
            if spec.name is not None:
                if spec.name in all_names:
                    msg = (
                        f"Name conflict: '{spec.name}' is used in multiple places in group "
                        f"'{self.name or self.data_type}'."
                    )
                    raise ValueError(msg)
                all_names.add(spec.name)
        return self

    @model_validator(mode="after")
    def check_no_duplicate_data_type(self) -> Self:
        # Check for duplicate data_type in groups and datasets.
        # This is to prevent ambiguity when resolving data types.
        all_data_types = set()
        for spec in self.groups + self.datasets:
            if spec.data_type is not None and spec.name is None:
                if spec.data_type in all_data_types:
                    msg = (
                        f"Duplicate data_type: '{spec.data_type}' is used in multiple unnamed subspecs in group "
                        f"'{self.name or self.data_type}'."
                    )
                    raise ValueError(msg)
                all_data_types.add(spec.data_type)
        return self

    @model_validator(mode="after")
    def check_no_duplicate_target_type(self) -> Self:
        # Check for duplicate target_type in links.
        # This is to prevent ambiguity when resolving target types.
        all_target_types = set()
        for spec in self.links:
            if spec.name is None:
                if spec.target_type in all_target_types:
                    msg = (
                        f"Duplicate target_type: '{spec.target_type}' is used in multiple unnamed links in group "
                        f"'{self.name or self.data_type}'."
                    )
                    raise ValueError(msg)
                all_target_types.add(spec.target_type)
        return self

    def set_group(self, spec: 'GroupSpec') -> None:
        ''' Add a subgroup specification to this group spec

        Parameters
        ----------
        spec : GroupSpec
            The subgroup specification to add

        Raises
        ------
        ValueError
            If a subgroup with the same name already exists
        '''
        if spec.name is not None and self.get_group(spec.name) is not None:
            raise ValueError(f"A subgroup with name '{spec.name}' already exists.")
        if spec.name is None and self.get_data_type(spec.data_type) is not None:
            raise ValueError(f"A spec with data_type '{spec.data_type}' already exists.")
        if not self.groups:  # explicitly set self.groups to update the "set" state of the field
            self.groups = list()
        self.groups.append(spec)
        spec.parent = self

    @validate_call
    def set_dataset(self, spec: DatasetSpec) -> None:
        ''' Add a dataset specification to this group spec

        Parameters
        ----------
        spec : DatasetSpec
            The dataset specification to add

        Raises
        ------
        ValueError
            If a dataset with the same name already exists
        '''
        if spec.name is not None and self.get_dataset(spec.name) is not None:
            raise ValueError(f"A dataset with name '{spec.name}' already exists.")
        if spec.name is None and self.get_data_type(spec.data_type) is not None:
            raise ValueError(f"A spec with data_type '{spec.data_type}' already exists.")
        if not self.datasets:  # explicitly set self.datasets to update the "set" state of the field
            self.datasets = list()
        self.datasets.append(spec)
        spec.parent = self

    @validate_call
    def set_link(self, spec: LinkSpec) -> None:
        ''' Add a link specification to this group spec

        Parameters
        ----------
        spec : LinkSpec
            The link specification to add

        Raises
        ------
        ValueError
            If a link with the same name already exists
        '''
        if spec.name is not None and self.get_link(spec.name) is not None:
            raise ValueError(f"A link with name '{spec.name}' already exists.")
        if spec.name is None and self.get_target_type(spec.target_type) is not None:
            raise ValueError(f"A spec with target_type '{spec.target_type}' already exists.")
        if not self.links:  # explicitly set self.links to update the "set" state of the field
            self.links = list()
        self.links.append(spec)
        spec.parent = self

    def get_group(self, name: str) -> Optional['GroupSpec']:
        ''' Get a subgroup specification by name

        Parameters
        ----------
        name : str
            The name of the subgroup specification to get

        Returns
        -------
        Optional[GroupSpec]
            The subgroup specification with the given name, or None if it does not exist
        '''
        # Some groups may not have names, so we need to search the list.
        for group in self.groups:
            if group.name == name:
                return group
        return None

    @validate_call
    def get_dataset(self, name: str) -> Optional[DatasetSpec]:
        ''' Get a dataset specification by name

        Parameters
        ----------
        name : str
            The name of the dataset specification to get

        Returns
        -------
        Optional[DatasetSpec]
            The dataset specification with the given name, or None if it does not exist
        '''
        # Some datasets may not have names, so we need to search the list.
        for dataset in self.datasets:
            if dataset.name == name:
                return dataset
        return None

    @validate_call
    def get_link(self, name: str) -> Optional[LinkSpec]:
        ''' Get a link specification by name

        Parameters
        ----------
        name : str
            The name of the link specification to get

        Returns
        -------
        Optional[LinkSpec]
            The link specification with the given name, or None if it does not exist
        '''
        # Some links may not have names, so we need to search the list.
        for link in self.links:
            if link.name == name:
                return link
        return None

    def get_data_type(self, data_type: str = None) -> Optional[
        Union['GroupSpec', DatasetSpec, list[Union['GroupSpec', DatasetSpec]]]
    ]:
        ''' Get a GroupSpec or DatasetSpec by "data_type"

        The "data_type" for a spec is defined as the value of "data_type_def" if it exists,
        otherwise the value of "data_type_inc".

        NOTE: If there is only one spec for a given data type, then it is returned.
        If there are multiple specs for a given data type and they are all named, then they are returned in a list.
        If there are multiple specs for a given data type and only one is unnamed, then the unnamed spec is returned.
        The other named specs can be returned using get_group or get_dataset.

        NOTE: this method looks for an exact match of the data type and does not consider the type hierarchy.
        '''
        # TODO: This is really confusing behavior. Consider always returning a list and letting the user handle it.
        matching_specs = [spec for spec in (self.groups + self.datasets) if spec.data_type == data_type]
        if len(matching_specs) == 1:
            return matching_specs[0]
        elif len(matching_specs) > 1:
            unnamed_specs = [spec for spec in matching_specs if spec.name is None]
            if len(unnamed_specs) == 1:
                return unnamed_specs[0]
            else:
                return matching_specs
        return None

    @validate_call
    def get_target_type(self, target_type: str = None) -> Optional[Union[LinkSpec, list[LinkSpec]]]:
        ''' Get a specification by "target_type"

        NOTE: If there is only one spec for a given target type, then it is returned.
        If there are multiple specs for a given target type and they are all named, then they are returned in a list.
        If there are multiple specs for a given target type and only one is unnamed, then the unnamed spec is returned.
        The other named specs can be returned using get_link.

        NOTE: this method looks for an exact match of the target type and does not consider the type hierarchy.
        '''
        # TODO: This is really confusing behavior. Consider always returning a list and letting the user handle it.
        matching_specs = [spec for spec in self.links if spec.target_type == target_type]
        if len(matching_specs) == 1:
            return matching_specs[0]
        elif len(matching_specs) > 1:
            unnamed_specs = [spec for spec in matching_specs if spec.name is None]
            if len(unnamed_specs) == 1:
                return unnamed_specs[0]
            else:
                return matching_specs
        return None

    # add namespace type hint
    def resolve_inc_spec(self, inc_spec: 'GroupSpec', namespace) -> None:
        pass

    # add is inherited / overridden tracking

    # TODO removed get_group, get_dataset, get_link methods

    @classmethod
    def dataset_spec_cls(cls) -> type[DatasetSpec]:
        ''' The class to use when constructing DatasetSpec objects

            Override this if extending to use a class other than DatasetSpec to build
            dataset specifications
        '''
        return DatasetSpec

    @classmethod
    def link_spec_cls(cls) -> type[LinkSpec]:
        ''' The class to use when constructing LinkSpec objects

            Override this if extending to use a class other than LinkSpec to build
            link specifications
        '''
        return LinkSpec

    @classmethod
    def build_spec(cls, spec_dict: dict[str, Any]) -> 'GroupSpec':
        ''' Build a specification from a dictionary

        Parameters
        ----------
        spec_dict : dict[str, Any]
            The dictionary to build the specification from

        Returns
        -------
        GroupSpec
            The constructed GroupSpec object
        '''
        input_dict = spec_dict.copy()
        if 'attributes' in input_dict:
            input_dict['attributes'] = [AttributeSpec.build_spec(sub_spec) for sub_spec in input_dict['attributes']]
        if 'datasets' in input_dict:
            input_dict['datasets'] = [
                cls.dataset_spec_cls().build_spec(sub_spec) for sub_spec in input_dict['datasets']
            ]
        if 'links' in input_dict:
            input_dict['links'] = [cls.link_spec_cls().build_spec(sub_spec) for sub_spec in input_dict['links']]
        if 'groups' in input_dict:
            input_dict['groups'] = [cls.build_spec(sub_spec) for sub_spec in input_dict['groups']]
        return cls(**input_dict)

# Validation of add_group can happen only after GroupSpec is fully defined
GroupSpec.set_group = validate_call(config=dict(arbitrary_types_allowed=True))(GroupSpec.set_group)
GroupSpec.get_group = validate_call(config=dict(arbitrary_types_allowed=True))(GroupSpec.get_group)
GroupSpec.get_data_type = validate_call(config=dict(arbitrary_types_allowed=True))(GroupSpec.get_data_type)
GroupSpec.resolve_inc_spec = validate_call(config=dict(arbitrary_types_allowed=True))(GroupSpec.resolve_inc_spec)

# TODO validate call on build_spec throughout
