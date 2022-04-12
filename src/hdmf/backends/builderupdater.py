import json
import jsonschema
import numpy as np
from pathlib import Path

from ..build import GroupBuilder, ObjectMapper, unicode, ascii
from ..utils import docval, getargs


class BuilderUpdater:

    json_schema_file = 'sidecar.schema.json'

    @classmethod
    def get_json_schema(cls):
        """Load the sidecar JSON schema."""
        with open(cls.json_schema_file, 'r') as file:
            schema = json.load(file)
        return schema

    @classmethod
    def validate_sidecar(cls, sidecar_dict):
        """Validate the sidecar JSON dict with the sidecar JSON schema."""
        try:
            jsonschema.validate(instance=sidecar_dict, schema=cls.get_json_schema())
        except jsonschema.exceptions.ValidationError as e:
            raise SidecarValidationError() from e

    @classmethod
    def convert_dtype_using_map(cls, value, dtype_str):
        dtype = ObjectMapper.get_dtype_mapping(dtype_str)
        if isinstance(value, (list, np.ndarray)):  # array
            if dtype is unicode:
                dtype = 'U'
            elif dtype is ascii:
                dtype = 'S'
            if isinstance(value, list):
                ret = np.array(value, dtype=dtype)
            else:
                ret = value.astype(dtype)
        else:  # scalar
            ret = dtype(value)
        return ret

    @classmethod
    def convert_dtype_helper(cls, value, dtype):
        # dtype comes from datasetbuilder or attribute
        if isinstance(value, (list, np.ndarray)):  # array
            if dtype is str:
                dtype = 'U'
            elif dtype is bytes:
                dtype = 'S'
            else:
                dtype = np.dtype(dtype)
            if isinstance(value, list):
                ret = np.array(value, dtype=dtype)
            else:
                ret = value.astype(dtype)
        else:  # scalar
            if dtype in (str, bytes):
                ret = dtype(value)
            else:
                ret = dtype.type(value)
        return ret

    @classmethod
    def convert_dtype(cls, value, dtype, old_value):
        if value is None:
            return None
        # TODO handle compound dtypes
        if dtype is not None:
            new_value = cls.convert_dtype_using_map(value, dtype)
        else:  # keep the same dtype
            if isinstance(old_value, np.ndarray):
                new_value = cls.convert_dtype_helper(value, old_value.dtype)
            else:
                assert old_value is not None, \
                    "Cannot convert new value to dtype without specifying new dtype or old value."
                new_value = cls.convert_dtype_helper(value, type(old_value))
        return new_value

    @classmethod
    @docval(
        {'name': 'file_builder', 'type': GroupBuilder, 'doc': 'A GroupBuilder representing the main file object.'},
        {'name': 'file_path', 'type': str,
         'doc': 'Path to the data file. The sidecar file is assumed to have the same base name but with suffix .json.'},
        returns='The same input GroupBuilder, now modified.',
        rtype='GroupBuilder'
    )
    def update_from_sidecar_json(cls, **kwargs):
        """Update the file builder in-place with the values specified in the sidecar JSON."""
        # the sidecar json must have the same name as the file but with suffix .json
        f_builder, path = getargs('file_builder', 'file_path', kwargs)
        sidecar_path = Path(path).with_suffix('.json')
        if not sidecar_path.is_file():
            return

        with open(sidecar_path, 'r') as f:
            sidecar_dict = json.load(f)
            cls.validate_sidecar(sidecar_dict)

            operations = sidecar_dict['operations']
            for operation in operations:
                object_id = operation['object_id']
                relative_path = operation['relative_path']
                operation_type = operation['type']
                new_value = operation.get('value')
                new_dtype = operation.get('dtype')

                builder_map = cls.__get_object_id_map(f_builder)
                builder = builder_map[object_id]
                # TODO handle paths to links
                # TODO handle object references
                if operation_type == 'replace':
                    cls.__replace(builder, relative_path, new_value, new_dtype)
                elif operation_type == 'delete':
                    cls.__delete(builder, relative_path)
                # elif operation_type == 'change_dtype':
                #     assert new_value is None
                #     cls.__change_dtype(builder, relative_path, new_dtype)
                # elif operation_type == 'create_attribute':
                #     cls.__create_attribute(builder, relative_path, new_value, new_dtype)
                else:
                    raise ValueError("Operation type: '%s' not supported." % operation_type)

        return f_builder

    @classmethod
    def __replace(cls, builder, relative_path, new_value, new_dtype):
        if relative_path in builder.attributes:
            cls.__replace_attribute(builder, relative_path, new_value, new_dtype)
        elif isinstance(builder, GroupBuilder):  # object_id points to GroupBuilder
            sub_builder, attr_name = builder.get_subbuilder(relative_path)
            if sub_builder is None:
                raise ValueError("Relative path '%s' not recognized as a group, dataset, or attribute."
                                 % relative_path)
            if attr_name is None:
                cls.__replace_dataset_data(sub_builder, new_value, new_dtype)
            else:
                cls.__replace_attribute(sub_builder, attr_name, new_value, new_dtype)
        else:  # object_id points to DatasetBuilder
            if not relative_path:
                cls.__replace_dataset_data(builder, new_value, new_dtype)
            else:
                raise ValueError("Relative path '%s' not recognized as None or attribute." % relative_path)

    @classmethod
    def __delete(cls, builder, relative_path):
        if relative_path in builder.attributes:  # relative_path is name of attribute in GroupBuilder/DatasetBuilder
            cls.__delete_attribute(builder, relative_path)
        elif isinstance(builder, GroupBuilder):  # object_id points to GroupBuilder
            sub_builder, attr_name = builder.get_subbuilder(relative_path)
            if sub_builder is None:
                raise ValueError("Relative path '%s' not recognized as a group, dataset, or attribute."
                                 % relative_path)
            if attr_name is None:
                # delete the DatasetBuilder from its parent
                cls.__delete_builder(sub_builder.parent, sub_builder)
            else:
                cls.__delete_attribute(sub_builder, attr_name)  # delete the attribute from the sub-Builder
        else:  # object_id points to DatasetBuilder
            if not relative_path:
                cls.__delete_builder(builder.parent, builder)  # delete the DatasetBuilder from its parent
            else:
                raise ValueError("Relative path '%s' not recognized as None or attribute." % relative_path)

    @classmethod
    # NOTE this function is currently unused
    def __change_dtype(cls, builder, relative_path, new_dtype):
        if relative_path in builder.attributes:
            cls.__change_dtype_attribute(builder, relative_path, new_dtype)
        elif isinstance(builder, GroupBuilder):  # GroupBuilder has object_id
            sub_dset_builder, attr_name = builder.get_subbuilder(relative_path)
            if sub_dset_builder is None:
                raise ValueError("Relative path '%s' not recognized as a group, dataset, or attribute."
                                 % relative_path)
            if attr_name is None:  # update data in sub-DatasetBuilder
                cls.__change_dtype_dataset_data(sub_dset_builder, new_dtype)
            else:
                cls.__change_dtype_attribute(sub_dset_builder, attr_name, new_dtype)
        else:  # DatasetBuilder has object_id
            if not relative_path:
                cls.__change_dtype_dataset_data(builder, new_dtype)
            else:
                raise ValueError("Relative path '%s' not recognized as None or attribute." % relative_path)

    @classmethod
    def __create_attribute(cls, builder, relative_path, new_value, new_dtype):
        # TODO validate in jsonschema that the relative path cannot start or end with '/'
        if '/' in relative_path:  # GroupBuilder has object_id
            assert isinstance(builder, GroupBuilder), \
                "Relative path '%s' can include '/' only if the object is a group." % relative_path
            sub_dset_builder, attr_name = builder.get_subbuilder(relative_path)
            if sub_dset_builder is None:
                raise ValueError("Relative path '%s' not recognized as a sub-group or sub-dataset."
                                 % relative_path)
            if attr_name in sub_dset_builder.attributes:
                raise ValueError("Attribute '%s' already exists. Cannot create attribute."
                                 % relative_path)
            cls.__create_builder_attribute(sub_dset_builder, attr_name, new_value, new_dtype)
        elif relative_path in builder.attributes:
            raise ValueError("Attribute '%s' already exists. Cannot create attribute." % relative_path)
        else:
            cls.__create_builder_attribute(builder, attr_name, new_value, new_dtype)

    @classmethod
    def __replace_dataset_data(cls, dset_builder, value, dtype):
        # TODO consider replacing slices of a dataset or attribute
        new_value = cls.convert_dtype(value, dtype, dset_builder['data'])
        dset_builder['data'] = new_value

    @classmethod
    def __replace_attribute(cls, builder, attr_name, value, dtype):
        new_value = cls.convert_dtype(value, dtype, builder.attributes[attr_name])
        builder.attributes[attr_name] = new_value

    @classmethod
    def __delete_attribute(cls, builder, attr_name):
        builder.remove_attribute(attr_name)

    @classmethod
    def __delete_builder(cls, parent_builder, child_builder):
        parent_builder.remove_child(child_builder)

    @classmethod
    def __change_dtype_dataset_data(cls, dset_builder, dtype):
        new_value = cls.convert_dtype(dset_builder['data'], dtype, dset_builder['data'])
        dset_builder['data'] = new_value

    @classmethod
    def __change_dtype_attribute(cls, builder, attr_name, dtype):
        new_value = cls.convert_dtype(builder.attributes[attr_name], dtype, builder.attributes[attr_name])
        builder.attributes[attr_name] = new_value

    @classmethod
    def __create_builder_attribute(cls, builder, attr_name, value, dtype):
        new_value = cls.convert_dtype(value, dtype, None)
        builder.attributes[attr_name] = new_value

    @classmethod
    def __get_object_id_map(cls, builder):
        stack = [builder]
        ret = dict()
        while len(stack):
            b = stack.pop()
            if 'object_id' in b.attributes:
                ret[b.attributes['object_id']] = b
            if isinstance(b, GroupBuilder):
                for g in b.groups.values():
                    stack.append(g)
                for d in b.datasets.values():
                    stack.append(d)
        return ret


class SidecarValidationError(Exception):
    """Error raised when a sidecar file fails validation with the JSON schema."""
    pass
