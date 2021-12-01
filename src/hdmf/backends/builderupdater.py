import json
import jsonschema
from pathlib import Path

from ..build import GroupBuilder
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

            versions = sidecar_dict['versions']
            builder_map = cls.__get_object_id_map(f_builder)
            for version_dict in versions:
                for change_dict in version_dict.get('changes'):
                    object_id = change_dict['object_id']
                    relative_path = change_dict.get('relative_path')
                    new_value = change_dict['value']

                    builder = builder_map[object_id]
                    # TODO handle paths to links
                    # TODO handle object references
                    if relative_path in builder.attributes:
                        # TODO handle different dtypes including compound dtypes
                        builder.attributes[relative_path] = new_value
                    elif isinstance(builder, GroupBuilder):  # GroupBuilder has object_id
                        sub_dset_builder, attr_name = builder.get_subbuilder(relative_path)
                        if sub_dset_builder is None:
                            raise ValueError("Relative path '%s' not recognized as a dataset or attribute"
                                             % relative_path)
                        if attr_name is None:  # update data in sub-DatasetBuilder
                            cls.__update_dataset_builder(sub_dset_builder, new_value)
                        else:  # update attribute
                            sub_dset_builder.attributes[attr_name] = new_value

                    else:  # DatasetBuilder has object_id
                        if not relative_path:  # update data
                            cls.__update_dataset_builder(builder, new_value)
                        else:
                            raise ValueError("Relative path '%s' not recognized as None or attribute" % relative_path)

        return f_builder

    @classmethod
    def __update_dataset_builder(cls, dset_builder, value):
        # TODO handle different dtypes including compound dtypes
        # TODO consider replacing slices of a dataset or attribute
        dset_builder['data'] = value

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
