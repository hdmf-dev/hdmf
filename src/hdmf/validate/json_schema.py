import os
import yaml
import json

import jsonschema


def validate_spec(fpath_spec, fpath_schema):

    schema_abs = 'file://' + os.path.abspath(fpath_schema)

    f_schema = open(fpath_schema, 'r')
    schema = json.load(f_schema)

    class FixResolver(jsonschema.RefResolver):
        def __init__(self):
            jsonschema.RefResolver.__init__(self,
                                            base_uri=schema_abs,
                                            referrer=None)
            self.store[schema_abs] = schema

    new_resolver = FixResolver()

    f_nwb = open(fpath_spec, 'r')
    instance = yaml.load(f_nwb, Loader=yaml.FullLoader)

    jsonschema.validate(instance, schema, resolver=new_resolver)


if __name__ == "__main__":
    cur_file = os.path.dirname(os.path.realpath(__file__))
    fpath_hdmf_common = os.path.join(os.path.split(cur_file)[0], 'common', 'hdmf-common-schema')
    fpath_namespace_schema = os.path.join(fpath_hdmf_common, 'hdmf-language.namespace.schema.json')
    fpath_namespace = os.path.join(fpath_hdmf_common, 'common', 'namespace.yaml')

    validate_spec(fpath_namespace, fpath_namespace_schema)
