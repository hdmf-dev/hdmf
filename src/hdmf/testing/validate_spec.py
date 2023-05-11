import json
import os
from argparse import ArgumentParser
from glob import glob

import jsonschema
import ruamel.yaml as yaml


def validate_spec(fpath_spec, fpath_schema):
    """
    Validate a yaml specification file against the json schema file that
    defines the specification language. Can be used to validate changes
    to the NWB and HDMF core schemas, as well as any extensions to either.

    :param fpath_spec: path-like
    :param fpath_schema: path-like
    """

    schemaAbs = 'file://' + os.path.abspath(fpath_schema)

    f_schema = open(fpath_schema, 'r')
    schema = json.load(f_schema)

    class FixResolver(jsonschema.RefResolver):
        def __init__(self):
            jsonschema.RefResolver.__init__(self,
                                            base_uri=schemaAbs,
                                            referrer=None)
            self.store[schemaAbs] = schema

    new_resolver = FixResolver()

    f_nwb = open(fpath_spec, 'r')
    instance = yaml.safe_load(f_nwb)

    jsonschema.validate(instance, schema, resolver=new_resolver)


def main():
    parser = ArgumentParser(description="Validate an HDMF/NWB specification")
    parser.add_argument("paths", type=str, nargs='+', help="yaml file paths")
    parser.add_argument("-m", "--metaschema", type=str,
                        help=".json.schema file used to validate yaml files")
    args = parser.parse_args()

    for path in args.paths:
        if os.path.isfile(path):
            validate_spec(path, args.metaschema)
        elif os.path.isdir(path):
            for ipath in glob(os.path.join(path, '*.yaml')):
                validate_spec(ipath, args.metaschema)
        else:
            raise ValueError('path must be a valid file or directory')


if __name__ == "__main__":
    main()
