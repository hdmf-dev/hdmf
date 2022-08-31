# -*- coding: utf-8 -*
import sys

from setuptools import setup, find_packages

# Some Python installations don't add the current directory to path.
if '' not in sys.path:
    sys.path.insert(0, '')

import versioneer

with open('README.rst', 'r') as fp:
    readme = fp.read()

pkgs = find_packages('src', exclude=['data'])
print('found these packages:', pkgs)

schema_dir = 'common/hdmf-common-schema/common'

reqs = [
    'h5py>=2.10,<4',
    'jsonschema>=2.6.0,<5',
    'numpy>=1.16,<1.24',
    'pandas>=1.0.5,<2',
    'ruamel.yaml>=0.16,<1',
    'scipy>=1.1,<2',
    'setuptools',
]

print(reqs)

setup_args = {
    'name': 'hdmf',
    'version': versioneer.get_version(),
    'cmdclass': versioneer.get_cmdclass(),
    'description': 'A package for standardizing hierarchical object data',
    'long_description': readme,
    'long_description_content_type': 'text/x-rst; charset=UTF-8',
    'author': 'Andrew Tritt',
    'author_email': 'ajtritt@lbl.gov',
    'url': 'https://github.com/hdmf-dev/hdmf',
    'license': "BSD",
    'install_requires': reqs,
    'packages': pkgs,
    'package_dir': {'': 'src'},
    'package_data': {'hdmf': ["%s/*.yaml" % schema_dir, "%s/*.json" % schema_dir]},
    'python_requires': '>=3.7',
    'classifiers': [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "License :: OSI Approved :: BSD License",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: MacOS",
        "Operating System :: Unix",
        "Topic :: Scientific/Engineering :: Medical Science Apps."
    ],
    'keywords': 'python '
                'HDF '
                'HDF5 '
                'cross-platform '
                'open-data '
                'data-format '
                'open-source '
                'open-science '
                'reproducible-research ',
    'zip_safe': False,
    'entry_points': {
        'console_scripts': ['validate_hdmf_spec=hdmf.testing.validate_spec:main'],
    }
}

if __name__ == '__main__':
    setup(**setup_args)
