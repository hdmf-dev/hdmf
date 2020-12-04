# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

import versioneer

with open('README.rst', 'r') as fp:
    readme = fp.read()

pkgs = find_packages('src', exclude=['data'])
print('found these packages:', pkgs)

schema_dir = 'common/hdmf-common-schema/common'

with open('requirements-min.txt', 'r') as fp:
    # replace == with >= and remove trailing comments and spaces
    reqs = [x.replace('==', '>=').split('#')[0].strip() for x in fp]
    reqs = [x for x in reqs if x]  # remove empty strings

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
    'classifiers': [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
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
