#!/usr/bin/env python
__license__ = """
Copyright 2015 Parse.ly, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import re
import sys

from setuptools import setup, find_packages

# Get version without importing, which avoids dependency issues


def get_version():
    with open('pymysqlslave/__init__.py') as version_file:
        return re.search(r"""__version__\s+=\s+(['"])(?P<version>.+?)\1""",
                         version_file.read()).group('version')

install_requires = [
    'sqlalchemy',
]

# tests_require = ['mock', 'nose', 'unittest2', 'python-snappy']
dependency_links = []

setup(
    name='pymysqlslave',
    version=get_version(),
    author='chuangwang',
    author_email='nashuiliang@gmail.com',
    url='https://github.com/nashuiliang/pymysqlslave',
    description='Simple Master Slave Load Balancing',
    keywords='mysqldb slave master balancing sqlalchemy',
    license='Apache License 2.0',
    packages=find_packages(),
    install_requires=install_requires,
    dependency_links=dependency_links,
    zip_safe=False,
    include_package_data=True,
)
