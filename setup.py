from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='fes',

    version='1.0.0.dev1',

    description='Future Event Service',
    long_description=long_description,

    url='https://github.com/erprice/fes',

    author='Evan Price',
    author_email='erprice3@gmail.com',

    license='Apache',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2.7'
    ],

    keywords='hbase redis rest json',

    packages=find_packages(exclude=['contrib', 'docs', 'tests*']),

    install_requires=['flask', 'hashlib', 'redis', 'ordereddict', 'requests', 'nose'],

    package_data={
        'hbase_schema': ['schema'],
        'readme': ['README.md'],
        'license': ['LICENSE.txt'],
        'todo': ['TODO.txt'],
        'gitignore': ['.gitignore']
    },
)