from setuptools import setup, find_packages

import sys
if sys.version_info < (3, 0):
    raise RuntimeError('This version requires Python 3.0+')

install_requires = ['pytest']

tests_require = ['pytest']

setup(
    name='quinn',
    version='0.2.0',
    author='Matthew Powers',
    author_email='matthewkevinpowers@gmail.com',
    url='https://github.com/MrPowers/quinn',
    description='Pyspark helper methods to maximize developer efficiency.',
    long_description='SparkSession extensions, DataFrame validation, Column extensions, SQL functions, and DataFrame transformations',
    license='APACHE',
    packages=find_packages(),
    install_requires=install_requires,
    tests_require=tests_require,
    setup_requires=[],
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': []
    },
    dependency_links=[],
    include_package_data=False,
    keywords=['apachespark', 'spark', 'pyspark'],
)
