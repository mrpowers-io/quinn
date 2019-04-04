from setuptools import setup, find_packages

install_requires = ['pytest']

tests_require = ['pytest']

setup(
    name='quinn',
    version='0.3.0',
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
    python_requires='>=2.7',
    extras_require={
        'test': tests_require,
        'all': install_requires + tests_require,
        'docs': ['sphinx'] + tests_require,
        'lint': []
    },
    classifiers=[
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
        'Framework :: Spark :: 2.4.0'
    ],
    dependency_links=[],
    include_package_data=False,
    keywords=['apachespark', 'spark', 'pyspark'],
)
