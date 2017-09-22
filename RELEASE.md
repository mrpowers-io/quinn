# Uploading the project to PyPi

Bump the version in `setup.py`

Make sure the release packages are installed with the Python version:

```
pip install wheel
pip install twine
```

Build the pure Python wheel.

```
python setup.py bdist_wheel
```

Upload the version to PyPi.

```
twine upload dist/quinn-0.1.0-py3-none-any.whl
```

Create a GitHub release.