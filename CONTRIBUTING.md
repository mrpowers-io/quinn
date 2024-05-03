# Welcome to the Quinn contributing guide

## Issues

### Create a new issue

If you spot a problem with the docs, search if an issue already. If a related issue doesn't exist, you can open a [new issue](https://github.com/MrPowers/quinn/issues/new).

### Solve an issue

Scan through our [existing issues](https://github.com/MrPowers/quinn/issues) to find one that interests you. If you find an issue to work on, make sure that no one else is already working on it, so you can get assigned. After that, you are welcome to open a PR with a fix.

### Good first issue

You can find a list of [good first issues](https://github.com/MrPowers/quinn/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) which can help you better understand code base of the project.

## Contributing

### Fork the repository

To start contributing you should fork this repository and only after that clone your fork. If you accidentally forked this repository you can fix it any time by this command:

```shell
# for user-login
git remote --set-url origin https://github.com/your-github-name/quinn.git
# for private keys way
git remote --set-url origin git@github.com:your-github-name/quinn.git
```

### Install the project

#### Installing poetry

After cloning the project you should install all the dependencies. We are using `poetry` as a build tool. You can install `poetry` by following [this instruction](https://python-poetry.org/docs/#installation).

#### Installing dependencies

You can create a virtualenv with `poetry`. The recommended version of Python is `3.9`:
```shell
poetry env use python3.9
```

After that you should install all the dependencies including development:
```shell
make install_deps
```

#### Setup Java

To run spark tests you need to have properly configured Java. Apache Spark currently supports mainly only Java 8 (1.8). You can find an instruction on how to set up Java [here](https://www.java.com/en/download/help/download_options.html). When you are running spark tests you should have `JAVA_HOME` variable in your environment which points to the  installation of Java 8.


### Running Tests

This project uses `pytest` and `chispa` for running spark tests. Please run all the tests before creating a pull request. In the case when you are working on new functionality you should also add new tests.
You can run test as following:
```shell
make test
```
### Code style

This project follows the [PySpark style guide](https://github.com/MrPowers/spark-style-guide/blob/main/PYSPARK_STYLE_GUIDE.md). All public functions and methods should be documented in `README.md` and also should have docstrings in `sphinx format`:

```python
"""[Summary]

:param [ParamName]: [ParamDescription], defaults to [DefaultParamVal]
:type [ParamName]: [ParamType](, optional)
...
:raises [ErrorType]: [ErrorDescription]
...
:return: [ReturnDescription]
:rtype: [ReturnType]
"""
```

We are using `isort`, `black` and `ruff` as linters. You can find instructions on how to set up and use these tools here:

1. [isort](https://pycqa.github.io/isort/)
2. [black](https://black.readthedocs.io/en/stable/)
3. [ruff](https://github.com/charliermarsh/ruff)

### Pull Request

When you're finished with the changes, create a pull request, also known as a PR.
- Don't forget to link PR to the issue if you are solving one.
- As you update your PR and apply changes, mark each conversation as resolved.
- If you run into any merge issues, checkout this [git tutorial](https://github.com/skills/resolve-merge-conflicts) to help you resolve merge conflicts and other issues.

## Maintainers and Reviewers

1. [MrPowers](https://github.com/MrPowers)
2. ...
