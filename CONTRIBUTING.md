# Welcome to the Quinn contributing guide

## Issues

### Create a new issue

If you spot a problem with the docs, search if an issue already. If a related issue doesn't exist, you can open a [new issue](https://github.com/MrPowers/quinn/issues/new).

### Solve an issue

Scan through our [existing issues](https://github.com/MrPowers/quinn/issues) to find one that interests you. If you find an issue to work on, make sure that no one else is already working on it, so you can get assigned. After that, you are welcome to open a PR with a fix.

### Good first issue

You can find a list of [good first issues](https://github.com/MrPowers/quinn/issues?q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22) which can help you better understand code base of the project.

### Auto-assigning issues

We have a workflow that automatically assigns issues to users who comment 'take' on an issue. This is configured in the `.github/workflows/assign-on-comment.yml` file.

## Contributing

### Fork the repository

To start contributing you should fork this repository and only after that clone your fork. If you accidentally forked this repository you can fix it any time by this command:

```shell
# for user-login
git remote set-url origin https://github.com/your-github-name/quinn.git
# for private keys way
git remote set-url origin git@github.com:your-github-name/quinn.git
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

### Pre-commit installation and execution

We use pre-commit hooks to ensure code quality. The configuration for pre-commit hooks is in the `.pre-commit-config.yaml` file. To install pre-commit, run:
```shell
pip install pre-commit
pre-commit install
```
To run pre-commit hooks manually, use:
```shell
pre-commit run --all-files
```

### Running Tests

This project uses `pytest` and `chispa` for running spark tests. Please run all the tests before creating a pull request. In the case when you are working on new functionality you should also add new tests.
You can run test as following:
```shell
make test
```

### GitHub Actions local setup using 'act'

You can run GitHub Actions locally using the `act` tool. The configuration for GitHub Actions is in the `.github/workflows/ci.yml` file. To install `act`, follow the instructions [here](https://github.com/nektos/act#installation). To run a specific job, use:
```shell
act -j <job-name>
```
For example, to run the `test` job, use:
```shell
act -j test
```
If you need help with `act`, use:
```shell
act --help
```
For MacBooks with M1 processors, you might have to add the `--container-architecture` tag:
```shell
act -j <job-name> --container-architecture linux/arm64
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

We are using `isort` and `ruff` as linters. You can find instructions on how to set up and use these tools here:

1. [isort](https://pycqa.github.io/isort/)
2. [ruff](https://github.com/charliermarsh/ruff)

### Adding ruff to IDEs

#### VSCode

1. Install the `Ruff` extension by Astral Software from the VSCode marketplace (Extension ID: *charliermarsh.ruff*).
2. Open the command palette (Ctrl+Shift+P) and select `Preferences: Open Settings (JSON)`.
3. Add the following configuration to your settings.json file:

```json
{
    "python.linting.ruffEnabled": true,
    "python.linting.enabled": true,
    "python.formatting.provider": "none",
    "editor.formatOnSave": true
}
```
The above settings will enable linting with Ruff, and format your code with Ruff on save.

#### PyCharm

To set up `Ruff` in PyCharm using `poetry`, follow these steps:

1. **Find the path to your `poetry` executable:**
   - Open a terminal.
   - For macOS/Linux, use the command `which poetry`.
   - For Windows, use the command `where poetry`.
   - Note down the path returned by the command.

2. **Open the `Preferences` window** (Cmd+, on macOS).
3. **Navigate to `Tools` > `External Tools`.**
4. **Click the `+` icon** to add a new external tool.
5. **Fill in the following details:**
   - **Name:** `Ruff`
   - **Program:** Enter the path to your `poetry` executable that you noted earlier.
   - **Arguments:** `run ruff check --fix $FilePathRelativeToProjectRoot$`
   - **Working directory:** `$ProjectFileDir$`
6. **Click `OK`** to save the configuration.
7. **To run Ruff,** right-click on a file or directory in the project view, select `External Tools`, and then select `Ruff`.

### Pull Request

When you're finished with the changes, create a pull request, also known as a PR.
- Don't forget to link PR to the issue if you are solving one.
- As you update your PR and apply changes, mark each conversation as resolved.
- If you run into any merge issues, checkout this [git tutorial](https://github.com/skills/resolve-merge-conflicts) to help you resolve merge conflicts and other issues.
