# Installation Guide

`fasthep-flow` is designed for flexibility, allowing users to integrate it
either as an independent module or as a component within the fasthep package
ecosystem.

## Installation options

1. **Standalone Installation**:

   - **Recommended For**: Users who are interested in leveraging the
     capabilities of `fasthep-flow` independently, without the need for the
     additional utilities present in the `fasthep` package ecosystem.
   - **Instructions**: `pip install fasthep-flow`

2. **Installation within `fasthep` Ecosystem**:
   - **Recommended For**: Users aiming to harness the full potential of
     `fasthep-flow` in conjunction with the comprehensive suite of tools and
     packages available within the `fasthep` ecosystem.
   - **Instructions**: `pip install fasthep[full]`

## Developer Installation Guid for `fasthep-flow`

If you're a developer looking to make modifications or contribute to
`fasthep-flow`, you'll likely want to install it directly from the GitHub
repository. Below are the steps to manually install `fasthep-flow`.

### Prerequisites:

1. **Git**: Ensure you have Git installed on your machine. If not, you can
   download and install it from [Git's official website](https://git-scm.com/).

2. **Python**: `fasthep-flow` requires Python. Make sure you have a compatible
   version installed. At least version 3.11 is recommended for development. You
   can download and install from
   [Python's official website](https://www.python.org/downloads/).

3. **pip**: Ensure you have `pip` installed. It usually comes with Python
   installations, but if for some reason you don't have it, you can find
   instructions [here](https://pip.pypa.io/en/stable/installation/).

### Installation Steps:

1. **Clone the Repository**:

   ```bash
   git clone https://github.com/FAST-HEP/fasthep-flow.git
   ```

   Replace `FAST-HEP` with your GitHub username if working on a fork.

2. **Navigate to the Repository Folder**:

   ```bash
   cd fasthep-flow
   ```

3. **Install the Package in Editable Mode**:

   Installing in editable mode (with the `-e` flag) means changes to the source
   files will immediately affect the installed package.

   ```bash
   pip install -e .
   ```

4. **Verify the Installation**:

   You can check if the installation was successful by trying to import
   `fasthep-flow` in Python:

   ```python
   import fasthep_flow
   ```

   If you don't receive any errors, the installation was successful.

### Next Steps:

With `fasthep-flow` installed, you can now make changes, run tests, and
contribute back to the project. Ensure you read the contribution guidelines if
available.
