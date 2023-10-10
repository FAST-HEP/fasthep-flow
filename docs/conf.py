from __future__ import annotations

import importlib.metadata

project = "FAST-HEP flow"
copyright = "2023, FAST-HEP"
author = "FAST-HEP"
version = release = importlib.metadata.version("fasthep_flow")

extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.mathjax",
    "sphinx.ext.napoleon",
    "sphinx_autodoc_typehints",
    "sphinx_copybutton",
    "sphinxcontrib.mermaid",
]

source_suffix = [".rst", ".md"]
exclude_patterns = [
    "_build",
    "**.ipynb_checkpoints",
    "Thumbs.db",
    ".DS_Store",
    ".env",
    ".venv",
]

html_theme = "furo"
html_title = "Introduction"
html_static_path = ["_static"]
html_theme_options = {
    "source_repository": "https://github.com/FAST-HEP/fasthep-flow",
    "source_branch": "main",
    "source_directory": "docs/",
    "light_logo": "fast-flow-black.png",
    "dark_logo": "fast-flow-white.png",
    "announcement": "<em>THIS IS WORK IN PROGRESS</em>!",
}
html_css_files = ["css/custom.css"]

myst_enable_extensions = [
    "colon_fence",
]

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
}

nitpick_ignore = [
    ("py:class", "_io.StringIO"),
    ("py:class", "_io.BytesIO"),
]

always_document_param_types = True
