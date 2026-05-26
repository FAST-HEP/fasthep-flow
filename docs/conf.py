from __future__ import annotations

project = "FAST-HEP Flow"
author = "FAST-HEP contributors"

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
]

source_suffix = {
    ".rst": "restructuredtext",
    ".md": "markdown",
}

html_theme = "pydata_sphinx_theme"

html_theme_options = {
    "github_url": "https://github.com/FAST-HEP/fasthep-flow",
    "logo": {
        "text": "FAST-HEP Flow",
    },
    "navbar_align": "left",
}

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "substitution",
]

mermaid_params = ['--theme', 'forest', '--width', '600', '--backgroundColor', 'transparent']
