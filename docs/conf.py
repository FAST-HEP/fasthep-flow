from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "_ext"))

project = "FAST-HEP Flow"
author = "FAST-HEP contributors"

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx_design",
    "sphinxcontrib.mermaid",
    "sphinx.ext.mathjax",
    "mermaid_styles",
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

mermaid_params = [
    '--theme',
    'forest',
    # '--width',
    # '600',
    '--backgroundColor',
    'transparent']
mermaid_height = "auto"

# conf.py
exclude_patterns = [
    "_build",
    "archive/**",
]
mermaid_init_config = {
    "startOnLoad": True,
    "theme": "base",
    "themeVariables": {
        "background": "#ffffff",
        "primaryTextColor": "#111111",
        "lineColor": "#5f6368",
        "fontSize": "18px",
    },
    "flowchart": {
        "htmlLabels": True,
        "nodeSpacing": 40,
        "rankSpacing": 50,
        "curve": "basis",
    },
}
