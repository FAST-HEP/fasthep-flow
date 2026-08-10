from __future__ import annotations

from sphinx.application import Sphinx


MERMAID_CLASSES = """
classDef input fill:#f7f7f7,stroke:#5f6368,stroke-width:2px,color:#111111;
classDef flow fill:#e8f0fe,stroke:#3c6fbd,stroke-width:2px,color:#111111;
classDef source fill:#e7f5ff,stroke:#1c7ed6,stroke-width:2px,color:#111111;
classDef transform fill:#fff9db,stroke:#f08c00,stroke-width:2px,color:#111111;
classDef observer fill:#f3f0ff,stroke:#7048e8,stroke-width:2px,color:#111111;
classDef sink fill:#ebfbee,stroke:#2f9e44,stroke-width:2px,color:#111111;
classDef capability fill:#f7f7f7,stroke:#5f6368,stroke-width:2px,color:#111111;
classDef plan fill:#d9ead3,stroke:#4f7d45,stroke-width:2px,color:#111111;
classDef runtime fill:#fce8d5,stroke:#b56b22,stroke-width:2px,color:#111111;
classDef artifact fill:#eadcf8,stroke:#7950a3,stroke-width:2px,color:#111111;
"""


def inject_mermaid_classes(
    app: Sphinx,
    doctree,
    docname: str,
) -> None:
    from sphinxcontrib.mermaid import mermaid

    for node in doctree.findall(mermaid):
        code = node["code"]
        node["code"] = f"{code}\n{MERMAID_CLASSES}"


def setup(app: Sphinx) -> dict[str, object]:
    app.connect("doctree-resolved", inject_mermaid_classes)

    return {
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
