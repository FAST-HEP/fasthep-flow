"""Exact structural-path classifier: which section a plan difference belongs to.

Classification never looks at key names such as ``path`` or ``hash``; a difference is
placed by the longest registered key-path prefix, and anything unregistered is ``unknown``
(which counts as semantic).
"""

from __future__ import annotations

from hepflow.compiler.plan_diff.model import DiffSection, Key, Location

#: Bump by hand with every table edit; ``test_classifier_version_guard`` enforces it.
CLASSIFIER_VERSION = 1

_G, _D, _E, _R, _P = (
    DiffSection.GRAPH,
    DiffSection.DATA,
    DiffSection.EXECUTION,
    DiffSection.REPORTS,
    DiffSection.PROVENANCE,
)

_NODE_META_GRAPH = (
    "stage_id",
    "workflow_op",
    "workflow_kind",
    "output_layout",
    "applies_to",
    "join_name",
    "source_name",
    "stream_id",
    "inserted_by",
    "observed_node",
    "parent_kind",
    "parent_name",
    "style_ref",
    "input_node",
    "variation",
    "variation_of",
    "export_of",
    "collection_for",
)

_REGISTRY_SECTIONS: dict[str, DiffSection] = {
    "functions": _G,
    "constants": _G,
    "sources": _G,
    "transforms": _G,
    "sinks": _G,
    "observers": _G,
    "render": _G,
    "backends": _E,
    "hooks": _E,
    "progress_sinks": _E,
    "execution_modifiers": _E,
    "product_handlers": _E,
    "resource_kinds": _E,
    "report_templates": _R,
    "compile_hooks": _P,
}

_TABLE: tuple[tuple[tuple[str, ...], DiffSection], ...] = (
    (("context", "datasets"), _D),
    (("context", "dataset_names"), _D),
    (("partitions",), _D),
    (("context", "globals"), _G),
    (("context", "variation"), _G),
    (("data_flow",), _G),
    (("context", "workflow_path"), _P),
    (("context", "project_root"), _P),
    (("execution",), _E),
    (("execution_hooks",), _E),
    (("reports",), _R),
    (("provenance",), _P),
    *((("nodes", key), _G) for key in ("id", "graph_node_id", "role", "impl")),
    *((("nodes", key), _G) for key in ("inputs", "params", "outputs")),
    *((("nodes", key), _E) for key in ("input_scope", "output_scope")),
    *((("nodes", key), _E) for key in ("partitioning", "materialize")),
    (("nodes", "meta", "execution"), _E),
    (("nodes", "meta", "compile_hooks"), _P),
    *((("nodes", "meta", key), _G) for key in _NODE_META_GRAPH),
    *((("registry", name), section) for name, section in _REGISTRY_SECTIONS.items()),
)

_BY_PREFIX = dict(_TABLE)


def classifier_table() -> tuple[tuple[tuple[str, ...], DiffSection], ...]:
    """The exact table, for the version guard and the documentation."""
    return _TABLE


def classify(
    location: Location, *, whole_object_section: DiffSection | None = None
) -> DiffSection:
    """Return the section of a difference at ``location``.

    Only mapping keys take part; list indices and identity selectors are skipped.
    ``whole_object_section`` is used for an added or removed identified object.
    """
    if whole_object_section is not None:
        return whole_object_section
    keys = tuple(s.name for s in location if isinstance(s, Key))
    for size in range(len(keys), 0, -1):
        section = _BY_PREFIX.get(keys[:size])
        if section is not None:
            return section
    return DiffSection.UNKNOWN
