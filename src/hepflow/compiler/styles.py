# hepflow/compiler/styles.py
from __future__ import annotations
from typing import Any, Dict


def deep_merge(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(a)
    for k, v in b.items():
        if k in out and isinstance(out[k], dict) and isinstance(v, dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def collect_styles(doc: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    raw = doc.get("styles") or {}
    if not isinstance(raw, dict):
        raise ValueError("styles must be a mapping")
    out: Dict[str, Dict[str, Any]] = {}
    for name, spec in raw.items():
        if not isinstance(spec, dict):
            raise ValueError(f"styles.{name} must be a mapping")
        out[str(name)] = dict(spec)
    return out


def resolve_style(name: str, styles: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    return _resolve_style_inner(name, styles, stack=[])


def _resolve_style_inner(
    name: str, styles: Dict[str, Dict[str, Any]], stack: list[str]
) -> Dict[str, Any]:
    if name in stack:
        cyc = " -> ".join(stack + [name])
        raise ValueError(f"Style 'use' cycle detected: {cyc}")
    if name not in styles:
        raise KeyError(f"Unknown style '{name}'. Available: {sorted(styles.keys())}")

    spec = styles[name]
    base_name = spec.get("use")
    with_ = spec.get("with")

    base: Dict[str, Any] = {}
    if base_name is not None:
        if not isinstance(base_name, str) or not base_name.strip():
            raise ValueError(f"styles.{name}.use must be a non-empty string")
        base = _resolve_style_inner(base_name, styles, stack=stack + [name])

    overlay = {k: v for k, v in spec.items() if k not in ("use", "with")}
    merged = deep_merge(base, overlay)

    if with_ is not None:
        if not isinstance(with_, dict):
            raise ValueError(f"styles.{name}.with must be a mapping")
        merged = deep_merge(merged, with_)

    return merged


def resolve_style_ref(style: Any, styles: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """
    style can be:
      - str: style name
      - dict: either raw dict, or {use, with}
    """
    if isinstance(style, str):
        return resolve_style(style, styles)
    if isinstance(style, dict):
        if "use" in style:
            base_name = style.get("use")
            if not isinstance(base_name, str) or not base_name.strip():
                raise ValueError("render.style.use must be a non-empty string")
            base = resolve_style(base_name, styles)
            overlay = {k: v for k, v in style.items() if k not in ("use", "with")}
            merged = deep_merge(base, overlay)
            if "with" in style:
                if not isinstance(style["with"], dict):
                    raise ValueError("render.style.with must be a mapping")
                merged = deep_merge(merged, style["with"])
            return merged
        return dict(style)
    raise ValueError(
        f"render.style must be string or mapping, got {type(style).__name__}"
    )


def resolve_style_tree(obj: Any, style_defs: dict[str, dict[str, Any]]) -> Any:
    """
    Recursively resolve {use: ..., with: ...} blocks anywhere in a nested structure.
    Returns a fully expanded structure (no 'use' keys remain).
    """
    if isinstance(obj, list):
        return [resolve_style_tree(x, style_defs) for x in obj]

    if not isinstance(obj, dict):
        return obj

    # If this dict is a template reference, expand it first
    if "use" in obj:
        ref = obj["use"]
        base = resolve_style_ref(ref, style_defs)
        overrides = obj.get("with") or {}
        merged = deep_merge(base, overrides)
        # IMPORTANT: resolve again after merge (nested 'use' may appear)
        return resolve_style_tree(merged, style_defs)

    # Otherwise recurse into fields
    return {k: resolve_style_tree(v, style_defs) for k, v in obj.items()}


def expand_render_transforms(
    spec_dict: dict[str, Any],
    dataset_entries: dict[str, Any],
) -> dict[str, Any]:
    """
    Expand author-facing render transform shorthands into explicit forms.

    Currently:
      - group by: dataset_group
        -> group by: {group_name: [dataset names]}
    """
    spec_dict = dict(spec_dict)
    transforms = list(spec_dict.get("transforms") or [])
    if not transforms:
        return spec_dict

    expanded: list[dict[str, Any]] = []

    for t in transforms:
        if not isinstance(t, dict):
            expanded.append(t)
            continue

        kind = t.get("kind")
        by = t.get("by")

        if kind == "group" and by == "dataset_group":
            groups: dict[str, list[str]] = {}

            for ds_name, ds_cfg in dataset_entries.items():
                if isinstance(ds_cfg, dict):
                    group = str(ds_cfg.get("group") or ds_name)
                else:
                    # dataclass-ish fallback
                    group = str(getattr(ds_cfg, "group", None) or ds_name)

                groups.setdefault(group, []).append(str(ds_name))

            expanded.append(
                {
                    **t,
                    "by": groups,
                }
            )
            continue

        expanded.append(t)

    spec_dict["transforms"] = expanded
    return spec_dict


def resolve_effective_dataset_categories_for_render(
    spec,
    dataset_entries: dict[str, Any],
) -> set[str]:
    """
    Return the effective dataset/category names visible to the renderer
    after applying category-changing transforms.

    Current rules:
      - start from dataset names
      - scale does not change category names
      - group replaces category names with group names
    """
    current: set[str] = set(dataset_entries.keys())

    for t in spec.transforms or []:
        if t.kind == "group" and t.group is not None:
            by = t.group.by

            if isinstance(by, dict):
                current = set(str(k) for k in by.keys())
                continue

            if isinstance(by, str):
                if by == "dataset_group":
                    groups = set()
                    for ds_name, ds_cfg in dataset_entries.items():
                        if isinstance(ds_cfg, dict):
                            groups.add(str(ds_cfg.get("group") or ds_name))
                        else:
                            groups.add(str(getattr(ds_cfg, "group", None) or ds_name))
                    current = groups
                    continue

                raise ValueError(f"Unsupported group transform mode: {by!r}")

        elif t.kind == "scale":
            # scaling does not change category names
            continue

    return current
