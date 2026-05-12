from __future__ import annotations

from hepflow.model.issues import FlowIssue, IssueLevel


def _issue_level_name(level: IssueLevel) -> str:
    return level.name.upper()


def _issue_level_rank(level: IssueLevel) -> int:
    return int(level)


def _format_meta(meta: dict, *, max_items: int = 20) -> str:
    if not meta:
        return ""
    items = list(meta.items())
    if len(items) > max_items:
        shown = items[:max_items]
        shown.append(("…", f"+{len(items) - max_items} more"))
        items = shown
    return ", ".join(f"{k}={v!r}" for k, v in items)


def format_validation_messages(
    issues: list[FlowIssue],
    *,
    min_level: IssueLevel = IssueLevel.INFO,
    max_meta_items: int = 20,
) -> str:
    """
    Format a flat list of issues.
    Shows issues with level <= min_level (ERROR=0 is strongest).
    """
    selected = [
        i for i in issues if _issue_level_rank(i.level) <= _issue_level_rank(min_level)
    ]
    selected.sort(key=lambda i: (_issue_level_rank(i.level), i.code, i.message))

    lines: list[str] = []
    for issue in selected:
        lines.append(
            f"[{_issue_level_name(issue.level)}] {issue.code}: {issue.message}"
        )
        meta_str = _format_meta(issue.meta or {}, max_items=max_meta_items)
        if meta_str:
            lines.append(f"  meta: {meta_str}")
    return "\n".join(lines)


def format_grouped_issues(
    grouped: dict[str, list[FlowIssue]],
    *,
    min_level: IssueLevel = IssueLevel.INFO,
    max_meta_items: int = 20,
) -> str:
    """
    Format grouped issues, preserving stage/module grouping.
    """
    groups_to_show: dict[str, list[FlowIssue]] = {}
    for group_name, issues in grouped.items():
        selected = [
            i
            for i in issues
            if _issue_level_rank(i.level) <= _issue_level_rank(min_level)
        ]
        if selected:
            groups_to_show[group_name] = sorted(
                selected,
                key=lambda i: (_issue_level_rank(i.level), i.code, i.message),
            )

    if not groups_to_show:
        return ""

    lines: list[str] = []
    for group_name in sorted(groups_to_show.keys()):
        lines.append(f"[{group_name}]")
        for issue in groups_to_show[group_name]:
            lines.append(
                f"  [{_issue_level_name(issue.level)}] {issue.code}: {issue.message}"
            )
            meta_str = _format_meta(issue.meta or {}, max_items=max_meta_items)
            if meta_str:
                lines.append(f"    meta: {meta_str}")
        lines.append("")

    # drop trailing blank line
    while lines and lines[-1] == "":
        lines.pop()

    return "\n".join(lines)


def count_grouped_issues(
    grouped: dict[str, list[FlowIssue]],
    *,
    max_level: IssueLevel = IssueLevel.ERROR,
) -> int:
    return sum(
        1
        for issues in grouped.values()
        for issue in issues
        if _issue_level_rank(issue.level) <= _issue_level_rank(max_level)
    )


def filter_grouped_issues(
    grouped: dict[str, list[FlowIssue]],
    *,
    max_level: IssueLevel = IssueLevel.ERROR,
) -> dict[str, list[FlowIssue]]:
    out: dict[str, list[FlowIssue]] = {}
    for group_name, issues in grouped.items():
        selected = [
            i
            for i in issues
            if _issue_level_rank(i.level) <= _issue_level_rank(max_level)
        ]
        if selected:
            out[group_name] = selected
    return out


def raise_on_grouped_issues(
    grouped: dict[str, list[FlowIssue]],
    *,
    context: str = "compile",
    error_level: IssueLevel = IssueLevel.ERROR,
) -> None:
    failing = filter_grouped_issues(grouped, max_level=error_level)
    if not failing:
        return

    nerr = count_grouped_issues(failing, max_level=error_level)
    body = format_grouped_issues(failing, min_level=error_level)
    raise ValueError(f"{context} failed with {nerr} error(s):\n{body}")
