from __future__ import annotations

from typing import Any

StrListOrNone = str | list[str] | None


class Uproot5Import:
    input_file: str
    tree_names: list[str]
    branches: list[str]

    def __init__(
        self,
        input_file: str,
        tree_names: StrListOrNone,
        branches: StrListOrNone,
    ):
        self.input_file = input_file
        if isinstance(tree_names, str):
            tree_names = [tree_names]
        if isinstance(branches, str):
            branches = [branches]
        self.tree_names = tree_names or []
        self.branches = branches or []

    def __call__(self, **kwargs: Any) -> dict[str, Any]:
        import uproot

        if len(self.tree_names) > 1:
            msg = "Multiple trees not supported yet"
            raise NotImplementedError(msg)
        tree_name: str = self.tree_names[0]
        path = self.input_file + ":" + tree_name
        tree = uproot.open(path)
        return tree.arrays(library="ak")


    def __repr__(self) -> str:
        return f"Uproot5Import(input_file='{self.input_file}', tree_names={self.tree_names}, branches={self.branches})"
