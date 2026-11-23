from __future__ import annotations

from typing import Any

import pytest

from hepflow.compiler.partitioning import derive_dataset_partitions, resolve_chunk_size


@pytest.mark.parametrize(
    ("cli_chunk_size", "dataset", "execution", "expected"),
    [
        (11, {"chunk_size": 22}, {"partitioning": {"chunk_size": 33}}, 11),
        (None, {"chunk_size": 22}, {"partitioning": {"chunk_size": 33}}, 22),
        (None, {}, {"partitioning": {"chunk_size": 33}}, 33),
        (None, {}, {}, None),
    ],
)
def test_chunk_size_precedence(
    cli_chunk_size: int | None,
    dataset: dict[str, Any],
    execution: dict[str, Any],
    expected: int | None,
) -> None:
    assert resolve_chunk_size(cli_chunk_size, dataset, execution) == expected


@pytest.mark.parametrize("chunk_size", [True, 0, -1])
def test_cli_chunk_size_must_be_a_positive_integer(chunk_size: object) -> None:
    with pytest.raises(ValueError, match="chunk size must be a positive integer"):
        resolve_chunk_size(chunk_size, {}, {})  # type: ignore[arg-type]


def test_derivation_uses_each_files_count_and_legacy_part_name() -> None:
    partitions = derive_dataset_partitions(
        {
            "name": "sample",
            "files": ["a.root", "b.root"],
            "file_nevents": [10, 7],
        },
        source_name="events",
        chunk_size=5,
    )

    assert [
        (partition.part, partition.start, partition.stop) for partition in partitions
    ] == [
        ("0_0", 0, 5),
        ("0_1", 5, 10),
        ("1_0", 0, 5),
        ("1_1", 5, 7),
    ]


def test_derivation_uses_dataset_count_for_one_file() -> None:
    partitions = derive_dataset_partitions(
        {"name": "sample", "files": ["a.root"], "nevents": "12"},
        source_name="events",
        chunk_size=5,
    )

    assert [(partition.start, partition.stop) for partition in partitions] == [
        (0, 5),
        (5, 10),
        (10, 12),
    ]


def test_derivation_warns_when_requested_chunking_has_no_counts() -> None:
    with pytest.warns(UserWarning, match="whole-file partition"):
        partitions = derive_dataset_partitions(
            {"name": "sample", "files": ["a.root"]},
            source_name="events",
            chunk_size=5,
        )

    assert [(partition.start, partition.stop) for partition in partitions] == [
        (None, None)
    ]


@pytest.mark.parametrize(
    ("nevents", "chunk_size"),
    [
        (469384, 93877),
        (77729, 15546),
        (142, 29),
        (5684, 1137),
        (36941, 7389),
        (109737, 21948),
        (4580, 916),
        (3367, 674),
        (2421, 485),
    ],
)
def test_zmumu_chunk_sizes_make_five_partitions(
    nevents: int, chunk_size: int
) -> None:
    partitions = derive_dataset_partitions(
        {"name": "sample", "files": ["sample.root"], "nevents": str(nevents)},
        source_name="events",
        chunk_size=chunk_size,
    )

    assert len(partitions) == 5


def test_sources_of_one_dataset_share_one_numbering() -> None:
    dataset = {"name": "sample", "files": ["a.root", "b.root"], "file_nevents": [4, 4]}

    first = derive_dataset_partitions(
        dataset, source_name="s1", chunk_size=None, local_numbers=True
    )
    second = derive_dataset_partitions(
        dataset,
        source_name="s2",
        chunk_size=None,
        local_numbers=True,
        first_number=len(first) + 1,
    )

    assert [p.part for p in [*first, *second]] == ["1", "2", "3", "4"]
    assert len({p.id for p in [*first, *second]}) == 4
