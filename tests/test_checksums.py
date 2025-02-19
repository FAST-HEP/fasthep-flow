from __future__ import annotations

from pathlib import Path

import pytest

from fasthep_flow import checksums


@pytest.fixture()
def test_bytes() -> list[bytes]:
    return [b"FAST-HEP", b" testing"]


@pytest.fixture()
def test_file(tmp_path: Path, test_bytes: list[bytes]) -> Path:
    file_path = tmp_path / "test.txt"
    with file_path.open("wb") as f:
        for test_text in test_bytes:
            f.write(test_text)
    return file_path


@pytest.mark.parametrize("checksum_type", ["adler32", "crc32c"])
def test_calculate_checksums(test_file, checksum_type, test_bytes):
    result = checksums.calculate_checksums(test_file, [checksum_type], buffer_size=4)
    assert result[checksum_type].bytes_read == 16
    assert result[checksum_type].number_of_buffers_read == 4
    assert result[checksum_type].checksum_type == checksum_type
    checksum: checksums.Checksum = checksums.AVAILABLE_CHECKSUM_TYPES[checksum_type]()
    [checksum.calculate(buffer) for buffer in test_bytes]
    assert result[checksum_type].value == checksum.as_int()
    assert result[checksum_type].hexdigest == checksum.hexdigest()
