"""This will be moved to either fasthep-core or fasthep-checksum
Most of this is a duplicate of https://github.com/BristolComputing/xrdsum"""

from __future__ import annotations

import struct
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import crc32c


class Checksum(ABC):
    """Base protocol for checksum implementations."""

    name: str = "Unknown"
    value: int = 0
    bytes_read: int = 0
    number_of_buffers_read: int = 0

    @abstractmethod
    def hexdigest(self) -> str:
        """Converts the checksum to a hex string"""
        raise NotImplementedError()

    @abstractmethod
    def as_int(self) -> int:
        """Converts the checksum to an integer"""
        raise NotImplementedError

    @abstractmethod
    def calculate(self, buffer: bytes) -> None:
        """Calculates the checksum"""
        raise NotImplementedError()


class Adler32(Checksum):
    """Adler32 checksum
    from https://github.com/snafus/cephsum/blob/master/cephsum/adler32.py"""

    name: str = "adler32"

    def hexdigest(self) -> str:
        """Converts an integer to a hex string"""
        # return hex(value)[2:]
        return "".join([f"{x:02x}" for x in struct.pack(">I", self.value)]).lower()

    def as_int(self) -> int:
        return self.value

    def calculate(self, buffer: bytes) -> None:
        self.value = zlib.adler32(buffer, self.value)
        self.bytes_read += len(buffer)
        self.number_of_buffers_read += 1


class CRC32C(Checksum):
    """CRC32C checksum implementation"""

    name: str = "crc32c"
    hash: crc32c.CRC32CHash

    def __init__(self) -> None:
        self.hash = crc32c.CRC32CHash()

    def as_int(self) -> int:
        self.value = int.from_bytes(self.hash.digest(), "big")
        return self.value

    def hexdigest(self) -> str:
        return self.hash.hexdigest()

    def calculate(self, buffer: bytes) -> None:
        self.hash.update(buffer)
        self.bytes_read += len(buffer)
        self.number_of_buffers_read += 1


AVAILABLE_CHECKSUM_TYPES: dict[str, type[Checksum]] = {
    "adler32": Adler32,
    "crc32c": CRC32C,
}


@dataclass
class ChecksumResult:
    """container for checksum results"""

    value: int
    hexdigest: str
    bytes_read: int
    number_of_buffers_read: int
    checksum_type: str


def calculate_checksums(
    file_path: Path, checksum_types: list[str], buffer_size: int = 1024 * 1024
) -> dict[str, ChecksumResult]:
    """Calculate checksums for a file"""
    checksums: dict[str, Checksum] = {}
    for checksum_type in checksum_types:
        checksums[checksum_type] = AVAILABLE_CHECKSUM_TYPES[checksum_type]()
    with file_path.open("rb") as f:
        block_read = partial(f.read, buffer_size)
        filebuffer = iter(block_read, b"")
        for buffer in filebuffer:
            for checksum in checksums.values():
                checksum.calculate(buffer)

    return {
        checksum_type: ChecksumResult(
            checksum.as_int(),
            checksum.hexdigest(),
            checksum.bytes_read,
            checksum.number_of_buffers_read,
            checksum_type,
        )
        for checksum_type, checksum in checksums.items()
    }
