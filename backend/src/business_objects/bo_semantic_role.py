"""BOSemanticRole enumeration, which represents the semantic roles of business objects for the frontend."""

from enum import StrEnum, auto


class BOSemanticRole(StrEnum):
    """Enumeration for the semantic roles of business objects for the frontend."""

    RAW = (
        auto()
    )  # Raw data to be displayed as is as purely technical information, e.g. for debugging purposes
    BONAME = (
        auto()
    )  # The name of the business object, to be displayed in the frontend as a human-readable identifier for the business object
