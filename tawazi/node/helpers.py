"""Helpers for node subpackage."""

from typing import Any, Callable, Optional, Union

from tawazi._helpers import ordinal
from tawazi.consts import USE_SEP_END, USE_SEP_START, Identifier


def _validate_tuple(func: Callable[..., Any], unpack_to: int) -> Optional[bool]:
    """Validate tuple typing when upack_to is set.

    Args:
        func: the function to validate
        unpack_to: the number of elements in the unpacked results.

    Returns:
        Optional[bool]: True if the validation is successful, None if validation can not be conclusive.
    """
    pass


def make_suffix(name_or_order: Union[int, str]) -> str:
    """Create the suffix of the id of ExecNode.

    Args:
        name_or_order (int | str): The name of the argument or its order of usage in the invocation of the function
    """
    pass


def _lazy_xn_id(base_id: Identifier, count_usages: int) -> Identifier:
    pass
