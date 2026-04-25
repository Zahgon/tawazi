"""Module for helper functions."""

from typing import Any, Callable, NoReturn, TypeVar

import yaml

from tawazi.errors import TawaziArgumentError


def ordinal(numb: int) -> str:
    """Construct the string corresponding to the ordinal of a number.

    Args:
        numb (int): order

    Returns:
        str: "0th", "1st", "2nd", etc...

    >>> ordinal(0)
    '0th'
    >>> ordinal(1)
    '1st'
    >>> ordinal(2)
    '2nd'
    >>> ordinal(3)
    '3rd'
    >>> ordinal(4)
    '4th'
    >>> ordinal(10)
    '10th'
    >>> ordinal(11)
    '11th'
    >>> ordinal(21)
    '21st'
    >>> ordinal(22)
    '22nd'
    >>> ordinal(23)
    '23rd'
    >>> ordinal(24)
    '24th'
    >>> ordinal(113)
    '113th'
    """
    pass


def make_raise_arg_error(func_name: str, arg_name: str) -> Callable[[], NoReturn]:
    # declare a local function that will raise an error in the scheduler if
    # the user doesn't pass in This ArgExecNode as argument to the Attached LazyExecNode
    pass


# courtesy of https://gist.github.com/pypt/94d747fe5180851196eb?permalink_comment_id=3401011#gistcomment-3401011
class UniqueKeyLoader(yaml.SafeLoader):
    """Unique key safe loader for yaml."""

    def construct_mapping(self, node: Any, deep: bool = False) -> Any:
        """Construct mapping of the corresponding YAML."""
        pass


T = TypeVar("T")
V = TypeVar("V")


class StrictDict(dict[T, V]):
    """A Dict that raises an error if key already used.

    >>> d = StrictDict({1: 2, 2: 3})
    >>> d[3] = 4
    >>> d[3] = 5
    Traceback (most recent call last):
    ...
    KeyError: 'key: 3, is already occupied by 4'
    """

    def __setitem__(self, key: T, value: V) -> None:
        if key in self:
            raise KeyError(f"key: {key}, is already occupied by {self[key]}")
        super().__setitem__(key, value)

    def force_set(self, key: T, value: V) -> None:
        """Force Set a key to a value."""
        pass
