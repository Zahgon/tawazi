"""Helpers for node subpackage that use both ExecNode and UsageExecNode."""

from collections.abc import Iterable, Iterator
from typing import Any, Callable, Optional, Union

from . import node
from .node import ReturnExecNode
from .uxn import UsageExecNode

ReturnUXNsType = Union[
    None, UsageExecNode, tuple[UsageExecNode, ...], list[UsageExecNode], dict[str, UsageExecNode]
]


def _wrap_in_iterator_helper(
    func: Callable[..., Any], r_val: Iterable[Any]
) -> Iterator[UsageExecNode]:
    pass


def _wrap_in_list(func: Callable[..., Any], r_val: Any) -> Optional[list[UsageExecNode]]:
    pass


def _wrap_in_tuple(func: Callable[..., Any], r_val: Any) -> Optional[tuple[UsageExecNode, ...]]:
    pass


def _wrap_in_dict(func: Callable[..., Any], r_val: Any) -> Optional[dict[str, UsageExecNode]]:
    pass


def _wrap_in_uxn(func: Callable[..., Any], r_val: Any) -> UsageExecNode:
    pass


def wrap_in_uxns(func: Callable[..., Any], r_val: Any) -> ReturnUXNsType:
    """Get the IDs of the returned UsageExecNodes.

    Args:
        func (ReturnXNsType): function of the DAG description
        r_val (Any): Returned value from DAG describing function
        results (Dict[Identifier, Any): The results of the DAG's description containg the results (containing the constants in this context)

    Raises:
        TawaziTypeError: _description_
        TawaziTypeError: _description_

    Returns:
        ReturnIDsType: Corresponding IDs of the returned UsageExecNodes
    """
    pass
