import inspect
import warnings
from typing import Any, Callable, Union

from tawazi._helpers import StrictDict
from tawazi.consts import RVDAG, P
from tawazi.node import ArgExecNode, ExecNode, ReturnUXNsType, UsageExecNode, node, wrap_in_uxns
from tawazi.node.node import make_axn_id

from .dag import DAG, AsyncDAG


def get_args_and_default_args(func: Callable[..., Any]) -> tuple[list[str], dict[str, Any]]:
    """Retrieves the arguments names and the default arguments of a function.

    Args:
        func: the target function

    Returns:
        A Tuple containing a List of argument names of non default arguments,
         and the mapping between the arguments and their default value for default arguments

    >>> def f(a1, a2, *args, d1=123, d2=None): pass
    >>> get_args_and_default_args(f)
    (['a1', 'a2', 'args'], {'d1': 123, 'd2': None})
    """
    pass


def make_dag(
    _func: Callable[P, RVDAG], max_concurrency: int, is_async: bool
) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]:
    """Make a DAG or AsyncDAG from the function that describes the DAG."""
    pass


def wrap_make_dag(
    _func: Callable[P, RVDAG], max_concurrency: int, is_async: bool
) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]:
    """Clean up before and after making the DAG."""
    pass


def threadsafe_make_dag(
    _func: Union[Callable[P, RVDAG]], max_concurrency: int, is_async: bool
) -> Union[DAG[P, RVDAG], AsyncDAG[P, RVDAG]]:
    """Make DAG or AsyncDAG form the function that describes the DAG.

    Thread safe and cleans after itself.
    """
    pass
