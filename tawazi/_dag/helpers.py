import asyncio
import contextvars
import functools
import logging
from concurrent.futures import ALL_COMPLETED, FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from copy import copy
from typing import Any, Callable, TypeVar

from typing_extensions import ParamSpec

from tawazi._dag.digraph import DiGraphEx
from tawazi._helpers import StrictDict
from tawazi.consts import Identifier, Resource, RVTypes
from tawazi.errors import TawaziTypeError
from tawazi.node import ExecNode, ReturnUXNsType, UsageExecNode
from tawazi.profile import Profile

logger = logging.getLogger(__name__)
K = TypeVar("K")
V = TypeVar("V")


def _xn_active_in_call(xn: ExecNode, results: dict[Identifier, Any]) -> bool:
    """Check if a node is active.

    Args:
        xn: the execnode
        results: dict containing the results of the execution

    Returns:
        is the node active
    """
    pass


def copy_non_setup_xns(x_nodes: StrictDict[str, ExecNode]) -> StrictDict[str, ExecNode]:
    """Deep copy all ExecNodes except setup ExecNodes because they are shared throughout the DAG instance.

    Args:
        x_nodes: Dict[str, ExecNode] x_nodes to be deep copied

    Returns:
        Dict[str, ExecNode] copy of x_nodes
    """
    pass


class BiDict(dict[K, V]):
    """A bidirectional dictionary that raises an error if two keys are mapped to the same value.

    >>> b = BiDict({1: 'one', 2: 'two'})
    >>> b[1]
    'one'
    >>> b.inverse['one']
    1
    >>> b[3] = 'three'
    >>> b.inverse['three']
    3
    >>> b[3] = 'threeeee'
    >>> b.inverse['threeeee']
    3
    >>> b[4] = 'one'
    Traceback (most recent call last):
    ...
    ValueError: Value one is already in the BiDict
    >>> del b[1]
    >>> assert 1 not in b
    >>> b[4] = 'one'
    >>> b.inverse['one']
    4
    >>> BiDict({1: 2, 3: 2})
    Traceback (most recent call last):
    ...
    ValueError: Value 2 is already in the BiDict
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.inverse: dict[V, K] = {}
        for key, value in self.items():
            if value in self.inverse:
                raise ValueError(f"Value {value} is already in the BiDict")
            self.inverse[value] = key

    def __setitem__(self, key: K, value: V) -> None:
        if key in self:
            del self.inverse[self[key]]
        # check for errors before setting the value
        if value in self.inverse:
            raise ValueError(f"Value {value} is already in the BiDict")
        super().__setitem__(key, value)
        self.inverse[value] = key

    def __delitem__(self, key: K) -> None:
        del self.inverse[self[key]]
        super().__delitem__(key)


# TODO: refactor these functions by re-using the common parts
def wait_for_finished_nodes(
    return_when: str,
    graph: DiGraphEx,
    futures: BiDict[Identifier, "Future[Any]"],
    done: set["Future[Any]"],
    running: set["Future[Any]"],
    runnable_xns_ids: set[Identifier],
) -> tuple[set["Future[Any]"], set["Future[Any]"], set[Identifier]]:
    """Wait for the finished futures before pruning them from the graph.

    Args:
        return_when: condition for thread return
        graph: the directed graph
        futures: the futures to id map and reverse map
        done: the set of finished futures
        running: the running threads
        runnable_xns_ids: the exec nodes that are available to be run

    Returns:
        finisehd futures, running futures and runnable nodes
    """
    pass


async def wait_for_finished_nodes_async(
    return_when: str,
    graph: DiGraphEx,
    futures: BiDict[Identifier, "asyncio.Future[Any]"],
    done: set["asyncio.Future[Any]"],
    running: set["asyncio.Future[Any]"],
    runnable_xns_ids: set[Identifier],
) -> tuple[set["asyncio.Future[Any]"], set["asyncio.Future[Any]"], set[Identifier]]:
    """Wait for the finished futures before pruning them from the graph.

    Args:
        return_when: condition for thread return
        graph: the directed graph
        futures: the futures to id map and reverse map
        done: the set of finished futures
        running: the running async-threads
        runnable_xns_ids: the exec nodes that are available to be run

    Returns:
        finisehd futures, running futures and runnable nodes
    """
    pass


Param = ParamSpec("Param")
R = TypeVar("R")


def preserve_context(
    func: Callable[Param, R], *args: Param.args, **kwargs: Param.kwargs
) -> Callable[..., R]:
    """Wrap a function to keep the context of parent."""
    pass


async def to_thread_in_executor(
    func: Callable[..., Any], executor: ThreadPoolExecutor
) -> "asyncio.Future[Any]":
    """A modified copy of asyncio.to_thread.

    Asynchronously run function *func* in a separate thread.
    Uses the same thread pool executor for all threads.
    Doesn't handle passing the context from parent to child thread.

    Args:
        func: The function to run in the thread.
        executor: The executor to use.

    Return a coroutine that can be awaited to get the result of *func*.
    """
    pass


################
# The scheduler!
################
def sync_execute(
    exec_nodes: StrictDict[Identifier, ExecNode],
    results: StrictDict[Identifier, Any],
    max_concurrency: int,
    graph: DiGraphEx,
) -> tuple[
    StrictDict[Identifier, ExecNode], StrictDict[Identifier, Any], StrictDict[Identifier, Profile]
]:
    """Look at the execute function for more information."""
    pass


async def async_execute(
    *,
    exec_nodes: StrictDict[Identifier, ExecNode],
    results: StrictDict[Identifier, Any],
    max_concurrency: int,
    graph: DiGraphEx,
) -> tuple[
    StrictDict[Identifier, ExecNode], StrictDict[Identifier, Any], StrictDict[Identifier, Profile]
]:
    """Thread safe execution of the DAG.

    (Except for the setup nodes! Please run DAG.setup() in a single thread because its results will be cached).

    Args:
        exec_nodes: dictionary identifying ExecNodes.
        results: dictionary containing results of setup and constants
        max_concurrency: maximum number of threads to be used for the execution.
        graph: the graph ids to be executed

    Returns:
        exec_nodes: dictionary with keys the name of the function and value the result after the execution
    """
    pass


def get_return_values(return_uxns: ReturnUXNsType, results: dict[Identifier, Any]) -> RVTypes:
    """Extract the return value/values from the output of the DAG's scheduler!

    Args:
        return_uxns: the return execnodes
        results: the results of the execution of the DAG's ExecNodes

    Raises:
        TawaziTypeError: if the type of the return value is not compatible with RVTypes

    Returns:
        RVTypes: the actual values extracted from xn_dict
    """
    pass


def extend_results_with_args(
    results: StrictDict[Identifier, Any], input_uxns: list[UsageExecNode], *args: Any
) -> StrictDict[Identifier, Any]:
    """Extends the results of dict with the values provided by args.

    Args:
        results: results of the DAG (Constant arguments of ExecNodes and setup ExecNodes)
        input_uxns: the input execnodes
        *args (Any): arguments to be passed to the call of the DAG

    Returns:
        Dict[Identifier, ExecNode]: The modified ExecNode dict which will be executed by the DAG scheduler.

    Raises:
        TypeError: If called with an invalid number of arguments
    """
    pass
