"""module containing DAG and DAGExecution which are the containers that run ExecNodes in Tawazi."""

import json
import logging
import pickle
import warnings
from collections import Counter
from collections.abc import Iterable, Sequence
from copy import deepcopy
from dataclasses import asdict, dataclass, field
from itertools import chain
from pathlib import Path
from typing import Any, Callable, Generic, NoReturn, Optional, Union

import networkx as nx
import yaml

from tawazi import consts
from tawazi._helpers import StrictDict, UniqueKeyLoader
from tawazi.config import cfg
from tawazi.consts import ARG_NAME_ACTIVATE, RVDAG, Identifier, P, Tag
from tawazi.errors import TawaziTypeError, TawaziUsageError
from tawazi.node import Alias, ArgExecNode, ExecNode, ReturnUXNsType, UsageExecNode, node
from tawazi.node.node import LazyExecNode, make_active, make_axn_id
from tawazi.profile import Profile

from .digraph import DiGraphEx
from .helpers import async_execute, extend_results_with_args, get_return_values, sync_execute

logger = logging.getLogger(__name__)


def construct_subdag_arg_uxns(
    *args: Iterable[Union[UsageExecNode, Any]], to_subdag_id: Callable[[str], str], qualname: str
) -> list[UsageExecNode]:
    """Construct UsageExecNodes from the arguments passed to a subdag."""
    pass


def detect_duplicates(expanded_config: list[tuple[Identifier, Any]]) -> None:
    pass


@dataclass
class BaseDAG(Generic[P, RVDAG]):
    """Data Structure containing ExecNodes with interdependencies.

    Please do not instantiate this class directly. Use the decorator `@dag` instead.
    The ExecNodes can be executed in parallel with the following restrictions:
        * Limited number of threads.
        * Parallelization constraint of each ExecNode (is_sequential attribute)
        * Priority of each ExecNode (priority attribute)
        * Specific Resource per ExecNode (resource attribute)
    This Class has two flavors:
        * DAG: for synchronous execution
        * AsyncDAG: for asynchronous execution

    Args:
        exec_nodes: all the ExecNodes
        input_uxns: all the input UsageExecNodes
        return_uxns: the return UsageExecNodes of various types: None, a single value, tuple, list, dict.
        max_concurrency: the maximal number of threads running in parallel
    """

    qualname: str
    results: StrictDict[Identifier, Any]
    exec_nodes: StrictDict[Identifier, ExecNode]
    input_uxns: list[UsageExecNode]
    return_uxns: ReturnUXNsType
    max_concurrency: int = 1
    graph_ids: DiGraphEx = field(init=False)

    def __post_init__(self) -> None:
        self.graph_ids = DiGraphEx.from_exec_nodes(
            input_nodes=self.input_uxns, exec_nodes=self.exec_nodes
        )

        # verification
        if not isinstance(self.max_concurrency, int):
            raise ValueError("max_concurrency must be an int")
        if self.max_concurrency < 1:
            raise ValueError("Invalid maximum number of threads! Must be a positive integer")
        self._max_concurrency = self.max_concurrency

        if not isinstance(self.results, StrictDict):
            raise ValueError("results must be a StrictDict")
        if not isinstance(self.exec_nodes, StrictDict):
            raise ValueError("exec_nodes must be a StrictDict")

    def draw(
        self, *, include_args: bool = False, filename: Optional[str] = None, view: bool = True
    ) -> None:
        """Draws the Networkx directed graph.

        Args:
            include_args: whether to include the arguments or not
            filename: the name of the file to save the graph to
            view: whether to view the graph or not
        """
        pass

    # getters
    def get_nodes_by_tag(self, tag: Tag) -> list[ExecNode]:
        """Get the ExecNodes with the given tag.

        Note: the returned ExecNode is not modified by any execution!
            This means that you can not get the result of its execution via `DAG.get_nodes_by_tag(<tag>).result`.
            In order to do that, you need to make a DAGExecution and then call
             `DAGExecution.get_nodes_by_tag(<tag>).result`, which will contain the results.

        Args:
            tag (Any): tag of the ExecNodes

        Returns:
            List[ExecNode]: corresponding ExecNodes
        """
        pass

    def get_node_by_id(self, node_id: Identifier) -> ExecNode:
        """Get the ExecNode with the given id.

        Note: the returned ExecNode is not modified by any execution!
            This means that you can not get the result of its execution via `DAG.get_node_by_id(<id>).result`.
            In order to do that, you need to make a DAGExecution and then call
             `DAGExecution.get_node_by_id(<id>).result`, which will contain the results.

        Args:
            node_id (Identifier): id of the ExecNode

        Returns:
            ExecNode: corresponding ExecNode
        """
        pass

    def _get_single_xn_by_alias(self, alias: Alias) -> ExecNode:
        """Get the ExecNode corresponding to the given Alias.

        Args:
            alias (Alias): the Alias to be resolved

        Raises:
            ValueError: if the Alias is not unique

        Returns:
            ExecNode: the ExecNode corresponding to the given Alias
        """
        pass

    # TODO: get node by usage (the order of call of an ExecNode)

    # TODO: implement ellipsis for composing with outputs
    # TODO: should we support kwargs when DAG.__call__ support kwargs?

    def compose(
        self,
        qualname: str,
        inputs: Union[Alias, Sequence[Alias]],
        outputs: Union[Alias, Sequence[Alias]],
        is_async: Optional[bool] = None,
        **kwargs: dict[str, Any],
    ) -> "Union[AsyncDAG[P, RVDAG], DAG[P, RVDAG]]":
        """Compose a new DAG using inputs and outputs ExecNodes (Experimental).

        All provided `Alias`es must point to unique `ExecNode`s. Otherwise ValueError is raised
        The user is responsible to correctly specify inputs and outputs signature of the `DAG`.
        * The inputs can be specified as a single `Alias` or a `Sequence` of `Alias`es.
        * The outputs can be specified as a single `Alias` (a single value is returned)
        or a `Sequence` of `Alias`es in which case a Tuple of the values are returned.
        If outputs are specified as [], () is returned.
        The syntax is the following:

        ```python
        >>> from tawazi import dag, xn, DAG
        >>> from typing import Tuple, Any
        >>> @xn
        ... def unwanted_xn() -> int: return 42
        >>> @xn
        ... def x(v: Any) -> int: return int(v)
        >>> @xn
        ... def y(v: Any) -> str: return str(v)
        >>> @xn
        ... def z(x: int, y: str) -> float: return float(x) + float(y)
        >>> @dag
        ... def pipe() -> Tuple[int, float, int]:
        ...     a = unwanted_xn()
        ...     res = z(x(1), y(1))
        ...     b = unwanted_xn()
        ...     return a, res, b
        >>> composed_dag = pipe.compose("twinkle", [x, y], z)
        >>> assert composed_dag(1, 1) == 2.0
        >>> # composed_dag: DAG[[int, str], float] = pipe.compose([x, y], [z])  # optional typing of the returned DAG!
        >>> # assert composed_dag(1, 1) == 2.0  # type checked!
        ```

        Args:
            qualname (str): the name of the composed DAG
            inputs (ellipsis, Alias | List[Alias]): the Inputs nodes whose results are provided. Provide ... to specify that you will provide every argument of the original DAG.
            outputs (Alias | List[Alias]): the Output nodes that must execute last, The ones that will generate results
            is_async (bool | None): if True, the composed DAG will be an AsyncDAG, if False, it will be a DAG. Defaults to whatever the original DAG is.
            **kwargs (Dict[str, Any]): additional arguments to be passed to the DAG's constructor
        """
        pass

    def alias_to_ids(self, alias: Alias) -> list[Identifier]:
        """Extract an ExecNode ID from an Alias (Tag, ExecNode ID or ExecNode).

        Args:
            alias (Alias): an Alias (Tag, ExecNode ID or ExecNode)

        Returns:
            The corresponding ExecNode IDs

        Raises:
            ValueError: if a requested ExecNode is not found in the DAG
            TawaziTypeError: if the Type of the identifier is not Tag, Identifier or ExecNode
        """
        pass

    def get_multiple_nodes_aliases(self, nodes: Sequence[Alias]) -> list[Identifier]:
        """Ensure correct Identifiers from aliases.

        Args:
            nodes: iterable of node aliases

        Returns:
            list of correct Identifiers
        """
        pass

    def _pre_setup(
        self,
        target_nodes: Optional[Sequence[Alias]],
        exclude_nodes: Optional[Sequence[Alias]],
        root_nodes: Optional[Sequence[Alias]],
    ) -> DiGraphEx:
        # 1. if target_nodes is not provided run all setup ExecNodes
        pass

    def _expand_config(
        self, config_nodes: dict[Union[Tag, Identifier], Any]
    ) -> list[tuple[Identifier, Any]]:
        pass

    def config_from_dict(self, config: dict[str, Any]) -> None:
        """Allows reconfiguring the parameters of the nodes from a dictionary.

        Args:
            config (Dict[str, Any]): the dictionary containing the config
                example: {"nodes": {"a": {"priority": 3, "is_sequential": True}}, "max_concurrency": 3}

        Raises:
            ValueError: if two nodes are configured by the provided config (which is ambiguous)
        """
        pass

    def config_from_yaml(self, config_path: str) -> None:
        """Allows reconfiguring the parameters of the nodes from a YAML file.

        Args:
            config_path: the path to the YAML file
        """
        pass

    def config_from_json(self, config_path: str) -> None:
        """Allows reconfiguring the parameters of the nodes from a JSON file.

        Args:
            config_path: the path to the JSON file
        """
        pass


@dataclass
class DAG(BaseDAG[P, RVDAG]):
    """SyncDAG implementation of the BaseDAG."""

    def executor(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
        cache_deps_of: Optional[Sequence[Alias]] = None,
        cache_in: str = "",
        from_cache: str = "",
    ) -> "DAGExecution[P, RVDAG]":
        """Generates a DAGExecution for the DAG.

        Args:
            target_nodes: the nodes to execute, excluding all nodes that can be excluded
            exclude_nodes: the nodes to exclude from the execution
            root_nodes: these nodes and their children will be included in the execution
            cache_deps_of: which nodes to cache the dependencies of
            cache_in: the path to the file where to cache
            from_cache: the cache

        Returns:
            the DAGExecution object associated with the dag
        """
        pass

    def setup(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
    ) -> None:
        """Run the setup ExecNodes for the DAG.

        If target_nodes are provided, run only the necessary setup ExecNodes, otherwise will run all setup ExecNodes.
        NOTE: `DAG` arguments should not be passed to setup ExecNodes.
            Only pass in constants or setup `ExecNode`s results.

        Args:
            target_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to use in the DAG.
                This might include setup or non setup ExecNodes. If None is provided, will run all setup ExecNodes.
                Defaults to None.
            exclude_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to exclude from the DAG.
                The user is responsible for ensuring that the overlapping between the target_nodes
                and exclude_nodes is logical.
            root_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to select as ancestor nodes.
                The user is responsible for ensuring that the overlapping between the target_nodes, the exclude_nodes
                and the root nodes is logical.
        """
        pass

    # TODO: discuss whether we want to expose it or not
    def run_subgraph(  # type: ignore[valid-type]
        self, subgraph: DiGraphEx, results: Optional[StrictDict[Identifier, Any]], *args: P.args
    ) -> tuple[
        StrictDict[Identifier, ExecNode],
        StrictDict[Identifier, Any],
        StrictDict[Identifier, Profile],
    ]:
        """Run a subgraph of the original graph (might be the same graph).

        Args:
            subgraph: the subgraph to run
            results: the results provided from the dag (containing setup) or coming from a modified DAG (from DAGExecution)
            *args: the args to pass to the graph

        Returns:
            a mapping between the execnodes and there identifiers
        """
        pass

    def _describe_subdag(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Describe current DAG as part of a DAG."""
        pass

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Execute the DAG scheduler via a similar interface to the function that describes the dependencies.

        Note: Currently kwargs are not supported.

        Args:
            *args (P.args): arguments to be passed to the call of the DAG
            **kwargs (P.kwargs): keyword arguments to be passed to the call of the DAG

        Returns:
            RVDAG: return value of the DAG's execution

        Raises:
            TawaziUsageError: kwargs are passed
        """
        description_context = node.exec_nodes_lock.locked()
        # is_active is only allowed when describing a SubDAG
        if kwargs and (not description_context or set(kwargs.keys()) != {ARG_NAME_ACTIVATE}):
            raise TawaziUsageError(f"currently DAG does not support keyword arguments: {kwargs}")

        if description_context:
            return self._describe_subdag(*args, **kwargs)

        graph = self.graph_ids.extend_graph_with_debug_nodes(self.graph_ids, cfg)
        _, results, _ = self.run_subgraph(graph, None, *args)
        return get_return_values(self.return_uxns, results)  # type: ignore[return-value]


@dataclass
class AsyncDAG(BaseDAG[P, RVDAG]):
    def executor(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
        cache_deps_of: Optional[Sequence[Alias]] = None,
        cache_in: str = "",
        from_cache: str = "",
    ) -> "AsyncDAGExecution[P, RVDAG]":
        """Generates a AsyncDAGExecution for the current AsyncDAG.

        Args:
            target_nodes: the nodes to execute, excluding all nodes that can be excluded
            exclude_nodes: the nodes to exclude from the execution
            root_nodes: these nodes and their children will be included in the execution
            cache_deps_of: which nodes to cache the dependencies of
            cache_in: the path to the file where to cache
            from_cache: the cache

        Returns:
            the DAGExecution object associated with the dag
        """
        pass

    async def setup(
        self,
        target_nodes: Optional[Sequence[Alias]] = None,
        exclude_nodes: Optional[Sequence[Alias]] = None,
        root_nodes: Optional[Sequence[Alias]] = None,
    ) -> None:
        """Run the setup ExecNodes for the DAG.

        If target_nodes are provided, run only the necessary setup ExecNodes, otherwise will run all setup ExecNodes.
        NOTE: `DAG` arguments should not be passed to setup ExecNodes.
            Only pass in constants or setup `ExecNode`s results.

        Args:
            target_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to use in the DAG.
                This might include setup or non setup ExecNodes. If None is provided, will run all setup ExecNodes.
                Defaults to None.
            exclude_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to exclude from the DAG.
                The user is responsible for ensuring that the overlapping between the target_nodes
                and exclude_nodes is logical.
            root_nodes (Optional[List[XNId]], optional): The ExecNodes that the user aims to select as ancestor nodes.
                The user is responsible for ensuring that the overlapping between the target_nodes, the exclude_nodes
                and the root nodes is logical.
        """
        pass

    # TODO: refactor this with previous method
    async def run_subgraph(  # type: ignore[valid-type]
        self, subgraph: DiGraphEx, results: Optional[StrictDict[Identifier, Any]], *args: P.args
    ) -> tuple[
        StrictDict[Identifier, ExecNode],
        StrictDict[Identifier, Any],
        StrictDict[Identifier, Profile],
    ]:
        """Run a subgraph of the original graph (might be the same graph).

        Args:
            subgraph: the subgraph to run
            results: the results provided from the dag (containing setup) or coming from a modified DAG (from DAGExecution)
            *args: the args to pass to the graph

        Returns:
            a mapping between the execnodes and there identifiers
        """
        pass

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Execute the DAG scheduler via a similar interface to the function that describes the dependencies.

        Note: Currently kwargs are not supported.

        Args:
            *args (P.args): arguments to be passed to the call of the DAG
            **kwargs (P.kwargs): keyword arguments to be passed to the call of the DAG

        Returns:
            RVDAG: return value of the DAG's execution

        Raises:
            TawaziUsageError: kwargs are passed
        """
        if kwargs:
            raise TawaziUsageError(f"currently DAG does not support keyword arguments: {kwargs}")

        graph = self.graph_ids.extend_graph_with_debug_nodes(self.graph_ids, cfg)
        _, results, _ = await self.run_subgraph(graph, None, *args)
        return get_return_values(self.return_uxns, results)  # type: ignore[return-value]


@dataclass
class BaseDAGExecution(Generic[P, RVDAG]):
    """A disposable callable instance of a DAG.

    It holds information about the last execution and is not threadsafe.

    Args:
        dag (DAG): The attached DAG.
        target_nodes (Optional[List[Alias]]): The leave ExecNodes to execute.
            If None will execute all ExecNodes.
        exclude_nodes (Optional[List[Alias]]): The leave ExecNodes to exclude.
            If None will exclude no ExecNode.
        root_nodes (Optional[List[Alias]]): The base ExecNodes that will server as ancestor for the graph.
            If None will run all ExecNodes.
        cache_deps_of (Optional[List[Alias]]): cache all the dependencies of these nodes.
            This option can not be used together with target_nodes nor exclude_nodes.
        cache_in (str):
            the path to the file where the execution should be cached.
            The path should end in `.pkl`.
            Will skip caching if `cache_in` is Falsy.
        from_cache (str):
            the path to the file where the execution should be loaded from.
            The path should end in `.pkl`.
            Will skip loading from cache if `from_cache` is Falsy.
    """

    dag: BaseDAG[P, RVDAG]
    target_nodes: Optional[Sequence[Alias]] = None
    exclude_nodes: Optional[Sequence[Alias]] = None
    root_nodes: Optional[Sequence[Alias]] = None

    # NOTE: from_cache is orthogonal to cache_in which means that if cache_in is set at the same time as from_cache.
    #  in this case the DAG will be loaded from_cache and the results will be saved again to the cache_in file.
    cache_deps_of: Optional[Sequence[Alias]] = None
    cache_in: str = ""
    from_cache: str = ""

    xn_dict: dict[Identifier, ExecNode] = field(init=False, default_factory=dict)
    executed: bool = False
    cached_nodes: list[ExecNode] = field(init=False, default_factory=list)

    profiles: dict[Identifier, Profile] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        """Dynamic construction of attributes."""
        # build the graph from cache if it exists
        if self.cache_deps_of is not None:
            if (
                self.target_nodes is not None
                or self.exclude_nodes is not None
                or self.root_nodes is not None
            ):
                raise ValueError(
                    "cache_deps_of can't be used together with target_nodes or exclude_nodes"
                )

            self.cache_deps_of = self.dag.get_multiple_nodes_aliases(self.cache_deps_of)
            graph = self.dag.graph_ids.make_subgraph(target_nodes=self.cache_deps_of)
            self.cached_nodes = list(graph.nodes)
        else:
            # clean user input
            if self.target_nodes is not None:
                self.target_nodes = self.dag.get_multiple_nodes_aliases(self.target_nodes)

            if self.exclude_nodes is not None:
                self.exclude_nodes = self.dag.get_multiple_nodes_aliases(self.exclude_nodes)

            if self.root_nodes is not None:
                self.root_nodes = self.dag.get_multiple_nodes_aliases(self.root_nodes)

            graph = self.dag.graph_ids.make_subgraph(
                target_nodes=self.target_nodes,
                exclude_nodes=self.exclude_nodes,
                root_nodes=self.root_nodes,
            )

        # add debug nodes
        self.graph = graph.extend_graph_with_debug_nodes(self.dag.graph_ids, cfg)

    @property
    def results(self) -> StrictDict[Identifier, Any]:
        """Returns the results of the previous DAGExecution.

        Before the DAG is executed, the results are the same as the underlying DAG. This also includes before/after setup.
        After Execution, the results have been enriched with all the ExecNodes' results.
        """
        pass

    @results.setter
    def results(self, value: StrictDict[Identifier, Any]) -> None:
        """Set results."""
        pass

    def _cache_results(self, results: dict[Identifier, Any]) -> None:
        """Cache execution results.

        We are currently only storing the results of the execution,
        so the configuration of the ExecNodes is lost
        But this it should not change between executions.
        """
        pass

    def _pre_call(self) -> None:
        pass

    def _post_call(self) -> RVDAG:
        # mark as executed. Important for the next step
        pass


@dataclass
class DAGExecution(BaseDAGExecution[P, RVDAG]):
    """Sync implementation of BaseDAGExecution."""

    dag: DAG[P, RVDAG]

    def setup(self) -> None:
        """Same thing as DAG.setup but `target_nodes` and `exclude_nodes` come from the DAGExecution's init."""
        pass

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Call the DAG.

        Args:
            *args: positional arguments to pass in to the DAG
            **kwargs: keyword arguments to pass in to the DAG

        Raises:
            TawaziUsageError: if the DAGExecution has already been executed.

        Returns:
            RVDAG: the return value of the DAG's Execution
        """
        self._pre_call()

        # 2. Execute the scheduler
        self.xn_dict, self.results, self.profiles = self.dag.run_subgraph(
            self.graph, self.results, *args
        )

        return self._post_call()


class AsyncDAGExecution(BaseDAGExecution[P, RVDAG]):
    """Async implementation of BaseDAGExecution."""

    dag: AsyncDAG[P, RVDAG]

    async def setup(self) -> None:
        """Same thing as DAG.setup but `target_nodes` and `exclude_nodes` come from the DAGExecution's init."""
        pass

    async def __call__(self, *args: P.args, **kwargs: P.kwargs) -> RVDAG:
        """Call the DAG.

        Args:
            *args: positional arguments to pass in to the DAG
            **kwargs: keyword arguments to pass in to the DAG

        Raises:
            TawaziUsageError: if the DAGExecution has already been executed.

        Returns:
            RVDAG: the return value of the DAG's Execution
        """
        self._pre_call()

        # 2. Execute the scheduler
        self.xn_dict, self.results, self.profiles = await self.dag.run_subgraph(
            self.graph, self.results, *args
        )

        return self._post_call()
