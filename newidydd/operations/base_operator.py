"""
BaseOperator is a base class which includes most of the 
heavy-lifting for the execution.
"""
import abc
import inspect
import hashlib
import functools
import time
import datetime
from ..helpers.block_chain import BlockChain
import logging

BLOCK_CHAIN = 'trace-block'


class BaseOperator(abc.ABC):

    def __init__(self, **kwargs):
        self.graph = None           # part of drawing dags
        self.visits = 0             # number of times this operator has been run
        self.execution_time_ns = 0  # nano seconds of cpu execution time
        self.errors = 0             # number of errors

        self.retry_count = _clamp(kwargs.get('retry_count', 1), 1, 5)
        self.retry_wait = _clamp(kwargs.get('retry_wait', 5), 1, 300)

        if inspect.getsource(self.execute) != inspect.getsource(self.__class__.execute):
            raise Exception("Operator's __call__ method must not be overridden.")

    @abc.abstractmethod
    def execute(self, data={}, context={}):
        raise NotImplementedError("execute must be overridden")

    def __call__(self, data={}, context={}):
        """
        DO NOT OVERWRITE THIS METHOD
        """
        self.visits += 1
        attempts_to_go = self.retry_count
        while attempts_to_go > 0:
            try:
                start_time = time.perf_counter_ns()
                outcome = self.execute(data, context)
                self.execution_time_ns += (time.perf_counter_ns() - start_time)
                break
            except Exception as err:
                self.errors += 1
                attempts_to_go -= 1
                print(F"[ERROR] {datetime.datetime.today().isoformat()} {self.__class__.__name__} {type(err).__name__} {err}")
                if attempts_to_go:
                    time.sleep(self.retry_wait)
                else:
                    return None

        if context.get('trace', False):
            #print(F"[TRACE] {datetime.datetime.today().isoformat()} {context.get('uuid')} {self.__class__.__name__} {outcome[0] if outcome else 'None'}")
            if not context.get(BLOCK_CHAIN):
                context[BLOCK_CHAIN] = BlockChain(data=data, uuid=context['uuid'])
            context[BLOCK_CHAIN].add_new_event(data=outcome[0] if outcome else 'None', operator=self.__class__.__name__)
            if not outcome:
                context[BLOCK_CHAIN].commit_block()

        return outcome

    def __repr__(self):
        return {
            "operation": self.__class__.__name__,
            "visits": self.visits,
            "execution_time": self.execution_time_ns
        }

    def __str__(self):
        return F"[SENSOR] {datetime.datetime.today().isoformat()} {self.__class__.__name__} {self.visits} {self.execution_time_ns / 1e9}"

    def __del__(self):
        logging.getLogger("newidydd").debug(F"Operator {self.__class__.__name__} ending, run {self.visits} times.")

    @functools.lru_cache(1)
    def version(self):
        """
        DO NOT OVERRIDE THIS METHOD.

        The version of the operation code, this is intended to
        facilitate reproducability and auditability of the pipeline.
        The version is the last 12 characters of the hash of the
        source code of the 'execute' method. This removes the need
        for the developer to remember to increment a version
        variable.
        
        Hashing isn't security sensitive here, it's to identify
        changes rather than protect information.
        """
        source = inspect.getsource(self.execute)
        full_hash = hashlib.sha224(source.encode())
        self.my_version = full_hash.hexdigest()[-12:]

    def __gt__(self, target):
        """
        Smart DAG builder. This allows simple DAGs to be defined
        using the following syntax:
        Op1 > Op2 > [Op3, Op4]
        """
        import networkx as nx

        # make sure the target is iterable
        if type(target).__name__ != "list":
            target = [target]
        if self.graph:
            # if I have a graph already, build on it
            graph = self.graph
        else:
            # if I don't have a graph, create one
            graph = nx.DiGraph()
            graph.add_node(self.__class__.__name__, function=self)
        for point in target:
            if type(point).__name__ == "DiGraph":
                # if we're pointing to a graph, merge with the
                # current graph, we need to find the node with no
                # incoming nodes we identify the entry-point
                graph = nx.compose(point, graph)
                graph.add_edge(
                    self.__class__.__name__,
                    [node for node in point.nodes() if len(graph.in_edges(node)) == 0][0],
                )
            else:
                # otherwise add the node and edge and set the
                # graph further down the line
                graph.add_node(point.__class__.__name__, function=point)
                graph.add_edge(self.__class__.__name__, point.__class__.__name__)
                point.graph = graph
        # this variable only exists to build the graph, we don't
        # need it anymore so destroy it
        self.graph = None

        return graph

def _clamp(value, low_bound, high_bound):
    """
    'clamping' is fixing a value within a range
    """
    if value <= low_bound:
        return low_bound
    if value >= high_bound:
        return high_bound
    return value