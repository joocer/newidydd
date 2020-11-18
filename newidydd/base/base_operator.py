"""
Operation is a base class which includes most of the heavy-lifting
for tracing and auditing.

"""

import abc
import inspect
import hashlib
import functools

class BaseOperator(abc.ABC):

    def __init__(self):
        self.graph = None
        self.executions = 0

    @abc.abstractmethod
    def execute(self, data={}, context={}):
        raise NotImplementedError("execute must be overridden")



    def __call__(self, data={}, context={}):
        self.executions += 1
        return self.execute(data, context)

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
        del self.graph

        return graph

