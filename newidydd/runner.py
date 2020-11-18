from .operations import UndefinedOperator


def _inner_runner(flow=None, node=None, data={}, context={}):
    """
    """
    func = flow.nodes()[node].get("function", UndefinedOperator())
    data, context = func(data, context)

    next_nodes = flow.out_edges(node, default=[])
    for next_node in next_nodes:
        _inner_runner(flow=flow, node=next_node[1], data=data, context=context)


class Runner(object):

    @staticmethod
    def start(flow=None, data={}, context={}):

        print("runner")

        nodes = [node for node in flow.nodes() if len(flow.in_edges(node)) == 0]

        for node in nodes:
            _inner_runner(flow=flow, node=node, data=data, context=context)
