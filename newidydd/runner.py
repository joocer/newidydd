from .operations import UndefinedOperator


def _inner_runner(flow=None, node=None, data={}, context={}, **kwargs):
    """
    """
    func = flow.nodes()[node].get("function", UndefinedOperator())
    next_nodes = flow.out_edges(node, default=[])
    outcome = func(data, context)

    if type(outcome).__name__ == "generator":
        for outcome_data, outcome_context in outcome:
            for next_node in next_nodes:
                _inner_runner(flow=flow, node=next_node[1], data=outcome_data, context=outcome_context)
    elif outcome:
        outcome_data, outcome_context = outcome
        for next_node in next_nodes:
            _inner_runner(flow=flow, node=next_node[1], data=outcome_data, context=outcome_context) 


def go(flow=None, data={}, context={}):
    """
    """
    nodes = [node for node in flow.nodes() if len(flow.in_edges(node)) == 0]
    for node in nodes:
        _inner_runner(flow=flow, node=node, data=data, context=context)
