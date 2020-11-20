from .operations import UndefinedOperator


def _inner_runner(flow=None, node=None, data={}, context={}, **kwargs):
    """
    Walk the flow by:
    - Getting the function of the current node
    - Execute the function, wrapped in the base class
    - Find the next step by finding outgoing edges
    - Call this method for the next step
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
    Execute a flow by discovering starting nodes and then
    calling a recursive function to walk the flow
    """
    nodes = [node for node in flow.nodes() if len(flow.in_edges(node)) == 0]
    for node in nodes:
        _inner_runner(flow=flow, node=node, data=data, context=context)
