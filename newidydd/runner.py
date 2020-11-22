from .operations import UndefinedOperator
import uuid
import random


def _inner_runner(flow=None, node=None, data={}, context={}, **kwargs):
    """
    Walk the dag/flow by:
    - Getting the function of the current node
    - Execute the function, wrapped in the base class
    - Find the next step by finding outgoing edges
    - Call this method for the next step
    """
    func = flow.nodes()[node].get("function", UndefinedOperator())
    next_nodes = flow.out_edges(node, default=[])
    outcome = func(data, context)

    if outcome:
        if not type(outcome).__name__ in ["generator", "list"]:
            outcome_data, outcome_context = outcome
            outcome = [(outcome_data, outcome_context)]
        for outcome_data, outcome_context in outcome:
            for next_node in next_nodes:
                _inner_runner(flow=flow, node=next_node[1], data=outcome_data, context=outcome_context)


def go(flow=None, data={}, context={}, trace_sample_rate=0.001):
    """
    Execute a flow by discovering starting nodes and then
    calling a recursive function to walk the flow
    """
    # create a copy of the context
    my_context = context.copy()
    # create a uuid for the message
    my_context['uuid'] = str(uuid.uuid4())

    # if trace hasn't been set - randomly select based on a sample rate
    if not my_context.get('trace'):
        my_context['trace'] = random.randint(1, round(1 / trace_sample_rate)) == 1  # nosec

    nodes = [node for node in flow.nodes() if len(flow.in_edges(node)) == 0]
    for node in nodes:
        _inner_runner(flow=flow, node=node, data=data, context=my_context)
