"""
Provides an explicit termination.

Using this as a convention will avoid problems when a flow
only has a single operator as this will not build a dag.
However, this stage doesn't do anything and is optional.
"""
from .base_operator import BaseOperator


class EndOperator(BaseOperator):

    def execute(self, data={}, context={}):


        return None

