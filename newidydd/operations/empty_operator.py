"""
Empty Operator

This does nothing - without error, for testing.

It optionally prints the class name.
"""

from .base_operator import BaseOperator


class EmptyOperator(BaseOperator):

    def __init__(self, print_message=False):
        self.print_message = print_message
        super().__init__()

    def execute(self, data={}, context={}):
        if self.print_message:
            print(self.__class__.__name__)
        return data, context