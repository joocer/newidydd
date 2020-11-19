from .base_operator import BaseOperator


class PrintOperator(BaseOperator):
    """
    Does nothing.
    """

    def execute(self, data={}, context={}):
        print(data)