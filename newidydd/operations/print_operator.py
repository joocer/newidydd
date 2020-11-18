from ..base import BaseOperator


class PrintOperator(BaseOperator):
    """
    Does nothing.
    """

    def execute(self, data={}, context={}):
        print(data)