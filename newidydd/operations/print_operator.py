from .base_operator import BaseOperator


class PrintOperator(BaseOperator):
    """
    Writes the data to the StdOut
    """

    def execute(self, data={}, context={}):
        print(data)
        return data, context