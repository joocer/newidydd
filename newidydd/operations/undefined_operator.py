from .base_operator import BaseOperator


class UndefinedOperator(BaseOperator):

    def execute(self, data={}, context={}):
        raise NotImplementedError()