"""
Dictionary Validation Operator
"""
from .base_operator import BaseOperator
from typhaon import Schema


class ValidationOperator(BaseOperator):

    def __init__(self, schema={}, *args, **kwargs):
        self.validator = Schema(schema)
        self.invalid_records = 0
        super().__init__(*args, **kwargs)

    def execute(self, data={}, context={}):
        valid = self.validator(subject=data)
        if not valid:
            self.errors += 1
            return None
        else:
            return data, context

