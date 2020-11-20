"""
This is kind of what I want to write
"""

from newidydd import BaseOperator, runner
import time
import random

class ThisOperator(BaseOperator):

    def execute(self, data={}, context={}):
        for i in range(10000):
            n = i * i
        return data, context

class ThatOperator(BaseOperator):

    def execute(self, data={}, context={}):
        return data, context

class TheOtherOperator(BaseOperator):

    def __init__(self, count, **kwargs):
        super().__init__(**kwargs)
        self.count = count

    def execute(self, data={}, context={}):
        for i in range(self.count):
            yield data, context


this = ThisOperator()
that = ThatOperator()
other = TheOtherOperator(count=5)
flow =  other > that > this

data_reader = ['A', 'B', 'C', 'D']

for data in data_reader:
    runner.go(flow=flow, data=data, context={"trace": random.choice([True, False])})

print(this)