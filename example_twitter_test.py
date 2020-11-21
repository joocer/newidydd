"""
This is kind of what I want to write
"""

from newidydd import BaseOperator, runner
import newidydd
import datasets.io
import time
import random


class ExtractFollowersOperation(BaseOperator):
    """
    Processing operation, unique to this flow.

    Create a class that inherits from Operation and override the
    execute method. The method is passed a message object, this
    has a payload attribute which is one record being pushed
    through the flow.

    Perform the porcessing, update the payload and pass back a list
    of message objects, even if there's only one.
    """
    def execute(self, data={}, context={}):
        followers = int(data.get("followers", -1))
        verified = data.get("user_verified", "False") == "True"
        result = {
            "followers": followers,
            "user": data.get("username", ""),
            "verified": verified,
        }
        return result, context


class MostFollowersOperation(BaseOperator):
    """
    Another processing operation unique to this flow.

    Includes an init method, this is called once when the flow
    is being initialized
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.user = ""
        self.followers = 0

    def execute(self, data, context):
        """
        This operation will drop records, it does that by returning
        None in the place of messages.

        No return also works but returning None is more explicit.
        """
        if data.get("followers") > self.followers:
            self.followers = data.get("followers")
            self.user = data.get("user")
            return data, context
        else:
            return None


def main():

    data_validation = newidydd.operations.ValidationOperator(schema=open("twitter.schema", "r").read())
    extract_followers = ExtractFollowersOperation()
    most_followers = MostFollowersOperation()
    screen_sink = newidydd.operations.PrintOperator()

    flow = data_validation > extract_followers > most_followers > screen_sink

    t = time.process_time_ns()

    file_reader = datasets.io.read_jsonl("small.jsonl", limit=-1)
    for record in file_reader:
        runner.go(flow=flow, data=record, context={}) # nosec

    print((time.process_time_ns() - t) / 1e9)

    print(data_validation)
    print(extract_followers)
    print(most_followers)
    print(screen_sink)

if __name__ == "__main__":
    main()
