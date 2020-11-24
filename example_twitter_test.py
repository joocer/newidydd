from newidydd import BaseOperator, runner
import newidydd
import datasets.io
import time
import random


class ExtractFollowersOperation(BaseOperator):

    def execute(self, data={}, context={}):
        followers = int(data.get("followers", 0))
        verified = data.get("user_verified", "False") == "True"
        result = {
            "followers": followers,
            "user": data.get("username", ""),
            "verified": verified,
        }
        return result, context


class MostFollowersOperation(BaseOperator):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.user = ""
        self.followers = 0

    def execute(self, data, context):

        if data.get("followers") > self.followers:
            self.followers = data.get("followers")
            self.user = data.get("user")
            return data, context
        else:
            return None

def main():

    newidydd.set_up_logging()

    data_validation = newidydd.operations.ValidationOperator(schema=open("twitter.schema", "r").read())
    extract_followers = ExtractFollowersOperation()
    filter_verified = newidydd.operations.FilterOperator(condition=lambda r: r.get('user_verified', 'False') == 'True')
    most_followers = MostFollowersOperation()
    screen_sink = newidydd.operations.PrintOperator()
    end = newidydd.operations.EndOperator()

    flow = data_validation > filter_verified > extract_followers > most_followers > screen_sink > end
    #flow = data_validation > [filter_verified, extract_followers > most_followers > screen_sink > end]

    t = time.process_time_ns()

    file_reader = datasets.io.read_jsonl("small.jsonl", limit=1)
    for record in file_reader:
        runner.go(flow=flow, data=record, context={}, trace_sample_rate=0) # nosec
        
    print((time.process_time_ns() - t) / 1e9)

    print(data_validation)
    print(filter_verified)
    print(extract_followers)
    print(most_followers)
    print(screen_sink)

if __name__ == "__main__":
    main()
