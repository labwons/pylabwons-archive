from pylabwons import DataDictionary
from pylabwons_stub.env import PATH
import json


class Log(DataDictionary):

    def __init__(self):
        with open(PATH.JSON.BUILD, 'r', encoding='utf-8') as f:
            super().__init__(json.load(f))
        return

    def save(self):
        with open(PATH.JSON.BUILD, 'w', encoding='utf-8') as f:
            json.dump(self, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    log = Log()
    # log.baseline = DataDictionary(
    #     date='20260227'
    # )
    print(log)
    # log.save()