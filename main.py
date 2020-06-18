from whetstone import Whetstone
import json
from os import path, makedirs


def extract(api, endpoint, tag=False):
    filename = f"data/{endpoint}.json"
    if path.exists(filename):
        pass
    else:
        data = api.get_all(endpoint, tag=tag)
        if not path.exists("data"):
            makedirs("data")
        with open(filename, "w") as f:
            f.write(json.dumps(data, indent=2))


def main():
    api = Whetstone()
    endpoints = [
        "assignments",
        "informals",
        "measurements",
        "meetings",
        "observations",
        "rubrics",
        "schools",
        "users",
    ]

    for endpoint in endpoints:
        extract(api, endpoint)

    tags = [
        "grades",
        "courses",
        "measurementgroups",
        "goaltypes",
        "meetingtypes",
        "observationtypes",
        "assignmenttypes",
        "assignmentpresets",
    ]

    for tag in tags:
        extract(api, tag, tag=True)


if __name__ == "__main__":
    main()
