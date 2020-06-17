from whetstone import Whetstone
import json


def main():
    api = Whetstone()
    users = api.get_all("users")
    with open("users.json", "w") as f:
        f.write(json.dumps(users))


if __name__ == "__main__":
    main()
