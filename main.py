from whetstone import Users
from sqlsorcery import SQLite


def main():
    sql = SQLite(path="test.db")
    Users(sql).import_all()


if __name__ == "__main__":
    main()
