from whetstone import Users
from sqlsorcery import MSSQL


def main():
    sql = MSSQL()
    Users(sql).import_all()


if __name__ == "__main__":
    main()
