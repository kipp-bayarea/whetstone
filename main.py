from whetstone import Users, Schools, Meetings, Observations
from sqlsorcery import MSSQL


def main():
    sql = MSSQL()
    Users(sql).import_all()
    Schools(sql).import_all()
    Meetings(sql).import_all()
    Observations(sql).import_all()


if __name__ == "__main__":
    main()
