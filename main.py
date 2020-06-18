import whetstone
from sqlsorcery import MSSQL


def main():
    sql = MSSQL()
    whetstone.Users(sql).import_all()
    whetstone.Schools(sql).import_all()
    whetstone.Meetings(sql).import_all()
    whetstone.Observations(sql).import_all()
    whetstone.Measurements(sql).import_all()


if __name__ == "__main__":
    main()
