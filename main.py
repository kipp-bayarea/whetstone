import whetstone
from sqlsorcery import MSSQL


def main():
    sql = MSSQL()
    whetstone.Users(sql).transform_and_load()
    whetstone.Schools(sql).transform_and_load()
    whetstone.Meetings(sql).transform_and_load()
    whetstone.Observations(sql).transform_and_load()
    whetstone.Measurements(sql).transform_and_load()


if __name__ == "__main__":
    main()
