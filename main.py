import whetstone
from sqlsorcery import MSSQL


def main():
    sql = MSSQL()
    whetstone.Users(sql).transform_and_load()
    whetstone.Schools(sql).transform_and_load()
    whetstone.Meetings(sql).transform_and_load()
    whetstone.Observations(sql).transform_and_load()
    whetstone.Measurements(sql).transform_and_load()
    whetstone.Assignments(sql).transform_and_load()
    whetstone.Informals(sql).transform_and_load()
    whetstone.Rubrics(sql).transform_and_load()

    whetstone.Tag(sql, "courses").transform_and_load()
    whetstone.Tag(sql, "tags").transform_and_load()
    whetstone.Tag(sql, "grades").transform_and_load()
    whetstone.Tag(sql, "measurement_groups").transform_and_load()
    whetstone.Tag(sql, "goal_types").transform_and_load()
    whetstone.Tag(sql, "meeting_types").transform_and_load()
    whetstone.Tag(sql, "observation_types").transform_and_load()
    whetstone.Tag(sql, "assignment_types").transform_and_load()
    whetstone.Tag(sql, "assignment_presets").transform_and_load()
    whetstone.Tag(sql, "user_types").transform_and_load()
    whetstone.Tag(sql, "coaching_move_types").transform_and_load()
    whetstone.Tag(sql, "collaboration_types").transform_and_load()
    whetstone.Tag(sql, "measurement_types").transform_and_load()
    whetstone.Tag(sql, "meeting_modules").transform_and_load()
    whetstone.Tag(sql, "meeting_standards").transform_and_load()
    whetstone.Tag(sql, "observation_labels").transform_and_load()
    whetstone.Tag(sql, "observation_modules").transform_and_load()
    whetstone.Tag(sql, "observation_types").transform_and_load()
    whetstone.Tag(sql, "periods").transform_and_load()
    whetstone.Tag(sql, "tracks").transform_and_load()
    whetstone.Tag(sql, "plu_content_areas").transform_and_load()
    whetstone.Tag(sql, "plu_event_types").transform_and_load()


if __name__ == "__main__":
    main()
