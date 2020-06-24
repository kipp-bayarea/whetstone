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

    tags = [
        "courses",
        "tags",
        "grades",
        "measurement_groups",
        "goal_types",
        "meeting_types",
        "observation_types",
        "assignment_types",
        "assignment_presets",
        "user_types",
        "coaching_move_types",
        "collaboration_types",
        "measurement_types",
        "meeting_modules",
        "meeting_standards",
        "observation_labels",
        "observation_modules",
        "observation_types",
        "periods",
        "tracks",
        "plu_content_areas",
        "plu_event_types",
    ]

    for tag in tags:
        whetstone.Tag(sql, tag).transform_and_load()


if __name__ == "__main__":
    main()
