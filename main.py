import logging
import sys
import traceback
import whetstone
from sqlsorcery import MSSQL


def configure_logging():
    logging.basicConfig(
        handlers=[
            logging.FileHandler(filename="data/app.log", mode="w+"),
            logging.StreamHandler(sys.stdout),
        ],
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %I:%M:%S%p %Z",
    )
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)


def main():
    configure_logging()
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
    try:
        main()
        error_message = None
    except Exception as e:
        logging.exception(e)
        error_message = traceback.format_exc()
