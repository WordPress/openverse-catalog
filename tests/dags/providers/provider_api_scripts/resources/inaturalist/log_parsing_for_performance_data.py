import re
from pathlib import Path

import pandas as pd


LOG_FILE_NAME = "dag_id=inaturalist_workflow_run_id=manual__2022-12-14T23 35 11.445478+00 00_task_id=ingest_data.load_image_data.load_transformed_data_attempt=1.log"  # noqa
LOG_DIR = Path(__file__).parent

re_start_line = re.compile(
    r"^\[(.*)\] \{inaturalist\.py\:115\} INFO - Starting at photo_id (\d{1,10})\, page (\d{1,4})"  # noqa E501
)  # (start_timestamp, start_photo_id, start_page)
re_done_line = re.compile(
    r"^\[(.*)\] \{inaturalist\.py\:137\} INFO - Loaded (\d{1,10}) records\, as of page (\d{1,4})"  # noqa E501
)  # (done_timestamp, records, start_page)

# initialize lists to collect parsed log results
start_lines = []
done_lines = []
# go through the log file
with open(LOG_DIR / LOG_FILE_NAME) as f:
    for line in f:
        if "INFO - Starting at photo_id" in line:
            re_match = re.match(re_start_line, line)
            # print(f"START {re_match.groups()=}")
            start_lines += [re_match.groups()]
        elif "{inaturalist.py:137} INFO - Loaded" in line:
            re_match = re.match(re_done_line, line)
            # print(f"DONE {re_match.groups()=}")
            done_lines += [re_match.groups()]
print(f"{len(start_lines)=}, {len(done_lines)=}")
# make data frames from lists of tuples
start_df = pd.DataFrame(
    data=start_lines,
    columns=["start_timestamp", "start_photo_id", "start_page"],
).set_index("start_page")
done_df = pd.DataFrame(
    data=done_lines,
    columns=["done_timestamp", "records", "start_page"],
).set_index("start_page")
# reformat timestamp
start_df["start_time_udf"] = start_df.start_timestamp.str.replace("T", " ").str[0:19]
# join the data frames and pick columns
joined_df = start_df.join(done_df, how="left")[["start_time_udf", "records"]]
print(joined_df)
# write to csv
joined_df.to_csv(
    LOG_DIR / "parsed_log.csv",
    header=True,
    index=True,
)
