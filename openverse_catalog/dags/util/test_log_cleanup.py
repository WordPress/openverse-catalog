from datetime import datetime, timedelta
from pathlib import Path

import pytest

from util.log_cleanup import clean_up

TEST_LOGS_FOLDER = Path(__file__).parent / "test_resources" / 'logs'
# Total number of logs in the `logs_folder` created
INITIAL_LOG_FILE_COUNT = 13
# Number of logs folders in the `test_resources/logs_folder`
# that are older than August 20
OLD_LOG_FOLDER_COUNT = 2
# 1 log file (`dag_process_manager.log`) is not deleted
NON_DELETED_FILE_COUNT = 1
ENABLE_DELETE = False

OLD_TIMESTAMP = datetime.fromisoformat('2021-08-10')
RECENT_TIMESTAMP = datetime.fromisoformat('2021-08-20')

logs_folder = Path(__file__).parent / 'test_resources' / 'logs'


@pytest.fixture
def cutoffs_in_days():
    NOW = datetime.now()
    one_day_before = OLD_TIMESTAMP - timedelta(days=1)
    one_day_after = OLD_TIMESTAMP + timedelta(days=1)
    delta_before = (NOW - one_day_before).days
    delta_after = (NOW - one_day_after).days
    return delta_before, delta_after

# @pytest.fixture()
# def logs_folder():
#
#     Path.mkdir(TEST_LOGS_FOLDER, parents=True, exist_ok=True)
#
#     recent_date = datetime.now(tz=timezone.utc)
#     old_date = datetime.now(tz=timezone.utc) - timedelta(days=8)
#     dates = [recent_date, old_date]
#
#     af_logs_folders = ['dag_processor_manager', 'scheduler', 'test_dag']
#     for folder in af_logs_folders:
#         Path.mkdir(TEST_LOGS_FOLDER / folder, parents=True, exist_ok=True)
#
#     dag_processor_manager_log = TEST_LOGS_FOLDER / 'dag_processor_manager' / 'dag_processor_manager.log'
#     dag_processor_manager_log.write_text('Dags processor manager log')
#
#     task_dir = TEST_LOGS_FOLDER / 'test_dag' / 'test_task'
#     Path.mkdir(task_dir, parents=True, exist_ok=True)
#
#     for log_date in dates:
#         timestamp = log_date.timestamp()
#
#         new_dir = task_dir / log_date.strftime('%Y-%m-%dT%H:%M:%S%z')
#         Path.mkdir(new_dir)
#
#         scheduler_dir = TEST_LOGS_FOLDER / 'scheduler' / log_date.strftime('%Y-%m-%d')
#         Path.mkdir(scheduler_dir, parents=True, exist_ok=True)
#
#         for item_dir in [new_dir, scheduler_dir]:
#             for num in range(1, 4):
#                 log_file = item_dir / f"{num}.log"
#                 log_file.write_text(f'Test log {num}')
#                 os.utime(log_file, (timestamp, timestamp))
#             os.utime(item_dir, (timestamp, timestamp))
#
#     yield TEST_LOGS_FOLDER


def test_log_cleaner_leaves_new_files(cutoffs_in_days):
    """ If all the log files are newer than the maxLogAgeInDays,
    no log files are deleted"""
    log_files_count = len(list(Path.glob(logs_folder, '**/*.log')))
    assert log_files_count == INITIAL_LOG_FILE_COUNT

    deleted_folders = clean_up(logs_folder, cutoffs_in_days[0], ENABLE_DELETE)
    deleted_count = len(deleted_folders)
    expected_count = 0

    assert deleted_count == expected_count


def test_log_cleaner_deletes_all_old_files(cutoffs_in_days):
    """Log cleaner deletes all the log files that are older than
    maxLogAgeInDays, but leaves the files that are newer"""
    deleted_folders = clean_up(logs_folder, cutoffs_in_days[1], ENABLE_DELETE)
    deleted_count = len(deleted_folders)

    expected_log_count = OLD_LOG_FOLDER_COUNT

    assert deleted_count == expected_log_count


def test_log_cleaner_deletes_all_but_one_files_if_max_is_minus_1(cutoffs_in_days):
    """If maxLogAgeInDays is set to -1, all log files except for
    `dag_processor_manager.log` are deleted
    Need to find out if the `dag_processor_manager.log` is recreated every day,
    or it just uses single log file for all time.
    """

    deleted_folders = clean_up(logs_folder, -1, ENABLE_DELETE)
    deleted_folder_count = len(deleted_folders)
    expected_count = 4

    assert deleted_folder_count == expected_count
