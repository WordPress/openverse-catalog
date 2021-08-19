import os
import shutil
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from log_cleanup import clean_up

TEST_LOGS_FOLDER = Path(__file__).parent / "test_resources" / 'logs'
# Total number of logs in the `logs_folder` created
INITIAL_LOG_FILE_COUNT = 13
# Number of logs in the `logs_folder` fixture that is older than 7 days
OLD_LOG_FILE_COUNT = 6
# 1 log file (`dag_process_manager.log`) is not deleted
NON_DELETED_FILE_COUNT = 1


@pytest.fixture()
def logs_folder():

    if Path.is_dir(TEST_LOGS_FOLDER):
        shutil.rmtree(TEST_LOGS_FOLDER)
    Path.mkdir(TEST_LOGS_FOLDER, parents=True, exist_ok=True)

    recent_date = datetime.now(tz=timezone.utc)
    old_date = datetime.now(tz=timezone.utc) - timedelta(days=8)
    dates = [recent_date, old_date]

    af_logs_folders = ['dag_processor_manager', 'scheduler', 'test_dag']
    for folder in af_logs_folders:
        Path.mkdir(TEST_LOGS_FOLDER / folder, parents=True, exist_ok=True)

    dag_processor_manager_log = TEST_LOGS_FOLDER / 'dag_processor_manager' / 'dag_processor_manager.log'
    dag_processor_manager_log.write_text('Dags processor manager log')

    task_dir = TEST_LOGS_FOLDER / 'test_dag' / 'test_task'
    Path.mkdir(task_dir, parents=True, exist_ok=True)

    for log_date in dates:
        timestamp = log_date.timestamp()

        new_dir = task_dir / log_date.strftime('%Y-%m-%dT%H:%M:%S%z')
        Path.mkdir(new_dir)

        scheduler_dir = TEST_LOGS_FOLDER / 'scheduler' / log_date.strftime('%Y-%m-%d')
        Path.mkdir(scheduler_dir, parents=True, exist_ok=True)

        for item_dir in [new_dir, scheduler_dir]:
            for num in range(1, 4):
                log_file = item_dir / f"{num}.log"
                log_file.write_text(f'Test log {num}')
                os.utime(log_file, (timestamp, timestamp))
            os.utime(item_dir, (timestamp, timestamp))

    yield TEST_LOGS_FOLDER
    shutil.rmtree(TEST_LOGS_FOLDER)


def test_log_cleaner_leaves_new_files(logs_folder):
    """ If all the log files are newer than the maxLogAgeInDays,
    no log files are deleted"""
    log_files_count = len(list(Path.glob(logs_folder, '**/*.log')))
    assert log_files_count == INITIAL_LOG_FILE_COUNT

    clean_up(logs_folder, 9, True)

    log_files_count = len(list(Path.glob(logs_folder, '**/*.log')))

    assert log_files_count == INITIAL_LOG_FILE_COUNT


def test_log_cleaner_deletes_all_old_files(logs_folder):
    """Log cleaner deletes all the log files that are older than
    maxLogAgeInDays, but leaves the files that are newer"""

    log_files_count = len(list(Path.glob(logs_folder, '**/*.log')))
    assert log_files_count == INITIAL_LOG_FILE_COUNT

    clean_up(logs_folder, 7, True)

    log_files_count = len(list(Path.glob(logs_folder, '**/*.log')))
    expected_log_count = INITIAL_LOG_FILE_COUNT - OLD_LOG_FILE_COUNT

    assert log_files_count == expected_log_count


def test_log_cleaner_deletes_all_but_one_files_if_max_is_minus_1(logs_folder):
    """If maxLogAgeInDays is set to -1, all log files except for
    `dag_processor_manager.log` are deleted
    Need to find out if the `dag_processor_manager.log` is recreated every day,
    or it just uses single log file for all time.
    """
    log_files_count = len(list(Path.glob(logs_folder, '**/*.log')))
    assert log_files_count == INITIAL_LOG_FILE_COUNT

    clean_up(logs_folder, -1, True)

    log_files_count = len(list(Path.glob(logs_folder, '**/*.log')))
    assert log_files_count == NON_DELETED_FILE_COUNT
