import argparse
import logging
import shutil
from datetime import timedelta, datetime
from pathlib import Path

logging.basicConfig(
    format='%(asctime)s: [%(levelname)s - Log cleanup] %(message)s',
    level=logging.DEBUG)

logger = logging.getLogger(__name__)

DEFAULT_MAX_LOG_AGE_IN_DAYS = 1


def is_older_than_cutoff(
        file_or_folder: Path, cutoff: int = DEFAULT_MAX_LOG_AGE_IN_DAYS):
    last_modified = file_or_folder.stat().st_mtime
    cutoff_time = datetime.now() - timedelta(days=cutoff)

    return datetime.fromtimestamp(last_modified) <= cutoff_time


def dir_size_in_mb(dir_path):
    return sum(f.stat().st_size for f in dir_path.glob('**/*')
               if f.is_file()) / (1024 * 1024)


def get_folders_to_delete(dag_log_folder, MAX_LOG_AGE_IN_DAYS):
    task_log_folders = [_ for _ in Path.iterdir(dag_log_folder)
                        if Path.is_dir(_)]
    folders_to_delete = []
    for task_log_folder in task_log_folders:
        run_log_folders_to_delete = [
            folder for folder in Path.iterdir(task_log_folder)
            if (Path.is_dir(folder)
                and is_older_than_cutoff(folder, MAX_LOG_AGE_IN_DAYS))
        ]
        folders_to_delete.extend(run_log_folders_to_delete)
    return folders_to_delete


def clean_up(BASE_LOG_FOLDER, MAX_LOG_AGE_IN_DAYS, ENABLE_DELETE):
    log_base = Path(BASE_LOG_FOLDER)
    logger.info(f"Cleaning up log files, \n"
                f"BASE_LOG_FOLDER: {BASE_LOG_FOLDER}\n"
                f"MAX_LOG_AGE_IN_DAYS: {MAX_LOG_AGE_IN_DAYS},"
                f"\nENABLE_DELETE: {ENABLE_DELETE}")
    log_folders = [item for item in Path.iterdir(log_base)
                   if Path.is_dir(item)]
    size_before = dir_size_in_mb(log_base)
    folders_to_delete = []
    for dag_log_folder in log_folders:
        if dag_log_folder.name == 'dag_processor_manager':
            continue
        elif dag_log_folder.name == 'scheduler':
            # Scheduler creates a folder for each date, and keeps
            # schedule logs for each dag in a separate file
            scheduler_log_folders_to_delete = [
                date_log_dir
                for date_log_dir in Path.iterdir(dag_log_folder)
                if (Path.is_dir(date_log_dir)
                    and not Path.is_symlink(date_log_dir)
                    and is_older_than_cutoff(
                            date_log_dir, MAX_LOG_AGE_IN_DAYS
                        ))
            ]
            folders_to_delete.extend(scheduler_log_folders_to_delete)
        else:
            task_log_folders_to_delete = get_folders_to_delete(
                dag_log_folder, MAX_LOG_AGE_IN_DAYS
            )
            folders_to_delete.extend(task_log_folders_to_delete)

    if ENABLE_DELETE:
        for dag_log_folder in folders_to_delete:
            if 'python_airflow_log_cleanup' not in str(dag_log_folder):
                logger.info(f"Deleting {dag_log_folder}")
                shutil.rmtree(dag_log_folder)
        size_after = dir_size_in_mb(log_base)
        logger.info(f"Deleted {len(folders_to_delete)} folders. "
                    f"Log directory size before: {size_before:.2f}MB, "
                    f"after: {size_after:.2f} MB")
    else:
        size_to_delete = sum(dir_size_in_mb(folder)
                             for folder in folders_to_delete)
        logger.info(f"Found {len(folders_to_delete)} log folders to delete. "
                    f"Run this DAG with ENABLE_DELETE set to True "
                    f"to free {size_to_delete:.2f} MB.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Log Cleanup Job',
        add_help=True
    )
    parser.add_argument(
        '--maxLogAgeInDays',
        help='Logs older than maxLogAgeInDays days will be deleted. '
             'Default is 7.')
    args = parser.parse_args()
    if args.maxLogAgeInDays:
        max_log_age_in_days = args.maxLogAgeInDays
    else:
        max_log_age_in_days = 7
    base_log_folder = Path(__file__).parent / 'test_resources' / 'logs'
    Path.mkdir(base_log_folder, parents=True, exist_ok=True)
    clean_up(base_log_folder, max_log_age_in_days, True)
