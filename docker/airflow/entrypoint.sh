#!/bin/bash

set -e

function help_text() {
  cat << 'END'

  ,-.  ;-.  ,--. .  . .   , ,--. ,-.   ,-.  ,--.
 /   \ |  ) |    |\ | |  /  |    |  ) (   ` |
 |   | |-'  |-   | \| | /   |-   |-<   `-.  |-
 \   / |    |    |  | |/    |    |  \ .   ) |
  `-'  '    `--' '  ' '     `--' '  '  `-'  `--'

Docker entrypoint script for Openverse Airflow. Unless specified, all commands
will wait for the database to be ready and will upgrade the Airflow schema.

Usage:
  help - print this help text and exit
  init - create an admin user account before starting the scheduler+webserver
  server - start the scheduler+webserver
  (anything else) - run the command provided
END
}


function header() {
  size=${COLUMNS:-80}
  # Print centered text between two dividers of length $size
  printf '#%.0s' $(seq 1 $size) && echo
  printf "%*s\n" $(( (${#1} + size) / 2)) "$1"
  printf '#%.0s' $(seq 1 $size) && echo
}

if [ "$1" == help ] || [ "$1" == --help ]; then help_text && exit 0; fi
sleep 0.1;  # The $COLUMNS variable takes a moment to populate

# Reformat Airflow connections that use https
header "MODIFYING ENVIRONMENT"
# Loop through environment variables, relying on naming conventions.
# Bash loops with pipes occur in a subprocess, so we need to do some special
# subprocess manipulation via <(...) syntax to allow the `export` calls
# to propagate to the outer shell.
# See: https://unix.stackexchange.com/a/402752
while read var_string; do
    # get the variable name
    var_name=`expr "$var_string" : '^\([A-Z_]*\)'`
    echo "Variable Name: $var_name"
    # get the old value
    old_value=`expr "$var_string" : '^[A-Z_]*=\(.*\)$'`
    echo "    Old Value: $old_value"
    # get the http clause, with http or https included (assume that http only appears once)
    http_clause=`expr "$old_value" : '.*\(http.*\)'`
    # if http_clause starts with http, then replace http with https
    url_encoded="${http_clause/"http:"/"https:"}"
    # call python to url encode the http clause
    url_encoded=`python -c"from urllib.parse import quote_plus; import sys; print(quote_plus(sys.argv[1]))" $url_encoded`
    # prepend https://
    url_encoded='https://'$url_encoded
    # replace the original http_clause with the url_encoded version
    new_value=${old_value/"$http_clause"/"$url_encoded"}
    echo "    New Value: $new_value"
    # set the environment variable
    export $var_name=$new_value
# only include airflow connections with http somewhere in the string
done < <(env | grep "^AIRFLOW_CONN[A-Z_]\+=.*http.*$")

# Wait for postgres
header "WAITING FOR POSTGRES"
python /opt/airflow/wait_for_db.py
# Upgrade the database -- command is idempotent.
header "MIGRATING DATABASE"
airflow db upgrade
if [[ "$1" == "init" ]]; then
  header "CREATING ADMIN USER"
  airflow users create -r Admin -u airflow -f Air -l Flow -p airflow --email airflow@example.org
fi

case "$1" in
  init|server|"")
    # Start scheduler and webserver in same container
    header "STARTING AIRFLOW"
    airflow scheduler &
    exec airflow webserver
    ;;
  *)
    # The command is something like bash, not an airflow subcommand. Just run it in the right environment.
    header "RUNNING \"$*\""
    exec "$@"
    ;;
esac
