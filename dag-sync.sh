#!/bin/bash
# DAG sync script used in production to update the repo files when invoked.
# This can be run using cron at the desired DAG sync interval.
#
# Inputs:
#   - The first and only argument to the script should be the Slack hook URL target
#     for the output sync message.
set -e

SLACK_URL=$1
# https://stackoverflow.com/a/246128 via Dave Dopson CC BY-SA 4.0
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd "$SCRIPT_DIR"

# Get current commit hash
current=$(git rev-parse HEAD)
# Pull repo
git pull --rebase
# Get new commit hash after pull
new=$(git rev-parse HEAD)
# Check if the hash has changed, if not quit early
[ $current = $new ] && exit
subject=$(git log -1 --format='%s')


if [ -z "$SLACK_URL" ]; then
  echo "Slack hook was not supplied! Updates will not be posted"
else
  curl
    $SLACK_URL \
    -X POST \
    -H 'Content-Type: application/json' \
    -d '{"text":"Deployed: '"$subject"'","username":"DAG Sync","icon_emoji":":recycle:","blocks":[{"type":"section","text":{"type":"mrkdwn","text":"Deployed: <https://github.com/WordPress/openverse-catalog/commit/'"$new"'|'"$subject"'>"}}]}'
fi
