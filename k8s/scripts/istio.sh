#!/bin/bash
set -e

echo "Running main: $@"
"$@"
status=$?

echo "Command exited with code $status"

if [ $status -eq 0 ]; then
  echo "Triggering Istio shutdown"
  curl -s -X POST http://127.0.0.1:15000/quitquitquit
else
  echo "Skipping Istio shutdown due to failure"
fi

exit $status
