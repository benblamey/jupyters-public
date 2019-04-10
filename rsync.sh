#!/usr/bin/env bash

# rsync --recursive jupyters-public/ldsa-2019 ben-uppmax-haste-spark-master:~/jupyters/jupyters-public/ldsa-2019


if [ -n "$(git status --porcelain)" ]; then
  echo "changes to commit - aborting";
  exit 1
else
  echo "no pending changes - proceeding to rysnc";
fi


rsync --exclude=".*" --delete --recursive ben-uppmax-haste-spark-master:~/jupyters/jupyters-public/ldsa-2019 .

