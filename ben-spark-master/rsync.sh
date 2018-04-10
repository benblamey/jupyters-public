if [ -n "$(git status --porcelain)" ]; then
  echo "changes to commit - aborting";
  exit 1
else
  echo "no pending changes - proceeding to rysnc";
fi

rsync --delete -r ben-spark-master:~/jupyter .
