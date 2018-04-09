if [ -n "$(git status --porcelain)" ]; then
  echo "there are changes";
  exit 1
else
  echo "no changes";
fi

rsync --delete -r ben-spark-master:~/jupyter .
