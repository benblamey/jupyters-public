Jupyter notebooks from different machines, including Apache Spark teaching examples

Ben Blamey


Note to self

```
Fetch:
rsync -vv --dry-run --recursive --delete benblamey-ldsa-2020:~/jupyters-public/ldsa-2020 ldsa-2020
rsync -vv --dry-run --recursive --delete benblamey-ldsa-2020:~/jupyters-public/ldsa-2020 .
rsync -vv --recursive --delete benblamey-2021:~/jupyters-public/de-2021 .

Send:
rsync -vv --dry-run --recursive --delete de-2021 benblamey-2021:~/jupyters-public/de-2021
```
