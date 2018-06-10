# s3-db
Testing S3 as a cost-efficient key-value database.

## Prerequisite

```
sudo yum install -y git
```

Then:

```
node parse-acs-geofiles.js $year
node parse-acs-schemas.js $year
``` 

for your target year (2014, 2015, 2016)

## Main

```
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
screen
bash run-data.sh 2015
```
