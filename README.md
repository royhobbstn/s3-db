# s3-db
Testing S3 as a cost-efficient key-value database.

Use on Amazon Linux.

## Prerequisite

```
sudo yum install -y git
```

## Main

```
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
```

Then:

```
node parse-acs-geofiles.js $year
node parse-acs-schemas.js $year
``` 

for your target year (2014, 2015, 2016)

```
screen
bash run-data.sh 2015
```
