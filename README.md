# s3-db
Testing S3 as a cost-efficient key-value database.

Data processed w/ (mostly) serverless pipeline.


## Main

Assumes NodeJS 8+, NPM.
Serverless via the [Serverless Framework](https://serverless.com/)

```
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
serverless deploy

```

Then create an ```aws_key.json``` file in the same format as ```aws_key.example.js```.


# Metadata

Populate metadata bucket (prerequisite for running data):

```
node parse-acs-geofiles.js $year
node parse-acs-schemas.js $year
``` 

where ```$year``` is one of (2014, 2015, 2016)


# Upload

Step one is to upload Census Data into a Cloud staging bucket.

```
upload-control $year
```

Step two is to parse that data into the desired format.

```
parse-control $year
```

# Notes

Bucket names are hardcoded (sorry).