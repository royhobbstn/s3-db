# s3-db
S3 as a cost-efficient database.

## Why?

1. Database data storage is (relatively) expensive.
2. S3 is really cheap!
3. I can load all the data for a dataset in < 30 minutes using AWS lambda (and that's a conservative estimate).  Previous naive automated attemps into PostgreSQL took half a day at best!  More previous manual attempts took about a week.

## How

Data is processed w/ (mostly) serverless pipeline.  

First Lambda (dataupload.js, controlled by upload-control.js), loads data to a staging bucket in the cloud.
Second Lambda (dataparse.js, controlled by parse-control.js), extracts data to s3 bucket.

## Notes

This will blow through your Lambda free tier credits and probably rack up a few dollars of charges (<$5).  But running hundreds of concurrent processes is fun so it might be worth it to you.


## Setup

Assumes NodeJS 8+, NPM.
Serverless via the [Serverless Framework](https://serverless.com/)

```
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
npm install
serverless deploy

```

If not using an Amazon Cloud machine of some sort, you may need to set up a ```aws_key.json``` file in the same format as ```aws_key.example.js```.

You'll then need to uncomment the lines in parse-control.js and upload-control.js marked **CREDENTIALS**.

# Instructions

## Populate Metadata

Populate metadata bucket (prerequisite for running data):

```
node parse-acs-geofiles.js $year
node parse-acs-schemas.js $year
``` 

where ```$year``` is one of (2014, 2015, 2016)


## Upload

Step one is to upload Census Data into a Cloud staging bucket.

```
node upload-control $year
```

Step two is to parse that data into the desired format.

```
node parse-control $year
```

## Notes

Bucket names are hardcoded (sorry).