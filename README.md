# s3-db
Testing S3 as a read-only database with potentially thousands of simultaneous queries.

Requires Node 8 or higher (async await)

Use a big machine!  (tested on r4.xlarge - failed at seq-m35.  Try larger instance.)

```
node --max-old-space-size=8192 direct_to_s3.js
```