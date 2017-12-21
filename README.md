# s3-db
Testing S3 as a read-only database with potentially thousands of simultaneous queries.

Requires Node 8 or higher (async await)

Use a big machine!  (try r4.2xlarge)

```
node --max-old-space-size=16384 direct_to_s3.js
```