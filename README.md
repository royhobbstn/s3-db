# s3-db
Testing S3 as a read-only database with potentially hundreds of simultaneous queries.

Requires Node 8 or higher (async await)

Use a big machine!  (try something with 8 cores and 32GB RAM)


```
sudo yum install -y git
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
bash run-data.sh 2015
```

For EMFILE errors for having too many files open at once:

https://stackoverflow.com/a/11345256/8896489
