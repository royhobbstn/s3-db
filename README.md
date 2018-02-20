# s3-db
Testing S3 as a cost-efficient key-value database.

Uses nodeJS cluster module for multiple threads.  Use the biggest machine possible for fastest load times.


```
sudo yum install -y git
git clone https://github.com/royhobbstn/s3-db.git
cd s3-db
screen
bash run-data.sh 2015
```

For EMFILE errors for having too many files open at once:

https://stackoverflow.com/a/11345256/8896489
