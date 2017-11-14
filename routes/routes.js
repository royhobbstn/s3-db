"use strict";

const AWS = require('aws-sdk');
const s3 = new AWS.S3();

var appRouter = function (app) {

    app.get("/test", function (req, res) {
        return res.send('test');
    });

    app.get("/get", function (req, res) {

        getS3Data('106/040/00.json')
            .then(data => {
                return res.json(data);
            })
            .catch(err => {
                return res.send(err);
            });


    });

};

module.exports = appRouter;

function getS3Data(Key) {
    const Bucket = 's3db-acs1115';

    return new Promise((resolve, reject) => {
        s3.getObject({ Bucket, Key }, function (err, data) {
            if (err) {
                console.log(err, err.stack);
                return reject(err);
            }
            const object_data = JSON.parse(data.Body.toString('utf-8'));
            return resolve(object_data);
        });
    });
}


// parse all incoming, create a list of key's to query
// get all data from list of keys - combine
// extract only relevant data from POSTed GEOIDs
// apply formula to retrieve only the statistics needed
// assembled return object
// as POST
