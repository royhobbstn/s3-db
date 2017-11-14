"use strict";

const AWS = require('aws-sdk');
const s3 = new AWS.S3();

var appRouter = function (app) {


    app.get("/test", function (req, res) {
        return res.send('test');
    });

    /*****************/

    app.get("/get", function (req, res) {

        const geoids = req.query.geoids.split(',');
        const expression = req.query.expression;

        const seq_list = getSeqNumFromExpression();
        const file_list = getKeyFromGeoid(geoids);

        function getKeyFromGeoid(geoids) {
            return geoids.map(d => {
                // state = 2 characters (state[2])
                // county = 5 characters (state[2]|county[3])
                // place - 7 characters (state[2]|place[5])
                // tract = 11 characters (state[2]|county[3]|tract[6])
                // bg = 12 characters (state[2]|county[3]|tract[6]|bg[1])

                // return proper s3 file, keeping in mind geographies
                // have often been aggregated up a level (or two)
                // to cut down on number of requests
                const len = d.length;
                switch (len) {
                case 2:
                    return `/040/00.json`;
                case 5:
                    return `/050/${d.slice(0,2)}`;
                case 7:
                    return `/160/${d.slice(0,2)}`;
                case 11:
                    return `/140/${d.slice(0,5)}`;
                case 12:
                    return `/150/${d.slice(0,5)}`;
                default:
                    console.error(`unexpected geoid: ${d}`);
                    return '';
                }

            });
        }

        function getSeqNumFromExpression() {
            //
        }


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


// could (will) run into issues, maximum memory usage on lambda is 1.5gb

// parse all incoming, create a list of key's to query
// parse expressions, determine seq file to use
// get all data from list of keys - combine
// extract only relevant data from POSTed GEOIDs
// apply formula to retrieve only the statistics needed
// assembled return object
// as POST
