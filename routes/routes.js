"use strict";

const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const table2seq = require('../reference/acs1115_table2seq.json');

var appRouter = function (app) {


    app.get("/test", function (req, res) {
        return res.send('test');
    });

    /*****************/

    // curl -d '{"geoids":["08031", "08005"], "expression":["(", "B01001003", "+", "B01001004", "+", "B01001027", "+", "B01001028", ")", "/", "B01001001"]}' -H "Content-Type: application/json" -X POST https://nodejs-server-royhobbstn.c9users.io/get
    app.post("/get", function (req, res) {

        // ['08031', '08005']
        const geoids = req.body.geoids;

        // ["(", "B01001003", "+", "B01001004", "+", "B01001027", "+", "B01001028", ")", "/", "B01001001"]
        const expression = req.body.expression;

        const fields = Array.from(new Set(getFieldsFromExpression(expression)));
        const table_list = getSeqNumFromExpression(expression);
        const raw_seq_list = table_list.map(d => {
            return table2seq[d];
        });
        const seq_list = Array.from(new Set(raw_seq_list));
        if (seq_list.length > 1) {
            console.error('can not use data from more than one sequence file');
            process.exit();
        }

        const file_list = Array.from(new Set(getKeyFromGeoid(geoids)));
        console.log(fields); // [ 'B01001003', 'B01001004', 'B01001027', 'B01001028', 'B00001001' ]
        console.log(file_list); // [ '/050/08' ]
        console.log(seq_list); // [ '001' ] - design decision... only grab from one seq

        let paths = [];

        seq_list.forEach(seq => {
            file_list.forEach(file => {
                paths.push(`${seq}${file}`);
            });
        });
        console.log(paths); // [ '002/050/08', '001/050/08' ]

        const returnedData = paths.map(path => {
            return getParsedExpression(path, geoids, fields, expression);
        });

        Promise.all(returnedData).then(results => {
            // reduce to single object
            return res.json(Object.assign({}, ...results));
        }).catch(err => {
            return res.status(500).send(err);
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


// get all data from list of keys - combine
// extract only relevant data from POSTed GEOIDs
// apply formula to retrieve only the statistics needed
// assembled return object






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

function getFieldsFromExpression(expression) {
    //
    return expression.filter(d => {
        return d.length > 1;
    });
}

function getSeqNumFromExpression(expression) {
    //
    const fields = expression.filter(d => {
        return d.length > 1;
    });

    const tables = fields.map(d => {
        return d.slice(0, 6);
    });

    return tables;
}


// an AWS Lambda function

function getParsedExpression(path, geoids, fields, expression) {
    //
    return new Promise((resolve, reject) => {
        //
        const sumlev = path.split('/')[1];

        const Parser = require('expr-eval').Parser;
        const parser = new Parser();
        const expr = parser.parse(expression.join(""));

        getS3Data(path + '.json')
            .then(data => {

                const evaluated = {};

                geoids.forEach(geo_part => {
                    const full_geoid = `${sumlev}00US${geo_part}`;

                    // not all geoids will be in each file.
                    // if they aren't here, their value will be undefined
                    if (data[full_geoid] !== undefined) {
                        const obj = {};
                        fields.forEach(field => {
                            obj[field] = parseFloat(data[full_geoid][field]);
                        });
                        evaluated[full_geoid] = expr.evaluate(obj);
                    }
                });

                resolve(evaluated);
            })
            .catch(err => {
                reject(err);
            });
    });

}
