const request = require('requestretry');
const csv = require('csvtojson');
const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const argv = require('yargs').argv;
const { dataset } = require('./modules/settings.js');

if (argv._.length === 0) {
  console.log('fatal error.  Run like: node parse-acs-schemas.js 2015');
  process.exit();
}

const YEAR = argv._[0];


const url = `https://www2.census.gov/programs-surveys/acs/summary_file/${YEAR}/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt`;
request(url, function(err, resp, body) {
  if (err) { return console.log(err); }

  csv({ noheader: false })
    .fromString(body)
    .on('end_parsed', data => {

      const fields = {};
      // filter out line number if non-integer value
      data.forEach(d => {
        const line_number = Number(d['Line Number']);
        if (Number.isInteger(line_number) && line_number > 0) {
          const field_name = d['Table ID'] + String(d['Line Number']).padStart(3, "0");
          const seq_num = d['Sequence Number'].slice(1);
          if (fields[seq_num]) {
            fields[seq_num].push(field_name);
          }
          else {
            fields[seq_num] = ["FILEID", "FILETYPE", "STUSAB", "CHARITER", "SEQUENCE", "LOGRECNO", field_name];
          }
        }
      });

      const myBucket = `s3db-acs-metadata-${dataset[YEAR].text}`;
      const key = `s${dataset[YEAR].text}.json`;

      return new Promise((resolve, reject) => {
        const params = { Bucket: myBucket, Key: key, Body: JSON.stringify(fields), ContentType: 'application/json' };
        s3.putObject(params, function(err, data) {
          if (err) {
            console.log(err);
            return reject(err);
          }
          else {
            console.log(`Successfully uploaded data to ${myBucket} - ${key}`);
            return resolve(data);
          }
        });
      });

    })
    .on('done', () => {
      //parsing finished
      console.log('finished parsing schema file');
    });
});



/*******************/



// https://github.com/uxitten/polyfill/blob/master/string.polyfill.js
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/String/padStart
if (!String.prototype.padStart) {
  String.prototype.padStart = function padStart(targetLength, padString) {
    targetLength = targetLength >> 0; //truncate if number or convert non-number to 0;
    padString = String((typeof padString !== 'undefined' ? padString : ' '));
    if (this.length > targetLength) {
      return String(this);
    }
    else {
      targetLength = targetLength - this.length;
      if (targetLength > padString.length) {
        padString += padString.repeat(targetLength / padString.length); //append to original to ensure we are longer than needed
      }
      return padString.slice(0, targetLength) + String(this);
    }
  };
}
