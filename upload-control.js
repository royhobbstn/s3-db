const AWS = require('aws-sdk');
const S3 = new AWS.S3;
const { dataset } = require('./modules/settings.js');
const Promise = require('bluebird');
const states = require('./modules/states');
// **CREDENTIALS** const aws_credentials = require('./aws_key.json');
const argv = require('yargs').argv;
const readline = require('readline');


if (argv._.length === 0) {
  console.log('fatal error.  Run like: node upload-control.js 2015');
  process.exit();
}

const YEAR = argv._[0];


loadPrototype();


/****/

AWS.config.update({
  region: 'us-west-2',
  maxRetries: 0,
  retryDelayOptions: {
    base: 10000
  }
});

/****/

const settings = dataset[YEAR];
const seq_count = parseInt(settings.seq_files, 10);

// all possible combinations
const lambda_invocations = [];

for (let i = 1; i <= seq_count; i++) {
  ['allgeo', 'trbg'].forEach(geo => {
    Object.keys(states).forEach(state => {
      const seq = String(i).padStart(3, '0');
      lambda_invocations.push({ seq, geo, state, name: `e${YEAR}5${state}0${seq}000_${geo}`, type: 'e' });
      lambda_invocations.push({ seq, geo, state, name: `m${YEAR}5${state}0${seq}000_${geo}`, type: 'm' });
    });
  });
}

// check bucket for existing

const s3_bucket = `s3db-acs-raw-${dataset[YEAR].text}`;

console.log(`Reading keys from bucket: ${s3_bucket}`);

// **CREDENTIALS if not on an Amazon Instance
const listAll = require('s3-list-all')( /*{ accessKeyId: aws_credentials.accessKeyId, secretAccessKey: aws_credentials.secretAccessKey }*/ );

listAll({ Bucket: s3_bucket, Prefix: '' }, function(err, results) {

  if (err) {
    console.log(err);
    process.exit();
  }

  console.log(`Found ${results.length} keys.`);

  const keys = results.map(d => d.Key.replace('.csv', ''));

  // filter out existing from possible
  const remaining = lambda_invocations.filter(opt => {
    return !(keys.includes(opt.name));
  });

  console.log(`Missing ${remaining.length} keys.`);

  // reduce down to file level by removing the MOE records
  const files_to_retrieve = remaining.filter(item => item.type === 'e');

  console.log(`There are ${files_to_retrieve.length} files left to upload.`);

  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  rl.question('Continue y/n ? ', (answer) => {
    rl.close();

    if (answer !== 'y') {
      process.exit();
    }

    const invoked = Promise.map(files_to_retrieve, (file) => {

      return new Promise((resolve, reject) => {

        let lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

        const params = {
          FunctionName: "s3-db-dev-dataupload",
          InvocationType: "Event",
          LogType: "None",
          Payload: JSON.stringify({
            'year': YEAR,
            'seq': file.seq,
            'geo': file.geo,
            'state': file.state
          })
        };
        lambda.invoke(params, function(err, data) {
          if (err) {
            console.log(err, err.stack);
            return reject(err);
          }
          else {
            console.log(data);
            return resolve(data);
          }
        });

      });

    }, { concurrency: 1 });


    Promise.all(invoked).then(() => {
        console.log('all lambdas invoked');
      })
      .catch(err => {
        console.log(err);
        console.log('something bad happened');
        process.exit();
      });

  });

});


/*****************/

function loadPrototype() {
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
}
