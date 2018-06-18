const AWS = require('aws-sdk');
const S3 = new AWS.S3;
const { dataset } = require('./modules/settings.js');
const Promise = require('bluebird');
const states = require('./modules/states');

loadPrototype();

const YEAR = '2015';

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
      lambda_invocations.push({ seq: String(i).padStart(3, '0'), geo, state });
    });
  });
}

// check bucket for existing

const all_keys = [];

let still_keys_left = true;


const s3_params = {
  Bucket: `s3db-acs-raw-${dataset[YEAR].text}`
};


S3.listObjectsV2(s3_params, function(err, data) {
  if (err) {
    console.log(err, err.stack);
    process.exit();
  }
  else {
    console.log(data.length);
    if (data.length < 1000) {
      still_keys_left = false;
    }
    all_keys.push(data);

  }
});


// remove existing from list of possible

// run like normal

// todo check only mode
/*

const invoked = Promise.map(lambda_invocations, (instance) => {

  return new Promise((resolve, reject) => {

    let lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

    const params = {
      FunctionName: "s3-db-dev-dataupload",
      InvocationType: "Event",
      LogType: "None",
      Payload: JSON.stringify({
        'year': YEAR,
        'seq': instance.seq,
        'geo': instance.geo,
        'state': instance.state
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

*/

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
