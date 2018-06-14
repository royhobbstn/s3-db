const AWS = require('aws-sdk');
AWS.config.update({ region: 'us-west-2' });

var lambda = new AWS.Lambda({ apiVersion: '2015-03-31' });

var params = {
  FunctionName: "s3-db-dev-dataupload",
  InvocationType: "Event",
  LogType: "None",
  Payload: "2014_001"
};
lambda.invoke(params, function(err, data) {
  if (err) console.log(err, err.stack); // an error occurred
  else console.log(data); // successful response
});
