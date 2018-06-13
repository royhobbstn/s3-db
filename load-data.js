var request = require('request');
var unzipper = require('unzipper');

unzipper.Open.url(request, 'https://www2.census.gov/programs-surveys/acs/summary_file/2014/data/1_year_seq_by_state/Alaska/20141ak0001000.zip')
  .then(function(d) {
    var buffers = d.files.map(function(file) {
      console.log(file.path);
      return file.buffer();
    })[0];
    return buffers;
  })
  .then(function(d) {
    console.log(d.toString());
  });
