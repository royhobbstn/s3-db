// run once
// data from https://www2.census.gov/programs-surveys/acs/summary_file/2015/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt
// output in reference/acs1115_table2seq.json

const fs = require('fs');
const csv = require('csvtojson');
const path = require('path');

const seq_obj = {};

csv()
    .fromFile(path.join(__dirname, `acs_doc/ACS1115_5yr_Seq_Table_Number_Lookup.txt`))
    .on('json', (jsonObj) => {
        seq_obj[jsonObj['Table ID']] = jsonObj['Sequence Number'].slice(-3);
    })
    .on('done', (error) => {
        if (error) {
            return console.log(error);
        }

        const content = JSON.stringify(seq_obj);

        fs.writeFile("./reference/acs1115_table2seq.json", content, 'utf8', function (err) {
            if (err) {
                return console.log(err);
            }
            console.log("The file was saved!");
        });
    });
