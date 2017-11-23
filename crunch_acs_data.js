#!/usr/bin/env node

const argv = require('yargs').argv;
const states = require('./modules/states');
const Promise = require('bluebird');
const request = require('request');
const fs = require('fs');
const unzip = require('unzip');

// if no states entered as parameters, use all states
const loop_states = argv._.length ? argv._ : Object.keys(states);

console.log(`Selected: ${loop_states}`);

// selected states array into array of objects to facilitate downloading multiple files
let selected_states = [];
loop_states.forEach(state => {
    selected_states.push({ state, isTractBGFile: true });
    selected_states.push({ state, isTractBGFile: false });
});

// TODO logic to make directories

// run up to 5 downloads concurrently
Promise.map(selected_states, function (state) {
    return requestAndSaveStateFileFromACS(state.state, state.isTractBGFile);
}, { concurrency: 5 }).then(d => {
    console.log(`downloaded: ${d.length} files from Census.`);
    console.log("Finished");
}).catch(err => {
    console.log(err);
    process.exit(); // exit immediately upon error
});



function requestAndSaveStateFileFromACS(abbr, isTractBGFile) {
    return new Promise((resolve, reject) => {
        const fileName = states[abbr];
        const fileType = isTractBGFile ? '_Tracts_Block_Groups_Only' : '_All_Geographies_Not_Tracts_Block_Groups';
        const outputDir = isTractBGFile ? 'CensusDL/group1/' : 'CensusDL/group2/';
        const fileUrl = `https://www2.census.gov/programs-surveys/acs/summary_file/2015/data/5_year_by_state/${fileName}${fileType}.zip`;
        const outputFile = `${states[abbr]}${fileType}.zip`;

        console.log(`downloading ${fileName}${fileType}.zip`);
        request({ url: fileUrl, encoding: null }, function (err, resp, body) {
            if (err) { return reject(err); }
            fs.writeFile(`${outputDir}${outputFile}`, body, function (err) {
                if (err) { return reject(err); }
                console.log(`${outputFile} written!`);

                // unzip
                const stream = fs.createReadStream(`${outputDir}${outputFile}`);
                stream.pipe(unzip.Extract({ path: `${outputDir}_unzipped` })
                    .on('close', function () {
                        console.log(`${outputFile} unzipped!`);
                        resolve('done unzip');
                    })
                    .on('error', function (err) {
                        reject(err);
                    })
                );
            });
        });
    });
}




/*

echo "creating temporary directories"
mkdir staged
mkdir combined
mkdir sorted

mkdir geostaged
mkdir geocombined
mkdir geosorted

mkdir joined

echo "processing all files: not tracts and bgs"
cd group1
for file in *20155**0***000.txt ; do mv $file ${file//.txt/a.csv} ; done
for i in *20155**0***000*.csv; do echo "writing p_$i"; while IFS=, read f1 f2 f3 f4 f5 f6; do echo "$f3$f6,"; done < $i > p_$i; done
mv p_* ../staged/

echo "processing all files: tracts and bgs"
cd ../group2
for file in *20155**0***000.txt ; do mv $file ${file//.txt/b.csv} ; done
for i in *20155**0***000*.csv; do echo "writing p_$i"; while IFS=, read f1 f2 f3 f4 f5 f6; do echo "$f3$f6,"; done < $i > p_$i; done
mv p_* ../staged/


cd ../staged
echo "combining tract and bg files with all other geographies: estimates"
for i in $(seq -f "%03g" 1 122); do cat p_e20155**0"$i"000*.csv >> eseq"$i".csv; done;
echo "combining tract and bg files with all other geographies: margin of error"
for i in $(seq -f "%03g" 1 122); do cat p_m20155**0"$i"000*.csv >> mseq"$i".csv; done;
mv *seq* ../combined/

cd ../combined

echo "replacing suppressed fields with null"
for file in *.csv; do perl -pi -e 's/\.,/,/g' $file; done;

for file in *.csv; do echo "sorting $file"; sort $file > ../sorted/$file; done;

echo "creating geography key file"
cd ../group1
for file in g20155**.csv; do mv $file ../geostaged/$file; done;

cd ../geostaged
for file in *.csv; do cat $file >> ../geocombined/geo_concat.csv; done;

cd ../geocombined
awk -F "\"*,\"*" '{print $2 $5}' geo_concat.csv > geo_key.csv
tr A-Z a-z < geo_key.csv > geo_key_lowercase.csv
paste -d , geo_key_lowercase.csv geo_concat.csv > acs_geo_2015.csv
sort acs_geo_2015.csv > ../geosorted/acs_geo_2015.csv

cd ../sorted
for file in *.csv; do echo "joining $file with geography"; join -t , -1 1 -2 1 ../geosorted/acs_geo_2015.csv ./$file > ../joined/$file; done;

echo "files joined"

cd ../joined

for file in *.csv; do sed -i 's/,$//' $file; done;

echo "removed trailing comma csv"

cd ..

mkdir schemas

echo "downloading master column list"
curl --progress-bar https://www2.census.gov/programs-surveys/acs/summary_file/2015/documentation/user_tools/ACS_5yr_Seq_Table_Number_Lookup.txt -O

echo "creating schema files"

# remove first line
sed 1d ACS_5yr_Seq_Table_Number_Lookup.txt > no_header.csv

# only copy actual column entries - no metadata
# columns only have integer values in field 4
awk -F, '$4 ~ /^[0-9]+$/' no_header.csv > columns_list.csv

# create a schema file for each sequence file.  kickstart it with geography fields
n=122;for i in $(seq -f "%04g" ${n});do echo -n "KEY,FILEID,STUSAB,SUMLEVEL,COMPONENT,LOGRECNO,US,REGION,DIVISION,STATECE,STATE,COUNTY,COUSUB,PLACE,TRACT,BLKGRP,CONCIT,AIANHH,AIANHHFP,AIHHTLI,AITSCE,AITS,ANRC,CBSA,CSA,METDIV,MACC,MEMI,NECTA,CNECTA,NECTADIV,UA,BLANK1,CDCURR,SLDU,SLDL,BLANK2,BLANK3,ZCTA5,SUBMCD,SDELM,SDSEC,SDUNI,UR,PCI,BLANK4,BLANK5,PUMA5,BLANK6,GEOID,NAME,BTTR,BTBG,BLANK7" > "./schemas/schema$i.txt"; done;

# loop through master column list, add each valid column to its schema file as type float
while IFS=',' read f1 f2 f3 f4 f5; do echo -n ","`printf $f2`"_"`printf %03d $f4` >> "./schemas/schema$f3.txt"; done < columns_list.csv;

cd schemas

for file in *.txt; do sed -i -e '$a\' $file; done;

echo "schema files created"

cd ..

mkdir ready

cd ./joined

echo "prepend file with header"
# prepend each file with corresponding header
for file in *.csv; do cat ../schemas/schema0${file:4:3}.txt $file > ../ready/$file; done;

cd ../ready

echo "remove trailing newlines"
# remove trailing newline to prevent csv parsing warnings in nodejs
for file in *.csv; do perl -i -pe 'chomp if eof' $file; done;

echo "done"
# files we need are in ready folder

*/
