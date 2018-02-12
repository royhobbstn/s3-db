const fs = require('fs');
const mkdirp = require('mkdirp');
const glob = require('glob');
const Promise = require('bluebird');
const zlib = require('zlib');

const OUTPUT = './output';
const ROOT = './outputSync/';

const LAST_CHAR_FILTER = process.argv[2];


fs.readdir(`${OUTPUT}`, (err, files) => {
    if (err) {
        console.log('error: ', err);
        process.exit();
    }

    // TODO filter out files by criteria for multithreading

    const aggregate_prefixes = getAggregatePrefixes(files);

    const filtered = filterByLastCharacter(aggregate_prefixes, LAST_CHAR_FILTER);

    console.log('aggregated prefixes');

    console.log('number of prefixes: ' + aggregate_prefixes.length);
    console.log('number of filtered: ' + filtered.length);

    makeNeededDirectories(filtered);

    console.log('made directories');

    aggregateJson(filtered).then((filenames) => {
        console.log(`${filenames.length} files written`);
        console.log('ready to sync to S3');
    });

});

function filterByLastCharacter(aggregate_prefixes, last_char) {
    if (!last_char) {
        return aggregate_prefixes;
    }

    return aggregate_prefixes.filter(prefix => {
        return prefix.slice(-1) === last_char;
    });
}


function getAggregatePrefixes(files) {
    //
    const split_filenames = files.map((file) => {
        return file.split('!')[0];
    });

    return Array.from(new Set(split_filenames));
}

function makeNeededDirectories(prefixes) {
    // take all prefixes, remove cluster portion to get a list of all directories needed

    const remove_cluster = prefixes.map(prefix => {
        return prefix.split('-').slice(0, -1).join('-');
    });

    const uniques = Array.from(new Set(remove_cluster));

    console.log('making ' + uniques.length + ' new directories');

    uniques.forEach(prefix => {

        const split_path = prefix.split('-');

        const attr = split_path[0];
        const sumlev = split_path[1];

        mkdirp.sync(`${ROOT}${attr}/${sumlev}`);
    });
}

function aggregateJson(aggregate_prefixes) {
    //
    console.log('begin aggregateJSON');

    let i = 0;

    return Promise.map(aggregate_prefixes, (prefix) => {

        i++;

        console.log('prefix: ' + prefix, ((i / aggregate_prefixes.length) * 100).toFixed(2) + "%");

        // work on one prefix at a time to avoid loading too much data into memory

        //return new Promise((resolve, reject) => {
        // create a list of all files that match the prefix pattern (a glob of files)

        glob(`${OUTPUT}/${prefix}*`, {}, function(err, files) {
            if (err) {
                console.log(err);
                //return reject(err);
            }

            // for each file in the glob, read it, parse it as a JSON object
            const file_text = files.map(file => {
                return new Promise((resolve, reject) => {
                    fs.readFile(`${file}`, (err, data) => {

                        if (err) {
                            console.log(err);
                            return reject(err);
                        }
                        resolve(JSON.parse(data));
                    });
                });
            });

            return Promise.all(file_text).then(json => {
                // merge all JSON from the glob together
                const merged_json = Object.assign({}, ...json);

                // re-derive final name from bundle entry
                const destination_path = files[0]
                    .replace(OUTPUT, ROOT.slice(0, -1))
                    .split('-')
                    .join('/')
                    .split('!')[0] + '.json';

                zlib.gzip(JSON.stringify(merged_json), function(error, result) {
                    if (error) {
                        console.log(error);
                        // return reject(error);
                    }

                    fs.writeFile(destination_path, result, err => {
                        if (err) {
                            console.log(err);
                            //return reject(err);
                        }
                        //return resolve(destination_path);
                    });

                });

            });

        });

        //});

    }, { concurrency: 100 });

}
