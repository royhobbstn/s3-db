const fs = require('fs');
const mkdirp = require('mkdirp');
const glob = require('glob');
const Promise = require('bluebird');
const zlib = require('zlib');

const OUTPUT = './output';
const ROOT = './outputSync/';


fs.readdir(`${OUTPUT}`, (err, files) => {
    if (err) {
        console.log('error: ', err);
        process.exit();
    }

    const aggregate_prefixes = getAggregatePrefixes(files);

    console.log('aggregated prefixes');
    console.log('number of prefixes: ' + aggregate_prefixes.length);

    makeNeededDirectories(aggregate_prefixes);

    console.log('made directories');

    aggregateJson(aggregate_prefixes).then((filenames) => {
        console.log(`${filenames.length} files written`);
        console.log('ready to sync to S3');
    });

});



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


    return Promise.map(aggregate_prefixes, prefix => {

        console.log('prefix: ' + prefix);

        // work on one prefix at a time to avoid loading too much data into memory

        return new Promise((resolve, reject) => {
            // create a list of all files that match the prefix pattern (a glob of files)

            glob(`${OUTPUT}/${prefix}*`, {}, function(err, files) {
                if (err) {
                    console.log(err);
                    return reject(err);
                }

                console.log('glob: ', files);

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

                Promise.all(file_text).then(json => {
                    // merge all JSON from the glob together
                    const merged_json = Object.assign({}, ...json);

                    // re-derive final name from bundle entry
                    const destination_path = files[0]
                        .replace(OUTPUT, ROOT.slice(0, -1))
                        .split('-')
                        .join('/')
                        .split('!')[0] + '.json';

                    // combined data will be stored at this path
                    console.log(destination_path);

                    zlib.gzip(JSON.stringify(merged_json), function(error, result) {
                        if (error) {
                            console.log(error);
                            return reject(error);
                        }

                        fs.writeFile(destination_path, result, err => {
                            if (err) {
                                console.log(err);
                                return reject(err);
                            }
                            return resolve(destination_path);
                        });

                    });

                });

            });

        });

    }, { concurrency: 200 });

}
