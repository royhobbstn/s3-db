const fs = require('fs');
const rimraf = require('rimraf');
const mkdirp = require('mkdirp');
const glob = require('glob');
const Promise = require('bluebird');
const zlib = require('zlib');

const OUTPUT = '../output';
const ROOT = '../outputSync/';


rimraf.sync(`${ROOT}`);
fs.mkdirSync(`${ROOT}`);


fs.readdir(`${OUTPUT}`, (err, files) => {
    if (err) {
        console.log('error: ', err);
        process.exit();
    }

    const aggregate_prefixes = getAggregatePrefixes(files);

    makeNeededDirectories(aggregate_prefixes);

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
    //
    const remove_cluster = prefixes.map(prefix => {
        return prefix.split('-').slice(0, -1).join('-');
    });

    Array.from(new Set(remove_cluster)).forEach(prefix => {

        const split_path = prefix.split('-');

        const attr = split_path[0];
        const sumlev = split_path[1];

        mkdirp.sync(`${ROOT}${attr}/${sumlev}`);
    });
}

function aggregateJson(aggregate_prefixes) {
    //

    const aggregate_bundles = getAggregateBundles(aggregate_prefixes);

    return Promise.all(aggregate_bundles).then(bundles => {
        //
        return Promise.map(bundles, bundle => {
            return aggregateJsonFiles(bundle);
        }, { concurrency: 5 });
    }).catch(err => {
        // catch all promise chain errors here and end program
        console.log(err);
        process.exit();
    });

}

function getAggregateBundles(aggregate_prefixes) {
    //
    return aggregate_prefixes.map(prefix => {
        //
        return new Promise((resolve, reject) => {
            //
            glob(`${OUTPUT}/${prefix}*`, {}, function(err, files) {
                if (err) {
                    console.log(err);
                    return reject(err);
                }
                resolve(files);
            });
            //
        });
    });
}

function aggregateJsonFiles(bundle) {

    const file_text = bundle.map(file => {
        return new Promise((resolve, reject) => {
            fs.readFile(`${OUTPUT}/${file}`, (err, data) => {

                if (err) {
                    console.log(err);
                    return reject(err);
                }
                resolve(JSON.parse(data));
            });
        });
    });

    return Promise.all(file_text).then(json => {
        // merge all JSON together
        const merged_json = Object.assign({}, ...json);

        // re-derive final name from bundle entry
        const destination_path = bundle[0]
            .replace(OUTPUT, ROOT.slice(0, -1))
            .split('-')
            .join('/')
            .split('!')[0] + '.json';

        console.log(destination_path);

        return new Promise((resolve, reject) => {
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


}
