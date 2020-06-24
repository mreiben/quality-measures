const fs = require('fs');
const util = require('util');
const readDirAsync = util.promisify(fs.readdir);
const readFileAsync = util.promisify(fs.readFile);

async function main() {
    let fileNames;
    try {
        fileNames = await readDirAsync('./schemas');
    } catch (err) {
        console.log('Unable to fetch schema file names: ', err);
    }
    for (const fileName of fileNames) {
        // remove file extension
        const name = fileName.substring(0, fileName.length - 4);
        let schemaData, textData;
        try {
            schemaData = await readFileAsync(`./schemas/${name}.csv`, 'utf8');
            textData = await readFileAsync(`./data/${name}.txt`, 'utf8');
        } catch (err) {
            console.log(`Unable to fetch schema file with name ${fileName}`, err);
        }
        const parsedSchema = parseSchema(schemaData);
        console.log('schema: ', parsedSchema);
        const parsedData = parseData(textData, parsedSchema);
        console.log('data: ', parsedData);
    }

}

function parseSchema(csvText) {
    const fields = csvText.split('\n').reduce(function(acc, v){
        if (v !== '') {
            const [name, width, dataType] = v.split(',');
            const row = { name, width: parseInt(width), dataType }
            acc.push(row);
        }
        return acc;
    }, []);
    return fields;
}


function parseData(dataText, schema) {
    const rows = dataText.match(/.+/g);
    const formattedData = [];
    let idx = 0;
    rows.forEach((row) => {
        const formattedRow = schema.reduce(function(acc, measure){
            const { name, width, dataType } = measure;
            const val = row.substring(idx, idx + width).trim();
            let formattedVal;
            switch (dataType) {
                case 'INTEGER':
                    formattedVal = parseInt(val);
                    break;
                case 'BOOLEAN':
                    formattedVal = !!parseInt(val);
                    break;
                default:
                    formattedVal = val;
            }
            acc[name] = formattedVal;
            idx += width;
            return acc;
        }, {});
        formattedData.push(formattedRow);
        idx = 0;
    });
    return formattedData;
}

main();

// Program flow:


// parseSchema() helper method to parse schema, takes .csv data -> [{ key: 'measure_id', length: 10, dataType: 'TEXT'}, {...}, ...]

// parseData() helper method to parse data using schema, takes .txt -> [{ measure_id: 'IA_PCMH', performance_year: 2017, is_required: true, minimum_score: 0}]

// main:
// -- identify pairs of data/schema file names

// -- for each pair:

// -- -- parseSchema(), then parseData() using the parsed schema

// -- -- make post request to https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/NavaInterview/measures.
//       with resulting json blobs

// IMPORTANT CONSIDERATIONS:
// - log error when shema/data file names don't have a match
// - log error when data doesn't match schema shape
//    - when rows don't have enough data in terms of length as specified in schema
//    - when specified data types such as INTEGER can't be parsed from a row of data
// - log error when post request is not successfull
// - set limits for size of payloads?