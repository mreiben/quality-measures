const fs = require('fs');
const util = require('util');
const readDirAsync = util.promisify(fs.readdir);
const readFileAsync = util.promisify(fs.readFile);
const axios = require('axios');
const URL = 'https://2swdepm0wa.execute-api.us-east-1.amazonaws.com/prod/NavaInterview/measures';

async function main() {
    let fileNames;
    try {
        // read schemas directory to find file names
        fileNames = await readDirAsync('./schemas');
    } catch (err) {
        console.error('Unable to fetch schema file names: ', err);
    }
    for (const fileName of fileNames) {
        // remove file extension
        const name = fileName.substring(0, fileName.length - 4);
        let schemaData, textData;
        try {
            // read schema and text file data
            schemaData = await readFileAsync(`./schemas/${name}.csv`, 'utf8');
            textData = await readFileAsync(`./data/${name}.txt`, 'utf8');
        } catch (err) {
            console.error(`Unable to fetch schema file with name ${fileName}`, err);
        }

        // parse schema from .csv
        const parsedSchema = parseSchema(schemaData);
        // parse data using parsedSchema
        const parsedData = parseData(textData, parsedSchema);
        for (const data of parsedData) {
            // send POST request with data blob as body
            axios.post(URL, data).then((res) => {
                console.log('Post status: ', res.status);
            }, (err) => {
                console.warn(`Error sending data with measure_id ${data.measure_id} from ${name}.txt`, err);
            });
        }
    }
}

/** Takes raw text from a csv schema file and returns an array of objects representing the schema
 * @ param {String}    csvText A string with rows representing schema fields
 * @ return {Array<{name: String, width: Number, dataType: String}}
 */
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

/** Takes raw data from a text file and returns an array of objects matching the schema
 * @ param {String}    dataText A string with rows representing records
 * @ param {Array<{name: String, width: Number, dataType: String}}  schema  Matches output from #parseSchema
 * @ return {Object[]}  An array of objects representing the rows from dataText parsed based on the schema
 */
function parseData(dataText, schema) {
    const rows = dataText.match(/.+/g);
    const formattedData = [];
    let idx = 0;
    for (row of rows) {
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
    }
    return formattedData;
}

main();

// POSSIBLE EXTENSIONS:
// - handle error when schema/data file names don't have a match
// - handle error when data doesn't match schema shape
//    - when rows don't have enough data in terms of length as specified in schema
//    - when specified data types such as INTEGER can't be parsed from a row of data