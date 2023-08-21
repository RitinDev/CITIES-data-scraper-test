const fs = require('fs');
const axios = require('axios');
const { google } = require('googleapis');

const fetchDataFromGithub = async (url) => {
    try {
        const response = await axios.get(url);
        return response.data;
    } catch (error) {
        console.error(`GitHub Data Fetch Error: ${error.response.statusText}`);
        return [];
    }
};

const getSheetNameByGid = async (sheetId, gid, apiKey) => {
    try {
        const sheets = google.sheets({ version: 'v4' });
        const response = await sheets.spreadsheets.get({
            key: apiKey,
            spreadsheetId: sheetId,
            fields: 'sheets(properties(sheetId,title))',
        });

        const sheet = response.data.sheets.find(sheet => sheet.properties.sheetId === parseInt(gid));
        return sheet ? sheet.properties.title : null;
    } catch (error) {
        console.error(`Error retrieving sheet name for GID ${gid}: ${error.message}`);
        return null;
    }
};

const fetchDataFromGoogleSheet = async (sheetId, gid, apiKey) => {
    const sheetName = await getSheetNameByGid(sheetId, gid, apiKey);
    if (!sheetName) {
        console.error(`Sheet with GID ${gid} not found in spreadsheet ${sheetId}`);
        return { sheetName: null, data: [] };
    }

    try {
        const sheets = google.sheets({ version: 'v4' });
        const response = await sheets.spreadsheets.values.get({
            key: apiKey,
            spreadsheetId: sheetId,
            range: sheetName,
        });
        return { sheetName, data: response.data.values || [] };
    } catch (error) {
        console.error(`Error fetching data for GID ${gid} from spreadsheet ${sheetId}: ${error.message}`);
        return { sheetName: null, data: [] };
    }
};


const arrayToCSV = (data) => {
    return data.map(row => row.join(',')).join('\n');
};

const saveDataToCSV = (data, path) => {
    if (data.length === 0) {
        console.warn(`No data to write to ${path}`);
        return;
    }

    if (!fs.existsSync(path) || fs.readFileSync(path, 'utf-8') !== data) {
        fs.writeFileSync(path, data);
    }
};

const main = async (apiKey, databaseUrl) => {
    const database = await fetchDataFromGithub(databaseUrl);

    for (const project of database) {
        const projectPath = `./${project.id}`;
        if (!fs.existsSync(projectPath)) {
            fs.mkdirSync(projectPath);
        }

        for (const dataset of project.rawDataTables) {
            const { sheetName, data } = await fetchDataFromGoogleSheet(project.sheetId, dataset.gid, apiKey);
            const csvData = arrayToCSV(data);
            if (!sheetName) {
                saveDataToCSV(csvData, `${projectPath}/${project.id}-${dataset.gid}.csv`);
            } else {
                const sanitizedSheetName = sheetName.replace(/[^a-zA-Z0-9-_]/g, "_");  // To ensure the sheet name doesn't contain invalid characters for filenames
                saveDataToCSV(csvData, `${projectPath}/${project.id}-${sanitizedSheetName}-${dataset.gid}.csv`);
            }
        }
        // Metadata logic here
        // Create a file metadata.json and write the metadata for each sheet to it
        // const metadata = {
        //     id: project.id,
        //     name: project.name,
        //     description: project.description,
        //     rawDataTables: project.rawDataTables,
        //     metadataTables: project.metadataTables,
        // };
        // saveDataToCSV(JSON.stringify(metadata), `${projectPath}/metadata.json`);

    }
};

const SHEETS_API_KEY = 'AIzaSyCjVRS9swFZFN8FQq9ChM0FHWb_kRc0LCI';
const TEMP_DATABASE_URL = 'https://raw.githubusercontent.com/CITIES-Dashboard/cities-dashboard.github.io/main/frontend/src/temp_database.json';
main(SHEETS_API_KEY, TEMP_DATABASE_URL).catch(console.error);

