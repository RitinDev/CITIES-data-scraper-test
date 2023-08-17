const fs = require('fs');
const fetch = require('node-fetch');
const { google } = require('googleapis');

const SHEETS_API_KEY = 'AIzaSyCjVRS9swFZFN8FQq9ChM0FHWb_kRc0LCI'; // Ensure you set this as a secret in your GitHub repo
const TEMP_DATABASE_URL = 'https://raw.githubusercontent.com/CITIES-Dashboard/cities-dashboard.github.io/main/frontend/src/temp_database.json';

const fetchDataFromGithub = async () => {
    const response = await fetch(TEMP_DATABASE_URL);
    if (!response.ok) {
        throw new Error(`GitHub Data Fetch Error: ${response.statusText}`);
    }
    return await response.json();
};

const getSheetNameByGid = async (sheetId, gid) => {
    const sheets = google.sheets({ version: 'v4' });
    const response = await sheets.spreadsheets.get({
        key: SHEETS_API_KEY,
        spreadsheetId: sheetId,
        fields: 'sheets(properties(sheetId,title))',
    });

    const sheet = response.data.sheets.find(sheet => sheet.properties.sheetId === parseInt(gid));
    return sheet ? sheet.properties.title : null;
};

const fetchDataFromGoogleSheet = async (sheetId, gid) => {
    const sheetName = await getSheetNameByGid(sheetId, gid);
    if (!sheetName) {
        throw new Error(`Sheet with GID ${gid} not found in spreadsheet ${sheetId}`);
    }

    const sheets = google.sheets({ version: 'v4' });
    const response = await sheets.spreadsheets.values.get({
        key: SHEETS_API_KEY,
        spreadsheetId: sheetId,
        range: sheetName,
    });
    return response.data.values;
};

const arrayToCSV = (data) => {
    return data.map(row => row.join(',')).join('\n');
};

const main = async () => {
    const database = await fetchDataFromGithub();

    for (const project of database) {
        const projectPath = `./${project.id}`;
        if (!fs.existsSync(projectPath)) {
            fs.mkdirSync(projectPath);
        }

        for (const dataset of project.rawDataTables) {
            const data = await fetchDataFromGoogleSheet(project.sheetId, dataset.gid);
            const csv = arrayToCSV(data);

            const filePath = `${projectPath}/${project.id}-${dataset.gid}.csv`;
            if (!fs.existsSync(filePath) || fs.readFileSync(filePath, 'utf-8') !== csv) {
                fs.writeFileSync(filePath, csv);
            }
        }

        // Add metadata logic here
        // Here, you would create/update metadata files (e.g., `datasets_metadata.json`)
        // to track each CSV's raw link, date, size, etc.
    }
};

main().catch(console.error);
