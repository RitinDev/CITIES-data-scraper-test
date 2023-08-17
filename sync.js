const fs = require('fs');
const axios = require('axios');
const { GoogleSpreadsheet } = require('google-spreadsheet');

// Configuration constants
const REPO_URL = 'https://raw.githubusercontent.com/CITIES-Dashboard/cities-dashboard.github.io/main/frontend/src/temp_database.json';

async function fetchDatabase() {
    try {
        const response = await axios.get(REPO_URL);
        return response.data;
    } catch (error) {
        console.error('Error fetching the temp_database.json:', error);
        throw error;
    }
}

async function fetchDataFromSheet(sheetId, gid) {
    // Initialize the sheets API client
    const doc = new GoogleSpreadsheet(sheetId);
    await doc.useServiceAccountAuth(JSON.parse(process.env.GOOGLE_SHEETS_CREDS));
    await doc.loadInfo();

    // Assume that the sheet's title can be fetched using the gid.
    const sheet = doc.sheetsById[gid];
    const rows = await sheet.getRows();
    return rows.map(row => row._rawData);
}

(async () => {
    try {
        const database = await fetchDatabase();

        for (const project of database) {
            if (project.rawDataTables) {
                for (const dataset of project.rawDataTables) {
                    const data = await fetchDataFromSheet(project.sheetId, dataset.gid);
                    const csv = data.map(row => row.join(',')).join('\n');

                    // Save the CSV to a file.
                    // You can further enhance this to save in desired directories or filenames
                    fs.writeFileSync(`./${project.id}-${dataset.gid}.csv`, csv);
                }
            }
        }

    } catch (error) {
        console.error('Error:', error);
    }
})();
