const { google } = require('googleapis');
const fs = require('fs');
const path = require('path');

const sheets = google.sheets('v4');

const GOOGLE_SHEETS_CREDS = JSON.parse(process.env.GOOGLE_SHEETS_CREDS);

async function authorize() {
    const jwtClient = new google.auth.JWT(
        GOOGLE_SHEETS_CREDS.client_email,
        null,
        GOOGLE_SHEETS_CREDS.private_key,
        ['https://www.googleapis.com/auth/spreadsheets']
    );
    await jwtClient.authorize();
    return jwtClient;
}

async function getSheetNameByGID(sheetId, gid) {
    const spreadsheetData = await sheets.spreadsheets.get({
        auth: auth,
        spreadsheetId: sheetId
    });

    for (let sheet of spreadsheetData.data.sheets) {
        if (sheet.properties.sheetId.toString() === gid.toString()) {
            return sheet.properties.title;
        }
    }
    throw new Error(`No sheet found with gid ${gid}`);
}

async function fetchGoogleSheet(sheetId, gid) {
    const sheetName = await getSheetNameByGID(sheetId, gid);

    const sheetData = await sheets.spreadsheets.values.get({
        auth: auth,
        spreadsheetId: sheetId,
        range: sheetName
    });
    return sheetData.data.values;
}

async function fetchAndSaveDataset(project) {
    for (let dataset of project.rawDataTables) {
        const data = await fetchGoogleSheet(project.sheetId, dataset.gid);
        const csv = data.map(row => row.join(",")).join("\n");
        fs.writeFileSync(path.join(project.id, `${dataset.gid}.csv`), csv);
    }
}

async function main() {
    const auth = await authorize();
    const db = JSON.parse(fs.readFileSync('temp_database.json', 'utf8'));

    for (let project of db) {
        if (!fs.existsSync(project.id)) {
            fs.mkdirSync(project.id);
        }
        await fetchAndSaveDataset(project);
    }
}

main().catch(error => {
    console.error('Error:', error);
});
