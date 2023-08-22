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

const getCSVFileSize = (filePath) => {
    const stats = fs.statSync(filePath);
    return (stats.size / 1024).toFixed(2); // Size in kilobytes with 2 decimal places
};

const main = async (apiKey, databaseUrl) => {
    const database = await fetchDataFromGithub(databaseUrl);
    let metadata = {};

    // Load the existing metadata
    if (fs.existsSync('./datasets_metadata.json')) {
        metadata = JSON.parse(fs.readFileSync('./datasets_metadata.json', 'utf-8'));
    }

    for (const project of database) {
        // Skip projects with no public datasets
        if (!project.rawDataTables ||
            project.rawDataTables.length === 0 ||
            Object.keys(project.rawDataTables[0]).length === 0) continue;

        const projectPath = `./${project.id}`;
        if (!fs.existsSync(projectPath)) {
            fs.mkdirSync(projectPath);
        }

        const projectMetadata = {};
        for (const dataset of project.rawDataTables) {
            const { sheetName, data } = await fetchDataFromGoogleSheet(project.sheetId, dataset.gid, apiKey);
            const csvData = arrayToCSV(data);
            let fileName;

            if (!sheetName) {
                fileName = `${project.id}-data.csv`;
            } else {
                const sanitizedSheetName = sheetName.replace(/[^a-zA-Z0-9-_]/g, "_");
                fileName = `${project.id}-${sanitizedSheetName}.csv`;
            }

            const filePath = `${projectPath}/${fileName}`;
            const oldData = fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf-8') : null;
            saveDataToCSV(csvData, filePath);

            const rawLink = `https://github.com/RitinDev/CITIES-data-scraper-test/blob/main/${project.id}/${fileName}`;
            const size = getCSVFileSize(filePath);

            let sheetTitle = sheetName || "data";
            let existingMetadata = projectMetadata[sheetTitle];
            if (!existingMetadata) {
                existingMetadata = {
                    rawLink,
                    lastModified: new Date().toISOString().split('T')[0], // Only YYYY-MM-DD format
                    size
                };
                projectMetadata[sheetTitle] = existingMetadata;
            }

            // Update the lastModified only if there's a change in the data
            if (oldData !== csvData) {
                existingMetadata.lastModified = new Date().toISOString().split('T')[0];
            }

            existingMetadata.size = size + " KB";
        }

        metadata[project.id] = projectMetadata;
    }

    // Save metadata to JSON
    fs.writeFileSync('./datasets_metadata.json', JSON.stringify(metadata, null, 2));
};

const SHEETS_API_KEY = process.env.SHEETS_NEW_API_KEY;
const TEMP_DATABASE_URL = 'https://raw.githubusercontent.com/CITIES-Dashboard/cities-dashboard.github.io/main/frontend/src/temp_database.json';
main(SHEETS_API_KEY, TEMP_DATABASE_URL).catch(console.error);

