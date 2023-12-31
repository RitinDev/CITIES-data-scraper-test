const fs = require('fs');
const axios = require('axios');
const { google } = require('googleapis');
const crypto = require('crypto');

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

const computeHash = (data) => {
    const hash = crypto.createHash('sha256');
    hash.update(data);
    return hash.digest('hex');
};

const getCSVFileSize = (filePath) => {
    const stats = fs.statSync(filePath);
    return stats.size;
};

const main = async (apiKey, databaseUrl, currentCommit) => {
    const database = await fetchDataFromGithub(databaseUrl);
    let metadata = {};

    if (fs.existsSync('./datasets_metadata.json')) {
        metadata = JSON.parse(fs.readFileSync('./datasets_metadata.json', 'utf-8'));
    }

    for (const project of database) {
        if (!project.rawDataTables ||
            project.rawDataTables.length === 0 ||
            Object.keys(project.rawDataTables[0]).length === 0) continue;


        const projectPath = `./${project.id}`;
        if (!fs.existsSync(projectPath)) {
            fs.mkdirSync(projectPath);
        }

        const projectMetadata = metadata[project.id] || [];

        for (const dataset of project.rawDataTables) {
            const { sheetName, data } = await fetchDataFromGoogleSheet(project.sheetId, dataset.gid, apiKey);
            const csvData = arrayToCSV(data);

            let fileName;
            let sanitizedSheetName = "data"; // default
            if (sheetName) {
                sanitizedSheetName = sheetName.toLowerCase().replace(/ /g, "-").replace(/[^a-z0-9-]/g, "_");
                fileName = `${project.id}-${sanitizedSheetName}.csv`;
            } else {
                fileName = `${project.id}-data.csv`;
            }

            const filePath = `${projectPath}/${fileName}`;
            const oldCSVData = fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf-8') : "";
            const oldHash = computeHash(oldCSVData);
            const newHash = computeHash(csvData);

            if (oldHash !== newHash) {
                fs.writeFileSync(filePath, csvData);

                const rawLinkLatest = `https://raw.githubusercontent.com/RitinDev/CITIES-data-scraper-test/main/${project.id}/${fileName}`;
                const currentCommitRawLink = `https://raw.githubusercontent.com/RitinDev/CITIES-data-scraper-test/${currentCommit}/${project.id}/${fileName}`;
                const size = getCSVFileSize(filePath);
                const currentVersion = {
                    name: sanitizedSheetName,
                    rawLink: rawLinkLatest,
                    version: new Date().toISOString().split('T')[0], // Only YYYY-MM-DD format
                    sizeInBytes: size
                };

                let datasetEntry = projectMetadata.find(entry => entry.id === dataset.gid.toString());

                if (!datasetEntry) {
                    datasetEntry = {
                        id: dataset.gid.toString(),
                        versions: []
                    };
                    projectMetadata.push(datasetEntry);
                }

                // If there's a previous version, update its rawLink to include the commit hash
                if (datasetEntry.versions.length > 0) {
                    datasetEntry.versions[0].rawLink = currentCommitRawLink;
                }

                // Prepend the new version
                datasetEntry.versions.unshift(currentVersion);
            }
        }

        metadata[project.id] = projectMetadata;
    }

    fs.writeFileSync('./datasets_metadata.json', JSON.stringify(metadata, null, 2));
};

const SHEETS_API_KEY = process.env.SHEETS_NEW_API_KEY;
const TEMP_DATABASE_URL = 'https://raw.githubusercontent.com/CITIES-Dashboard/cities-dashboard.github.io/main/frontend/src/temp_database.json';
const CURRENT_COMMIT_HASH = process.env.CURRENT_COMMIT;
main(SHEETS_API_KEY, TEMP_DATABASE_URL, CURRENT_COMMIT_HASH).catch(console.error);
