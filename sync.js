const { google } = require('googleapis');
const fetch = require('node-fetch');
const fs = require('fs');
const path = require('path');

const REPO_URL = 'https://raw.githubusercontent.com/CITIES-Dashboard/cities-dashboard.github.io/main/frontend/src/temp_database.json';

async function fetchProjects() {
  const response = await fetch(REPO_URL);
  if (!response.ok) {
    throw new Error('Failed to fetch projects JSON');
  }
  return await response.json();
}

async function authenticate() {
  const jwt = new google.auth.JWT(
    process.env.GOOGLE_CLIENT_EMAIL,
    null,
    process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'), 
    ['https://www.googleapis.com/auth/spreadsheets']
  );
  await jwt.authorize();
  return jwt;
}

async function fetchDataFromSheet(jwt, spreadsheetId, gid) {
  const sheets = google.sheets({ version: 'v4', auth: jwt });
  const { data } = await sheets.spreadsheets.values.get({
    spreadsheetId: spreadsheetId,
    range: `'${gid}'!A:Z`, 
  });
  return data.values;
}

(async () => {
  try {
    const projects = await fetchProjects();
    
    const jwt = await authenticate();

    for (let project of projects) {
      // Continue with your logic to handle each project's data...
      const data = await fetchDataFromSheet(jwt, project.sheetId, project.gid);
      // Continue with your logic to save each dataset to your repo...
    }
  } catch (error) {
    console.error(`Error: ${error.message}`);
  }
})();
