const fs = require('fs');
const path = require('path');

const url = process.env.CHAT_SERVER_URL || null;
const outPath = path.join(process.cwd(), 'public', 'config.js');
const content = `window.CHAT_SERVER_URL = ${JSON.stringify(url)};\n`;

fs.writeFileSync(outPath, content, 'utf8');
console.log(`Wrote ${outPath} with CHAT_SERVER_URL=${url}`);
