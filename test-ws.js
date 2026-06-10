const WebSocket = require('ws');
const ws = new WebSocket('wss://real.shelteric.com/?token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6InJpZGVyLTAwMSIsInJvbGUiOiJyaWRlciJ9.pz5qZubhjBOCuM-BwbaImq21Hfm-4Iu_W4NF3JL2_ig');

ws.on('open', () => {
  console.log('Connected!');
  ws.close();
});
ws.on('error', (err) => {
  console.error('Error:', err);
});
ws.on('close', (code) => {
  console.log('Closed', code);
});
