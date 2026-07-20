import { WebSocket } from 'ws';
const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImRyaXZlci0wMDEiLCJyb2xlIjoiZHJpdmVyIn0.LlItUpllcsY5aXlIHt9pH8eI7oRzAc-WJoOcIo0H9NY';
const ws = new WebSocket(`ws://localhost:8080?token=${token}`);
ws.on('open', () => {
    console.log('Driver connected!');
    ws.send(JSON.stringify({ type: 'auth', role: 'driver', id: 'D001' }));
    setTimeout(() => {
        ws.send(JSON.stringify({ type: 'driver_status', status: 'available' }));
        console.log('Sent driver_status: available');
    }, 1000);
});
ws.on('message', (msg) => {
    console.log('Received:', msg.toString());
});
//# sourceMappingURL=test-driver.js.map