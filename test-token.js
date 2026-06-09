import jwt from 'jsonwebtoken';
const token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6ImRyaXZlci0wMDEiLCJyb2xlIjoiZHJpdmVyIn0.LlItUpllcsY5aXlIHt9pH8eI7oRzAc-WJoOcIo0H9NY';
const secret = '60651d89b02641afeea358be4762f0b047ebae446572e906180e6bd1d4ba6ff05bd4341226a414f5f0db15ea6efd54eb98b7e719c5dc8e0f6370d326cbe79b39';
try {
  console.log('Verifying token...');
  const decoded = jwt.verify(token, secret);
  console.log('Decoded:', decoded);
} catch (e) {
  console.error('Error:', e.message);
}
