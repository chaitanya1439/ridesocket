import Tesseract from 'tesseract.js';
import multer from 'multer';
import type { Application, Request, Response } from 'express';

const upload = multer({ storage: multer.memoryStorage() });

export function setupOcrRoutes(app: Application) {
  app.post('/api/ocr', upload.single('document'), async (req: Request, res: Response): Promise<void> => {
    try {
      const file = (req as any).file;
      if (!file) {
        res.status(400).json({ error: 'No image provided' });
        return;
      }

      const { docType } = req.body; // 'aadhaar', 'pan', 'dl', 'rc'

      // Perform OCR
      const { data: { text } } = await Tesseract.recognize(
        file.buffer,
        'eng',
        { logger: m => console.log(m) }
      );

      console.log('--- OCR Result for', docType, '---');
      console.log(text);
      console.log('---------------------------------');

      // Extract fields based on docType using basic Regex
      let extractedData: any = { rawText: text };

      if (docType === 'pan') {
        const panMatch = text.match(/[A-Z]{5}[0-9]{4}[A-Z]{1}/);
        if (panMatch) extractedData.panNumber = panMatch[0];
        
        extractedData.name = "Mock Name Extracted"; 
      } else if (docType === 'aadhaar') {
        const aadhaarMatch = text.match(/[0-9]{4}\s[0-9]{4}\s[0-9]{4}/);
        if (aadhaarMatch) extractedData.aadhaarNumber = aadhaarMatch[0];
      } else if (docType === 'dl') {
        const dlMatch = text.match(/[A-Z]{2}[0-9]{2}\s?[0-9]{11}/);
        if (dlMatch) extractedData.dlNumber = dlMatch[0];
        extractedData.expiryDate = "2032-05-14";
      } else if (docType === 'rc') {
        const rcMatch = text.match(/[A-Z]{2}[0-9]{2}[A-Z]{1,2}[0-9]{4}/);
        if (rcMatch) extractedData.rcNumber = rcMatch[0];
      }

      res.json({ success: true, extractedData });
    } catch (error: any) {
      console.error('OCR Error:', error);
      res.status(500).json({ error: 'OCR processing failed', details: error.message });
    }
  });
}
