require('dotenv').config();
const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const PORT = process.env.WEBHOOK_PORT || 3000;
const SHEETDB_URL = 'https://sheetdb.io/api/v1/my6bx0lb6c50k';

/**
 * Formcord Webhook Endpoint
 */
app.post('/formcord', async (req, res) => {
    try {
        console.log('📩 Formcord webhook received:', JSON.stringify(req.body, null, 2));

        const fields = req.body?.fields || [];

        // Convert fields array into key/value map
        const formData = {};
        for (const field of fields) {
            if (field.label && field.value) {
                formData[field.label.trim()] = field.value.trim();
            }
        }

        const username = formData["Game Username"] || '';
        let coords = formData["Coords"] || '';
        const title = formData["Title"] || '';

        if (!username) {
            console.log('⚠ Username missing, ignoring submission.');
            return res.status(400).send('Missing username');
        }

        // Normalize coords
        coords = coords.replace(/[-,]/g, ':');

        // Date / Time (UTC)
        const now = new Date();
        const day = now.toLocaleDateString('en-GB'); // dd/mm/yyyy
        const timeUTC = now.toISOString().slice(11, 16); // hh:mm

        const sheetData = {
            data: [
                {
                    Day: day,
                    "Time (UTC)": timeUTC,
                    "Reservations (UTC)": '',
                    Alliance: '',
                    Username: username,
                    Coords: coords,
                    Title: title,
                    Guardian: '',
                    Done: '',
                    "Set title": ''
                }
            ]
        };

        console.log('📤 Sending to SheetDB:', sheetData);

        await axios.post(SHEETDB_URL, sheetData);

        console.log('✅ Submission saved to SheetDB');
        res.status(200).send('OK');

    } catch (error) {
        console.error('❌ Webhook error:', error.response?.data || error);
        res.status(500).send('Server error');
    }
});

app.listen(PORT, () => {
    console.log(`🚀 Formcord webhook listening on port ${PORT}`);
});
