const express = require('express');
const app = express();
const port = process.env.PORT || 3001;

app.use(express.json());

app.get('/health', (req, res) => res.send('OK'));

app.listen(port, () => console.log(`Webhook Service listening on port ${port}!`));
