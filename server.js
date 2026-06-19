const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

// ✅ CONFIG
//const KAFKA_REST_URL = "http://cp-kafka-rest.railway.internal:8082";
//const API_KEY = "my-secret-key";

const KAFKA_REST_URL = process.env.KAFKA_REST_URL;
const API_KEY = process.env.API_KEY;


// ✅ ROOT (health check)
app.get('/', (req, res) => {
    res.send("Kafka Gateway running using private network✅");
});

// ✅ PRODUCER ENDPOINT
app.post('/topics/:topic', async (req, res) => {

    const apiKey = req.headers['x-api-key'];

    // ✅ AUTH CHECK
    if (!apiKey || apiKey !== API_KEY) {
        return res.status(401).json({ error: "Unauthorized" });
    }

    try {

        const response = await axios.post(
            `${KAFKA_REST_URL}/topics/${req.params.topic}`,
            req.body,
            {
                headers: {
                    "Content-Type": "application/vnd.kafka.json.v2+json"
                }
            }
        );

        res.json(response.data);

    } catch (err) {

        res.status(500).json({
            error: "Kafka call failed",
            details: err.response?.data || err.message
        });

    }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Gateway running on port " + PORT + "key:" + API_KEY));
