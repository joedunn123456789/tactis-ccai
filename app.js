const axios = require('axios');
const jwt = require('jsonwebtoken');
const { parse } = require('csv-parse');
const { Storage } = require('@google-cloud/storage');
const { Firestore, FieldValue } = require('@google-cloud/firestore');
const { ContactCenterInsightsClient } = require('@google-cloud/contact-center-insights');
const dotenv = require('dotenv');
dotenv.config();

const express = require('express');
const { createServer } = require('node:http');
const path = require('path');
const cors = require('cors');
const { Server } = require('socket.io');

const { initializeApp } = require('firebase-admin/app');
const { getAuth } = require('firebase-admin/auth');
const firebaseConfig = {
    apiKey: process.env.firebaseAPIKey,
    authDomain: process.env.firebaseAuthDomain,
    projectId: process.env.firebaseProjectID,
    storageBucket: process.env.firebaseStorageBucket,
    messagingSenderId: process.env.firebaseMessagingSenderId,
    appId: process.env.firebaseAppId
};
const firebaseApp = initializeApp(firebaseConfig);

const storage = new Storage();
const db = new Firestore({ projectId: process.env.googleProjectID, databaseId: process.env.databaseId });
const cciClient = new ContactCenterInsightsClient({ projectId: process.env.googleProjectID });

let token = null;
let baseURL = null;
let lastRunDate = null;
const minRunInterval = 31000;   // the CXone API rate limit is 1 call per 30 seconds, so we wait 31 seconds to be safe

const CCAIMaxOperationQueueLength = 120;    // Maximum number of operations in the CCAI Insights queue before waiting
const CCAIOperationQueueWaitTime = 30000;   // Time (in milliseconds) to wait before checking the operation queue again

const bqMaxExportsPerPeriod = 59;            // Maximum number of BigQuery exports allowed per period
const bqExportPeriodLengthInMs = 60000;     // Length of the BigQuery export period in milliseconds

let bqRateLimiterQueuePointer = 0;
let bqRateLimiterQueue = new Array(bqMaxExportsPerPeriod).fill(0);

const desiredTimeZone = 'America/New_York'; // Time zone used to pull CXone reports and for all log timestamps

let shouldContinue = false;

const callStatuses = [
    { code: 'COPY_TO_BUCKET_START', description: '1_COPY_TO_BUCKET_START' },
    { code: 'COPY_TO_BUCKET_COMPLETE', description: '2_COPY_TO_BUCKET_COMPLETE' },
    { code: 'UPLOAD_TO_CCAI', description: '3_UPLOAD_TO_CCAI_START' },
    { code: 'CCAI_PROCESSING_COMPLETE', description: '4_CCAI_PROCESSING_COMPLETE' },
    { code: 'UPLOAD_TO_BIGQUERY', description: '5_UPLOAD_TO_BIGQUERY' }
]


/* #region  Express / socket.io Config */
const port = process.env.PORT || '3000';
const corsOptions = {
    origin: '*',
    optionsSuccessStatus: 200,
};

const app = express();
app.use(cors(corsOptions));
const server = createServer(app);
const io = new Server(server);

io.use((socket, next) => {
    const token = socket.handshake.auth.token;
    if (token) {
        getAuth()
            .verifyIdToken(token)
            .then((decodedToken) => {
                const uid = decodedToken.uid;
                next();
            })
            .catch((error) => {
                next(new Error('Invalid token'));
            });
    } else {
        next(new Error('Invalid token'));
    }
});

io.on('connection', (socket) => {
    if (io) {
        io.emit('serverStatus', shouldContinue ? 'Running' : 'Stopped');
    }

    socket.on('serverControl', (cmd) => {
        if (cmd === 'start') {
            if (shouldContinue) {
                log('Processing server is already running.');
            } else {
                log(`Starting processing server. Time zone set to: ${desiredTimeZone}. All logging will use this time zone.`);
                shouldContinue = true;
                mainLoop();
                io.emit('serverStatus', 'Running');
            }
        } else if (cmd === 'stop') {
            if (shouldContinue) {
                log('Shutting down processing server...');
                shouldContinue = false;
                io.emit('serverStatus', 'Stopping...');
            } else {
                log('Processing server is already stopped.');
            }
        }
    });
});

app.use(express.static(path.join(__dirname, 'public')));

server.listen(port, () => {
    console.log(`Listening on port ${port}.`)
});
/* #endregion */

/* #region  CXone -> CCAI Functions */
async function authenticate() {
    token = null;
    baseURL = null;

    log('[Authentication] Getting token endpoint...');
    const tokenEndpoint = await getTokenEndpoint();
    if (tokenEndpoint) {
        log('[Authentication] Getting access token...');
        token = await getAccessToken(tokenEndpoint);
        if (token) {
            const tenantId = jwt.decode(token).tenantId;
            if (tenantId) {
                log('[Authentication] Performing API discovery...');
                const apiDiscoveryData = await performApiDiscovery(tenantId);
                if (apiDiscoveryData) {
                    baseURL = `https://api-${apiDiscoveryData.area}.${apiDiscoveryData.domain}`;
                }
            }
        }
    }
}

async function getTokenEndpoint() {
    try {
        const url = process.env.issuerURL + '/.well-known/openid-configuration';

        const response = await axios.get(url);

        return response.data.token_endpoint;
    } catch (error) {
        console.error('Error getting auth token endpoint: ', error);
    }
}

async function getAccessToken(tokenEndpoint) {
    try {
        const params = new URLSearchParams(
            {
                'grant_type': 'password',
                'username': process.env.cxoneUsername,
                'password': process.env.cxonePassword,
                'client_id': process.env.client_id,
                'client_secret': process.env.client_secret
            }
        );

        const response = await axios.post(
            tokenEndpoint,
            params
        );

        return response.data.access_token;
    } catch (error) {
        console.error('Error getting auth token: ', error);
    }
}

async function performApiDiscovery(tenantId) {
    try {
        const url = process.env.issuerURL + `/.well-known/cxone-configuration?tenantId=${tenantId}`;

        const response = await axios.get(url);

        return response.data;
    } catch (error) {
        console.error('Error performing API discovery: ', error);
    }
}

async function createJob(startDate, endDate) {
    log(`Creating data extraction job for ${startDate} to ${endDate}...`);
    const now = getCurrentDateInDesiredTimeZone();
    const msSinceLastRun = now - lastRunDate;
    if (msSinceLastRun < minRunInterval) {
        const delay = minRunInterval - msSinceLastRun;
        log(`Waiting ${Math.round(delay / 1000)} seconds before calling Data Extraction API again...`);
        await new Promise(resolve => setTimeout(resolve, delay));
    }

    try {
        let config = {
            headers: {
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        };

        const body = {
            'entityName': 'recording-interaction-metadata',
            'version': '7',
            'startDate': startDate,
            'endDate': endDate
        };

        const url = baseURL + '/data-extraction/v1/jobs';

        let response = await makeHttpPostRequest(url, body, config);

        if (response.status === 401) {
            log('Unauthorized.  Re-authenticating...');
            await authenticate();
            if (token && baseURL) {
                config.headers.Authorization = `Bearer ${token}`;
                response = await makeHttpPostRequest(url, body, config);
            }
        }

        if (response.status !== 202) {
            log(`Error creating data extraction job: ${response.message}`);
            return null;
        }

        await updateLastRunDate()

        return response.data;

    } catch (error) {
        console.error('Error creating data extraction job: ', error);
    }
}

async function getJobStatus(jobID) {
    try {
        let config = {
            headers: {
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        };

        const url = baseURL + `/data-extraction/v1/jobs/${jobID}`;

        let response = await makeHttpGetRequest(url, config);

        if (response.status === 401) {
            log('Unauthorized.  Re-authenticating...');
            await authenticate();
            if (token && baseURL) {
                config.headers.Authorization = `Bearer ${token}`;
                response = await makeHttpGetRequest(url, config);
            }
        }

        if (response.status !== 200) {
            log(`Error getting data extraction job status: ${response.message}`);
            return null;
        }

        return response.data.jobStatus;

    } catch (error) {
        console.error('Error getting data extraction job status: ', error);
    }
}

async function waitForJobCompletion(jobID) {
    log(`Checking job status for job: ${jobID}...`);
    let jobResponse = await getJobStatus(jobID);

    while (jobResponse.status !== 'SUCCEEDED' && jobResponse.status !== 'FAILED' && jobResponse.status !== 'CANCELLED' && jobResponse.status !== 'EXPIRED') {
        log(`Job status: ${jobResponse.status} for job ${jobID}, waiting 5 seconds to try again...`);
        await new Promise(resolve => setTimeout(resolve, 5000));
        jobResponse = await getJobStatus(jobID);
    }

    return jobResponse;
}

async function getInteractionsFromReport(url) {
    try {
        log('Retrieving interaction CSV report...');
        const response = await axios({
            method: 'get',
            url: url,
            responseType: 'stream'
        });

        const processResponse = await new Promise((resolve, reject) => {
            const reportData = [];

            const parser = parse({
                delimiter: ','
            });

            response.data.pipe(parser);

            parser.on('readable', function () {
                let record;
                while ((record = parser.read()) !== null) {
                    reportData.push(record);
                }
            });

            parser.on('error', function (err) {
                reject(err);
            });

            parser.on('end', function () {
                const callIDField = reportData[0].findIndex(field => field.startsWith('ACD Call ID'));
                const startDateField = reportData[0].findIndex(field => field === 'Start Date');
                const recordingStatusField = reportData[0].findIndex(field => field === 'Voice Recording Status');
                const skillField = reportData[0].findIndex(field => field === 'Agent 1 Skills');
                const agentID = reportData[0].findIndex(field => field === 'Agent 1 ACD ID');
                const agentName = reportData[0].findIndex(field => field === 'Agent 1 Name');
                const agentTeam = reportData[0].findIndex(field => field === 'Agent 1 Team');
                const ANI = reportData[0].findIndex(field => field === 'ANI');
                const DNIS = reportData[0].findIndex(field => field === 'DNIS');
                const Disposition = reportData[0].findIndex(field => field === 'Disposition');

                const interactions = [];
                for (let n = 1; n < reportData.length; n++) {
                    const interaction = {
                        callID: reportData[n][callIDField],
                        recordingStatus: reportData[n][recordingStatusField],
                        skill: reportData[n][skillField],
                        startDate: new Date(reportData[n][startDateField]),
                        agentID: reportData[n][agentID],
                        agentName: reportData[n][agentName],
                        agentTeam: reportData[n][agentTeam],
                        ANI: reportData[n][ANI],
                        DNIS: reportData[n][DNIS],
                        Disposition: reportData[n][Disposition]
                    };

                    const coIndex = interactions.findIndex(co => co.callID === reportData[n][callIDField]);
                    if (coIndex === -1) {
                        interactions.push(interaction);
                    } else if (interaction.startDate < interactions[coIndex].startDate) {
                        interactions[coIndex] = interaction;
                    }
                }

                resolve(interactions);
            });
        });

        return processResponse;
    } catch (error) {
        console.error('Error retrieving interaction CSV: ', error);
    }
}

async function processInteractions(interactions) {
    for (const element of interactions) {
        if (!shouldContinue) break;

        if (element.recordingStatus.localeCompare('Success', undefined, { sensitivity: 'base' }) === 0 && shouldContinue) {
            const recordingURLResponse = await getRecordingURL(element.callID);
            if (recordingURLResponse.status === 'OK' && shouldContinue) {
                const copyResponse = await copyRecordingToBucket(recordingURLResponse.recordingURL, element.callID, element.skill);
                if (copyResponse.status === 'OK' && shouldContinue) {
                    await uploadRecordingToCCAI(element, `gs://${process.env.bucketName}/${element.skill}/${element.callID}.mp4`);
                }
            }
        } else {
            log(`Recording for Call ID ${element.callID} is not available.`);
        }
    }
}

async function getRecordingURL(callID) {
    try {
        log(`Getting recording URL for call ID ${callID}...`);
        let config = {
            headers: {
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        };
        const url = baseURL + `/media-playback/v1/contacts?acd-call-id=${callID}&media-type=voice-only&exclude-waveforms=true&exclude-qm-categories=true&isDownload=true`;

        let response = await makeHttpGetRequest(url, config);

        if (response.status === 401) {
            log('Unauthorized.  Re-authenticating...');
            await authenticate();
            if (token && baseURL) {
                config.headers.Authorization = `Bearer ${token}`;
                response = await makeHttpGetRequest(url, config);
            }
        }

        if (response.status !== 200) {
            log(`Error getting recording URL for call ID ${callID}: ${response.message}`);
            return {
                status: 'ERROR',
                callID: callID,
                message: response.message
            };
        }

        return {
            status: 'OK',
            callID: callID,
            recordingURL: response.data.redirectUrl
        };
    } catch (error) {
        let message = `ERROR: Unable to get recording URL for call ID ${callID}: `;
        if (error.response?.status && error.response?.data?.message) {
            message += `[${error.response.status}] ${error.response.data.message}`;
        } else {
            message += JSON.stringify(error);
        }
        log(message);

        return {
            status: 'ERROR',
            callID: callID,
            message: message
        };
    }
}

async function makeHttpGetRequest(url, config) {
    try {
        const response = await axios.get(url, config);

        return {
            status: response.status,
            data: response.data
        }
    } catch (error) {
        return {
            status: error.response.status ? error.response.status : 'ERROR',
            message: error.message ? error.message : error
        }
    }
}

async function makeHttpPostRequest(url, body, config) {
    try {
        const response = await axios.post(url, body, config);

        return {
            status: response.status,
            data: response.data
        };
    } catch (error) {
        return {
            status: error.response.status ? error.response.status : 'ERROR',
            message: error.message ? error.message : error
        };
    }
}

async function copyRecordingToBucket(url, callID, skill) {
    try {
        log(`Copying recording for call ID ${callID} to bucket...`);
        // updateRecordingStatus(callID, 'Starting copy to bucket');
        const response = await axios({
            method: 'get',
            url: url,
            responseType: 'stream'
        });

        const copyResult = await new Promise((resolve, reject) => {
            (async () => {
                const bucketName = process.env.bucketName;
                const destFileName = `${skill}/${callID}.mp4`;

                const recordingsBucket = storage.bucket(bucketName);
                const file = recordingsBucket.file(destFileName);
                const exists = await file.exists();
                if (exists[0]) {
                    resolve(`File ${destFileName} already exists in bucket, skipping.`);
                } else {
                    const bucketStream = file.createWriteStream({ options: { resumable: false } });

                    updateRecordingStatus(callID, 'COPY_TO_BUCKET_START');
                    response.data.pipe(bucketStream)
                        .on('error', function (err) {
                            reject(err);
                        })
                        .on('finish', function () {
                            updateRecordingStatus(callID, 'COPY_TO_BUCKET_COMPLETE');
                            resolve(`Success: Recording ${callID} uploaded to ${skill} folder in ${bucketName} bucket.`);
                        });
                }
            })()
        });

        log(copyResult);

        return {
            status: 'OK',
            message: copyResult
        };
    } catch (error) {
        let message = 'Unable to copy recording for call ID ${callID} to bucket: ';
        if (error.response?.data) {
            message += error.response.data;
        } else {
            message += JSON.stringify(error);
        }

        log(`Error: ${message}`);

        return {
            status: 'ERROR',
            message: message
        };
    }
}

async function uploadRecordingToCCAI(interaction, bucketURI) {
    try {
        await cciClient.getConversation({ name: `projects/${process.env.googleProjectID}/locations/${process.env.ccaiLocation}/conversations/${interaction.callID}` });
        log(`Conversation for Call ID ${interaction.callID} already exists, skipping upload.`);
    } catch (error) {
        if (error.code && error.code === 5) {
            const parent = `projects/${process.env.googleProjectID}/locations/${process.env.ccaiLocation}`;

            const isSpanish = interaction.skill.toLowerCase().includes('spanish');
            const startTime = Firestore.Timestamp.fromDate(new Date(interaction.startDate));

            const labels = {
                callID: interaction.callID
            };
            if (interaction.ANI) {
                labels['ANI'] = interaction.ANI.replace('\\', '').replace('+', '');
            }
            if (interaction.DNIS) {
                labels['DNIS'] = interaction.DNIS.replace('\\', '').replace('+', '');
            }
            if (interaction.skill) {
                labels['skill'] = interaction.skill;
            }
            if (interaction.agentName) {
                labels['agentName'] = interaction.agentName;
            }
            if (interaction.agentTeam) {
                labels['agentTeam'] = interaction.agentTeam;
            }

            const conversation = {
                name: interaction.callID,
                dataSource: {
                    gcsSource: {
                        audioUri: bucketURI
                    }
                },
                startTime: startTime,
                languageCode: isSpanish ? 'es-US' : 'en-US',
                agentId: interaction.agentID,
                callMetadata: {
                    customerChannel: 2,
                    agentChannel: 1
                }
            }

            if (labels) {
                conversation.labels = labels;
            }

            const request = {
                parent: parent,
                conversation: conversation,
                conversationId: interaction.callID,
                speech_config: {
                    speech_recognizer: isSpanish ? process.env.ccaiSpeechRecognizerSpanish : process.env.ccaiSpeechRecognizerEnglish
                }
            };

            try {
                log(`Uploading Call ID ${interaction.callID} to CCAI Insights...`);
                await waitForOperationQueue();
                const [operation] = await cciClient.uploadConversation(request);
                log(`Success: Call ID ${interaction.callID} upload started - operation name: ${operation.name}`);
                updateRecordingStatus(interaction.callID, 'UPLOAD_TO_CCAI');
            } catch (error) {
                message = `Error uploading Call ID ${interaction.callID} to CCAI Insights. Error Code ${error.code}: ${error.details}`;
                log(`Error: ${message}`);

                return {
                    status: 'ERROR',
                    message: message
                };
            }
        } else {
            const message = `Error searching CCAI Insights for Call ID ${interaction.callID}: ${error}`;
            log(message);
            return {
                status: 'ERROR',
                message: message
            }
        }
    }

    return {
        status: 'OK'
    };
}

async function waitForOperationQueue() {
    try {
        // const cciClient = new ContactCenterInsightsClient({ projectId: process.env.googleProjectID });
        let [operations] = await cciClient.operationsClient.listOperations({ name: `projects/${process.env.googleProjectID}/locations/${process.env.ccaiLocation}`, filter: 'done="false"' });

        while (operations.length >= CCAIMaxOperationQueueLength) {
            log(`CCAI operation queue length has reached ${operations.length}.  Waiting ${CCAIOperationQueueWaitTime / 1000} seconds to try again...`);
            await new Promise(resolve => setTimeout(resolve, CCAIOperationQueueWaitTime));
            // await waitForOperationQueue();
            [operations] = await cciClient.operationsClient.listOperations({ name: `projects/${process.env.googleProjectID}/locations/${process.env.ccaiLocation}`, filter: 'done="false"' });
        }
    } catch (error) {
        log(`Error waiting for operation queue: ${error}`);
    }
}

// For each recording that has been uploaded to CCAI but has not been marked as analyzed,
// check to see if its analysis is complete and then flag it as CCAI_PROCESSING_COMPLETE
async function updateCCAIProcessingStatuses() {
    if (shouldContinue) {
        log('Updating CCAI processing statuses...');
        try {
            const recordingStatusRef = db.collection(process.env.recordingStatusCollection);
            const uploadToCCAIField = callStatuses.find(element => element.code === 'UPLOAD_TO_CCAI').description;
            const processingCompleteField = callStatuses.find(element => element.code === 'CCAI_PROCESSING_COMPLETE').description;
            const snapshot = await recordingStatusRef.where(uploadToCCAIField, '!=', null).where(processingCompleteField, '==', null).get();
            if (!snapshot.empty) {
                let updateCount = 0;
                let promises = [];
                snapshot.forEach(doc => {
                    promises.push(updateCCAIProcessingStatuss(doc.id))
                    updateCount++;
                });
                await Promise.all(promises);
                log(`CCAI processing statuses update complete. ${updateCount} record(s) updated.`);
            } else {
                log('No recordings found that require CCAI processing status updates.');
            }
        } catch (error) {
            log(`Error updating CCAI processing statuses: ${error.message}`);
        }
    }
}

async function updateCCAIProcessingStatuss(callID) {
    const analysisComplete = await checkIfCCAIAnalysisComplete(callID);
    if (analysisComplete) {
        await updateRecordingStatus(callID, 'CCAI_PROCESSING_COMPLETE');
        log(`CCAI processing complete status updated for callID ${callID}`);
    }
}

async function checkIfCCAIAnalysisComplete(callID) {
    try {
        // const cciClient = new ContactCenterInsightsClient({ projectId: process.env.googleProjectID });
        const conversation = await cciClient.getConversation({ name: `projects/${process.env.googleProjectID}/locations/${process.env.ccaiLocation}/conversations/${callID}` });

        if (conversation[0]?.latestAnalysis?.createTime) {
            return true;
        } else {
            return false;
        }
    } catch (error) {
        return false;
    }
}

// Find all recordings that have been analyzed by CCAI but not yet exported to BigQuery
// then start an export job for each one and mark them as UPLOAD_TO_BIGQUERY
async function exportRecordingsDataToBigQuery() {
    if (shouldContinue) {
        log('Checking for recordings to exoprt to BigQuery...');
        try {
            const recordingStatusRef = db.collection(process.env.recordingStatusCollection);
            const processingCompleteField = callStatuses.find(element => element.code === 'CCAI_PROCESSING_COMPLETE').description;
            const bigQueryField = callStatuses.find(element => element.code === 'UPLOAD_TO_BIGQUERY').description;
            const snapshot = await recordingStatusRef.where(processingCompleteField, '!=', null).where(bigQueryField, '==', null).get();
            let processCount = 0;
            let rateLimitReached = false;
            if (!snapshot.empty) {
                let promises = [];
                for (const doc of snapshot.docs) {
                    if (canExportToBQ()) {
                        promises.push(exportToBigquery(doc.id));
                        processCount++;
                    } else {
                        rateLimitReached = true;
                        break;
                    }
                }
                await Promise.all(promises);
                log(`BigQuery export check complete. Export job(s) started for ${processCount} record(s).`);
                if (rateLimitReached) {
                    log(`BigQuery export rate limit reached. Waiting for next period to start more exports.`);
                }
            } else {
                log('No recordings found that are ready to export to BigQuery.');
            }
        } catch (error) {
            log(`Error checking fo recordings to export BigQuery: ${error.message}`);
        }
    }
}

function canExportToBQ() {
    const now = Date.now();
    const oldestTime = bqRateLimiterQueue[bqRateLimiterQueuePointer];
    if (now - oldestTime > bqExportPeriodLengthInMs) {
        bqRateLimiterQueue[bqRateLimiterQueuePointer] = now;

        bqRateLimiterQueuePointer++;

        if (bqRateLimiterQueuePointer >= bqMaxExportsPerPeriod) {
            bqRateLimiterQueuePointer = 0;
        }

        return true;
    }
    return false;
}

async function exportToBigquery(callID) {
    try {
        const [operation] = await cciClient.exportInsightsData({
            parent: cciClient.locationPath(process.env.googleProjectID, process.env.ccaiLocation),
            bigQueryDestination: {
                projectId: process.env.googleProjectID,
                dataset: process.env.bigQueryDataset,
                table: process.env.bigQueryTable,
            },
            filter: `labels.callID:"${callID}"`,
            writeDisposition: 'WRITE_APPEND',
        });

        log(`Started export to BigQuery for callID ${callID} - operation name: ${operation.name}`);
        await updateRecordingStatus(callID, 'UPLOAD_TO_BIGQUERY');
    } catch (error) {
        log(`Error exporting to BigQuery for callID ${callID}: ${error}`);
    }
}

/**
 * Returns an object with the current date and the next day's date
 * @returns Object with startDate and endDate properties containing the current date and the next day's date
 */
function getRunDates() {
    const today = getCurrentDateInDesiredTimeZone();
    const tomorrow = getCurrentDateInDesiredTimeZone();
    tomorrow.setDate(today.getDate() + 1);

    return {
        startDate: today.toISOString().split('T')[0],
        endDate: tomorrow.toISOString().split('T')[0]
    };
}

async function getLastRunDate() {
    try {
        const document = db.doc(process.env.lastRunDocumentPath);
        const doc = await document.get();
        if (doc.exists) {
            lastRunDate = new Date(doc.data().timeStamp._seconds * 1000);
        } else {
            log(`WARNING: Unable to retrieve last run date from Firestore ${process.env.lastRunDocumentPath}. Using current date/time.`);
            lastRunDate = getCurrentDateInDesiredTimeZone();
        }
    } catch (error) {
        log(`WARNING: Unable to retrieve last run date from Firestore ${process.env.lastRunDocumentPath}. Using current date/time.`);
        lastRunDate = getCurrentDateInDesiredTimeZone();
    }
}

async function updateLastRunDate() {
    try {
        lastRunDate = getCurrentDateInDesiredTimeZone();
        const document = db.doc(process.env.lastRunDocumentPath);
        await document.update({ timeStamp: new Date(lastRunDate) });
    } catch (error) {
        log('Error setting last run date: ' + error.message);
    }
}

function log(message) {
    const timeStamp = getCurrentDateInDesiredTimeZone().toLocaleString('en-US');
    console.log(`[${timeStamp}] ${message}`);

    if (process.env.LOG_TO_FIRESTORE === 'true') {
        db.collection(process.env.logCollection).add({
            timeStamp: new Date(timeStamp),
            message: message
        })
            .catch((error) => {
                console.error(`Error logging to Firestore ${process.env.logCollection} collection: ${error}`);
            });
    }

    if (io) {
        io.emit('logMessage', `[${timeStamp}] ${message}`);
    }
}

function getCurrentDateInDesiredTimeZone() {
    return new Date(new Date().toLocaleString("en-US", { timeZone: desiredTimeZone }));
}

async function updateRecordingStatus(callID, statusCode) {
    try {
        const docRef = db.collection(process.env.recordingStatusCollection).doc(callID);
        const doc = await docRef.get();
        if (!doc.exists) {
            let docData = {};
            for (const element of callStatuses) {
                if (element.code === statusCode) {
                    docData[element.description] = FieldValue.serverTimestamp();
                } else {
                    docData[element.description] = null;
                }
            }

            await docRef.set(docData);
        } else {
            docData = doc.data();
            const statusDescription = callStatuses.find(element => element.code === statusCode).description;
            if (statusDescription) {
                docData[statusDescription] = FieldValue.serverTimestamp();
                await docRef.update(docData);
            } else {
                log(`ERROR: Invalid status code ${statusCode} for call ID ${callID}`);
            }
        }
    } catch (error) {
        log(`Error updating recording status for call ID ${callID}: ${error.message}`);
    }
}

async function mainLoop() {
    if (process.env.LOG_TO_FIRESTORE === 'true') {
        console.warn('Logging to Firestore is enabled');
    }

    while (shouldContinue) {
        console.log('Processing server is running');

        await getLastRunDate();

        await authenticate();
        if (token && baseURL) {
            const runDates = getRunDates();
            const job = await createJob(runDates.startDate, runDates.endDate);
            // const job = await createJob('2024-07-04', '2024-07-05');
            if (job && shouldContinue) {
                log(`SUCCESS: Job ${job} created successfully`);
                const jobResponse = await waitForJobCompletion(job);

                if (jobResponse.status === 'SUCCEEDED' && shouldContinue) {
                    const interactions = await getInteractionsFromReport(jobResponse.result.url);
                    await processInteractions(interactions);
                } else {
                    if (shouldContinue) {
                        log(`ERROR: Job ${job} failed with message: ${jobResponse.status}`);
                    }
                }

                await updateCCAIProcessingStatuses();

                await exportRecordingsDataToBigQuery();
            } else if (shouldContinue) {
                log('ERROR: Job creation failed.');
                shouldContinue = false;
            }
        }
    }

    log('Processing server has stopped.');
    if (io) {
        io.emit('serverStatus', 'Stopped');
    }
}
/* #endregion */

