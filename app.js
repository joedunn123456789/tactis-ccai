const axios = require('axios');
const jwt = require('jsonwebtoken');
const { parse } = require('csv-parse');
const { Storage } = require('@google-cloud/storage');
const { Firestore } = require('@google-cloud/firestore');
const dotenv = require('dotenv');
dotenv.config();

const db = new Firestore({ projectId: process.env.googleProjectID, databaseId: process.env.databaseId });

let token = null;
let baseURL = null;
let lastRunDate = null;
////////////////////////////////////
//////////// FIX THIS //////////////
// checks last run date. if no run date is present, waits 30 seconds before executing.
// const minRunInterval = 30000;   // 30 seconds, per the CXone API rate limit
const minRunInterval = 30;
////////////////////////////////////
////////////////////////////////////

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
                // process.env.username throws an error becuase it is using sys env variables
                // process.env.password throws an error becuase it is using sys env variables
                // 'username': process.env.username,
                // 'password': process.env.password,
                // modified values to work with tactis infrastructure 
                'username': process.env.accessKey,
                'password': process.env.secretAccessKey,
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
    log('Creating data extraction job...');
    msSinceLastRun = new Date() - lastRunDate;
    if (msSinceLastRun < minRunInterval) {
        log(`Waiting ${minRunInterval - msSinceLastRun} ms before calling API again...`);
        await new Promise(resolve => setTimeout(resolve, minRunInterval - msSinceLastRun));
    }

    try {
        const config = {
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

        const response = await axios.post(
            url,
            body,
            config
        );

        await updateLastRunDate()

        return response.data;

    } catch (error) {
        console.error('Error creating data extraction job: ', error);
    }
}

async function getJobStatus(jobID) {
    try {
        const config = {
            headers: {
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        };

        const url = baseURL + `/data-extraction/v1/jobs/${jobID}`;

        const response = await axios.get(
            url,
            config
        );

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

                const interactions = [];
                for (let n = 1; n < reportData.length; n++) {
                    const interaction = {
                        callID: reportData[n][callIDField],
                        recordingStatus: reportData[n][recordingStatusField],
                        skill: reportData[n][skillField],
                        startDate: new Date(reportData[n][startDateField])
                    };

                    const coIndex = interactions.findIndex(co => co.callID === reportData[n][callIDField]);
                    if (coIndex === -1) {
                        interactions.push(interaction);
                    } else {
                        if (interaction.startDate < interactions[coIndex].startDate) {
                            interactions[coIndex] = interaction;
                        }
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
    for (let i = 0; i < interactions.length; i++) {
        if (interactions[i].recordingStatus.localeCompare('Success', undefined, { sensitivity: 'base' }) === 0) {
            const recordingURLResponse = await getRecordingURL(interactions[i].callID);

            if (recordingURLResponse.status === 'OK') {
                const copyResponse = await copyRecordingToBucket(recordingURLResponse.recordingURL, interactions[i].callID, interactions[i].skill);
                if (copyResponse.status === 'OK') {
                    log(`Success: ${copyResponse.message}`);
                } else {
                    log(`Error: ${copyResponse.message}`);
                }
            } else {
                log(`Unable to get URL for Call ID ${recordingURLResponse.callID}: ${recordingURLResponse.message}`);
            }
        } else {
            log(`Recording for Call ID ${interactions[i].callID} is not available`);
        }
    }
}

async function getRecordingURL(callID) {
    try {
        log(`Getting recording URL for call ID ${callID}...`);
        const config = {
            headers: {
                'accept': 'application/json',
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        };
        const url = baseURL + `/media-playback/v1/contacts?acd-call-id=${callID}&media-type=voice-only&exclude-waveforms=true&exclude-qm-categories=true&isDownload=true`;
        const response = await axios.get(url, config);

        return {
            status: 'OK',
            callID: callID,
            recordingURL: response.data.redirectUrl
        };
    } catch (error) {
        return {
            status: 'ERROR',
            callID: callID,
            message: '[' + error.response.status + '] ' + error.response.data.message
        };
    }
}

async function copyRecordingToBucket(url, callID, skill) {
    try {
        log(`Copying recording for call ID ${callID} to bucket...`);
        const response = await axios({
            method: 'get',
            url: url,
            responseType: 'stream'
        });

        const copyResult = await new Promise((resolve, reject) => {
            (async () => {
                const bucketName = process.env.bucketName;
                const destFileName = `${skill}/${callID}.mp4`;

                const storage = new Storage();
                const myBucket = storage.bucket(bucketName);
                const file = myBucket.file(destFileName);
                const exists = await file.exists();
                if (exists[0]) {
                    resolve(`File ${destFileName} already exists in bucket, skipping...`);
                } else {
                    const bucketStream = file.createWriteStream({ options: { resumable: false } });

                    response.data.pipe(bucketStream)
                        .on('error', function (err) {
                            reject(err);
                        })
                        .on('finish', function () {
                            resolve(`Recording ${callID} uploaded to ${skill} folder in ${bucketName} bucket`);
                        });
                }
            })()
        });

        return {
            status: 'OK',
            message: copyResult
        };
    } catch (error) {
        return {
            status: 'ERROR',
            message: error.response.data
        };
    }
}

/**
 * Returns an object with the current date and the next day's date
 * @returns Object with startDate and endDate properties containing the current date and the next day's date
 */
function getRunDates() {
    //TODO: modify logic to handle cases where the last run was more than 24 hours ago
    const today = new Date();
    const tomorrow = new Date();
    tomorrow.setDate(today.getDate() + 1);

    return {
        startDate: today.toISOString().split('T')[0],
        endDate: tomorrow.toISOString().split('T')[0]
    };
}

async function getLastRunDate() {
    //  runs without database, delete below line and uncomment 344-357 to restore database functions
    lastRunDate = new Date();
    // try {
    //     const document = db.doc(process.env.lastRunDocumentPath);
    //     const doc = await document.get();
    //     if (doc.exists) {
    //         lastRunDate = new Date(doc.data().timeStamp._seconds * 1000);
    //     } else {
    //         log(`WARNING: Unable to retrieve last run date from Firestore ${process.env.lastRunDocumentPath}. Using current date/time.`);
    //         lastRunDate = new Date();
    //     }
    // } catch (error) {
    //     log('ERROR retrieving last run date from Firestore: ', error.message);
    // }
}

async function updateLastRunDate() {
//  runs without database, delete below line and uncomment 362-368 to restore database functions    
    lastRunDate = new Date();
//  try {
//         lastRunDate = new Date();
//         const document = db.doc(process.env.lastRunDocumentPath);
//         await document.update({ timeStamp: new Date(lastRunDate) });
//     } catch (error) {
//         log('Error setting last run date: ', error.message);
//     }
}

function log(message) {
    //TODO: Find out how Tactis would like to handle logging
    const timeStamp = new Date().toLocaleString('en-US');
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
}

async function mainLoop() {
    if (process.env.LOG_TO_FIRESTORE === 'true') {
        console.warn('Logging to Firestore is enabled');
    }

    let shouldContinue = true;
    while (shouldContinue) {
        await getLastRunDate();
        //TODO: Add logic to handle cases where the last run was not today

        await authenticate();
        if (token && baseURL) {
            const runDates = getRunDates();
            /////////////////////////////////////
            ////////////// FIX THIS //////////////
            // runs for current date
            // const job = await createJob(runDates.startDate, runDates.endDate);
            // runs for specific date
            const job = await createJob('2024-07-04', '2024-07-05');
            /////////////////////////////////////
            /////////////////////////////////////
            if (job) {
                log(`SUCCESS: Job ${job} created successfully`);
                const jobResponse = await waitForJobCompletion(job);

                if (jobResponse.status === 'SUCCEEDED') {
                    const interactions = await getInteractionsFromReport(jobResponse.result.url);
                    await processInteractions(interactions);
                } else {
                    //TODO: Add logic to handle any status other than 'SUCCEEDED' (use jobResult.messageError)
                    log(`ERROR: Job ${job} failed with message: ${jobResponse.status}`);
                }
            } else {
                //TODO: Add logic to handle failed job creation
                log('ERROR: Job creation failed');
            }

            //TODO: Change this to run the job forever. Stop on catastrophic failure
            shouldContinue = false;
        }
    }
}

mainLoop();


// to do - how to run this - cloud run, cloudspace, vm google cloud (cost), 