// dependencies
const AWS = require('aws-sdk');
const util = require('util');
const s3 = new AWS.S3();
const fs = require('fs'); //file read/write

exports.handler = async(event, context, callback) => {

    // Read options from the event parameter.
    console.log("Reading options from event:\n", util.inspect(event, { depth: 5 }));
    const srcBucket = event.Records[0].s3.bucket.name;
    // Object key may have spaces or unicode non-ASCII characters.
    const srcKey = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, " "));
    const dstBucket = srcBucket;
    const dstKey = "resized-" + srcKey;

    // Download the file from the S3 source bucket. 
    console.log("start");
    try {
        const params = {
            Bucket: srcBucket,
            Key: srcKey
        };
        var templateFile = await s3.getObject(params).promise();
        console.log("success");
        console.log(templateFile.Body.toString('utf-8'));
    } catch (error) {
        console.log(error);
        return;
    }
    console.log('Doneski 1');

    // replace words
    try {
        //var buffer = "new texxt";
        var buffer = templateFile.Body.toString('utf-8').replace(/Your secret customer unique id/g, 'abc123');
        
    } catch (error) {
        console.log(error);
        return;
    }
    
    // Upload to the destination bucket
    try {
        const destparams = {
            Bucket: dstBucket,
            Key: dstKey,
            Body: buffer,
            ContentType: "text"
        };

        const putResult = await s3.putObject(destparams).promise();

    } catch (error) {
        console.log(error);
        return;
    }
    
    console.log('Successfully resized ' + srcBucket + '/' + srcKey +
        ' and uploaded to ' + dstBucket + '/' + dstKey); 
    
};