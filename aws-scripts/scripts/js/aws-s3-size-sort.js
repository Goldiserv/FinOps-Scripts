// Unfortunately doesn't accurately return bucket sizes

const AWS = require('aws-sdk');

// Set the AWS region
AWS.config.update({region: 'us-west-2'});

// Create an instance of the S3 client
const s3 = new AWS.S3();

// Define the parameters for the listBuckets operation
const params = {};

// Call the listBuckets operation
s3.listBuckets(params, (err, data) => {
  if (err) {
    console.log(err, err.stack);
  } else {
    // Get an array of all bucket names
    const bucketNames = data.Buckets.map(bucket => bucket.Name);

    // Array to store the size of each bucket
    const bucketSizes = [];

    // Get the size of each bucket
    bucketNames.forEach(bucketName => {
      s3.listObjectsV2({Bucket: bucketName}, (err, data) => {
        if (err) {
          console.log(err, err.stack);
        } else {
          // Add the size of the current bucket to the array
          const size = data.Contents.reduce((acc, obj) => acc + obj.Size, 0);
          bucketSizes.push({Bucket: bucketName, Size: size});

          // Check if all bucket sizes have been calculated
          if (bucketSizes.length === bucketNames.length) {
            // Sort the buckets based on size
            const sortedBuckets = bucketSizes.sort((a, b) => b.Size - a.Size);

            // Get the top 10 largest buckets
            const largestBuckets = sortedBuckets.slice(0, 10);

            // Display the largest buckets
            console.log(largestBuckets);
          }
        }
      });
    });
  }
});
