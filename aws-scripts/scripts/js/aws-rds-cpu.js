const AWS = require('aws-sdk');
const moment = require('moment');
const util = require('util')

// Set the AWS region
AWS.config.update({ region: 'us-east-1' });

// Create an instance of the RDS client
const rds = new AWS.RDS();

// Define the start and end times for the data retrieval
const startTime = moment().subtract(2, 'hours').toDate();
const endTime = new Date();

// Define the parameters for the describeDBInstances operation
const params = {};

// Call the describeDBInstances operation
rds.describeDBInstances(params, (err, data) => {
  if (err) {
    console.log(err, err.stack);
  } else {
    // Get an array of all RDS instance identifiers
    const dbInstanceIdentifiers = data.DBInstances.map(instance => instance.DBInstanceIdentifier);

    // Array to store the peak CPU utilization of each instance
    const peakCPUUtilizations = [];

    // Get the peak CPU utilization of each RDS instance
    dbInstanceIdentifiers.forEach(dbInstanceIdentifier => {
      const cloudwatch = new AWS.CloudWatch();

      const cloudwatchParams =
      {
        "StartTime": startTime,
        "EndTime": endTime,
        "MetricDataQueries": [
          {
            "Id": "m1",
            "Label": "CPUUtilization, peak of ${MAX} was at ${MAX_TIME}",
            "MetricStat": {
              "Metric": {
                "Namespace": "AWS/RDS",
                "MetricName": "CPUUtilization",
                "Dimensions": [
                  {
                    "Name": "DBInstanceIdentifier",
                    "Value": "database-1"
                  }
                ]
              },
              "Period": 300,
              "Stat": "Average"
            }
          }
        ]
      };

      console.log({ cloudwatchParams })

      cloudwatch.getMetricData(cloudwatchParams, (err, data) => {
        if (err) {
          console.log(err, err.stack);
        } else {
          // Add the peak CPU utilization of the current instance to the array
          // console.log({ data })
          console.log(util.inspect(data, false, null, true /* enable colors */))
          peakCPUUtilizations.push({
            DBInstanceIdentifier: dbInstanceIdentifier,
            PeakCPUUtilization: data.MetricDataResults[0].Values[0]
          });

          // Check if the peak CPU utilization of all instances have been calculated
          if (peakCPUUtilizations.length === dbInstanceIdentifiers.length) {
            // Sort the instances based on peak CPU utilization
            const sortedInstances = peakCPUUtilizations.sort((a, b) => b.PeakCPUUtilization - a.PeakCPUUtilization);

            // Display the peak CPU utilization of all instances
            console.log(sortedInstances);
          }
        }
      });
    });
  }
});
