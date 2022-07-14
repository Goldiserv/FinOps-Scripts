//terminal cmd to run: node .\s3BulkRename.js 

const profileName = 'default' //183945581419_ExtAwsSpendingReviewerMgmt

console.log('Starting script');
const execSync = require('child_process').execSync;

var outputBuf;
if (profileName == '') {
    outputBuf = execSync(`aws s3 ls`); //output is buffer
} else {
    outputBuf = execSync(`aws s3 ls --profile ` + profileName); //output is buffer
}
const output = outputBuf.toString();
console.log('Output was:');
console.log(output);

// function myFunction(p1, p2) {
// 	return p1 * p2; // The function returns the product of p1 and p2
// }
// console.log(myFunction(1, 2));