//terminal cmd to run: node .\s3BulkRename.js 

console.log('Starting script');

const execSync = require('child_process').execSync;
for (var i = 1; i <= 5; i++) {
    var currentIndex = i;
    currentIndex = ("0" + (currentIndex)).slice(-2)
        //console.log(currentIndex);
    const output = execSync(`aws s3 mv s3://path/filename_${currentIndex}.ext s3://path/filename_${currentIndex}.new_ext`, { encoding: 'utf-8' }); // the default is 'buffer'
    console.log('Output was:\n', output);
}