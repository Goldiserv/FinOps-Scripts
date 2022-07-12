# Serverless Datadog API - AWS CDK

This is a project to create an API Gateway + Lambda to call the Datadog API.

Code is written in Java using AWS CDK, and a [Maven](https://maven.apache.org/) based project, so you can open this project with any Maven compatible Java IDE to build and run tests.

## Useful commands

 * `mvn package`     compile and run tests
 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

 Optionally add `--profile <profile-name>`

## Guide to using CDK with Java
1. Install Node. https://nodejs.org/en/download/
1. Install AWS CDK. npm i -g aws-cdk
1. Install Java (v11 Corretto supported by AWS Lambda). e.g. https://aws.amazon.com/corretto/
1. Install Maven. https://maven.apache.org/install.html
    1. Update Maven packages: mvn dependency:resolve
1. -ExecutionPolicy Bypass for shell commands https://stackoverflow.com/questions/56199111/visual-studio-code-cmd-error-cannot-be-loaded-because-running-scripts-is-disabl/67420296#67420296
1. Build the .jar file for Lambda upload 
    1. nav to .\aws-cdk\datadog-api\functions\FunctionOne
    1. shell: mvn clean package
    1. shell: mvn compile to update java version if needed
1. Nav .\aws-cdk\datadog-api\infra and run:
    1. cdk synth
    1. cdk diff
    1. cdk deploy