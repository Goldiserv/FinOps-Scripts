/* 
  Bulk populates "Account Groups" into Cloudability via API.
  Ensure first row of spreadsheet contains column headings
*/
const dotenv = require("dotenv");
dotenv.config({ path: ".env" });
const { Curl } = require("node-libcurl");

const readExcelMappings = (xlPath, xlSheetName) => {
  const XLSX = require("xlsx");

  const workbook = XLSX.readFile(xlPath);
  const worksheet = workbook.Sheets[xlSheetName];

  // Define the range of cells to read
  // const range = XLSX.utils.decode_range(xlRange);
  const data = XLSX.utils.sheet_to_json(worksheet, {
    raw: true,
    cellText: false,
  });
  // console.log({workbook});
  return data;
};

async function postToCloudability(accountGroupId, accountIdentifier, value) {
  const apiKey = process.env.CLOUDABILITY_API_KEY;
  const url = "https://api.cloudability.com/v3/account_group_entries";

  const curl = new Curl();

  const postData = JSON.stringify({
    account_group_id: accountGroupId,
    account_identifier: accountIdentifier,
    value: value,
  });

  curl.setOpt(Curl.option.URL, url);
  curl.setOpt(Curl.option.HTTPHEADER, ["Content-Type: application/json"]);
  curl.setOpt(Curl.option.POSTFIELDS, postData);
  curl.setOpt(Curl.option.USERPWD, apiKey);
  curl.setOpt(Curl.option.SSL_VERIFYHOST, false);
  curl.setOpt(Curl.option.SSL_VERIFYPEER, false);

  return new Promise((resolve, reject) => {
    curl.on("end", (statusCode, data) => {
      if (statusCode >= 200 && statusCode < 300) {
        resolve(data);
      } else {
        reject(`HTTP status code ${statusCode}: ${data}`);
      }
      curl.close();
    });

    curl.on("error", (error) => {
      reject(error);
      curl.close();
    });

    curl.perform();
  });
}

function convertTo12DigitStringWithHyphens(number) {
  // console.log({ number });
  if (!number || typeof (number / 1) != "number") return "-";

  // Convert the number to a string
  const numberString = number.toString();

  // Calculate the number of zeros to pad the string with
  const numberOfZeros = 12 - numberString.length;

  // Pad the string with zeros
  const paddedString = "0".repeat(numberOfZeros) + numberString;

  // Insert hyphens after the fourth and eighth digits
  const hyphenatedString =
    paddedString.slice(0, 4) +
    "-" +
    paddedString.slice(4, 8) +
    "-" +
    paddedString.slice(8);

  return hyphenatedString;
}

async function main(
  xlPath,
  xlSheetName,
  accountIdColName,
  accountGroupId,
  spreadsheetDataColumnName
) {
  console.log("start");
  // read excel
  const xlData = readExcelMappings(xlPath, xlSheetName);

  xlData.forEach((e) => {
    e["accountIdMod"] = convertTo12DigitStringWithHyphens(e[accountIdColName]);
  });
  // console.log({ xlData });

  const startCounter = 0;
  const endCounter = 999;
  let currentCounter = 0;
  for (const entry of xlData) {
    if (currentCounter >= endCounter) break;

    if (currentCounter >= startCounter) {
      const accountIdentifier = entry.accountIdMod; // e.g. '1234-1234-1234';
      const value = entry[spreadsheetDataColumnName]; 
      try {
        let resp = await postToCloudability(
          accountGroupId,
          accountIdentifier,
          value
        );
        console.log({ resp });
      } catch (e) {
        console.log(e);
      }
    }

    currentCounter++;
  }
}

//Controls
const accountGroupIdMap = {
  //found via .\get-account-group-entries.js
  environment: 6650,
  "billing-dept-id": 6651,
  "business-owner": 6901,
  "technical-owner": 6902,
};
// const spreadsheetStartRow = 2;
const spreadsheetColumnNamesLegacyAWS = {
  environment: "environment account group",
  "billing-dept-id": "billing-dept-id account group",
  "business-owner": "business-owner account group",
  "technical-owner": "technical-owner account group",
};
const xlPath = process.env.XL_PATH;
const xlSheetName = `Legacy AWS`;
const accountIdColName = `Account ID`;

main(
  xlPath,
  xlSheetName,
  accountIdColName,
  accountGroupIdMap.environment,
  spreadsheetColumnNamesLegacyAWS.environment
);
