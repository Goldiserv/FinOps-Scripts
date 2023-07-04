/**
 * Call Cloudability API to run report and save to .json files.
 * Handles pagination. 
 * Default filter includes usage cost only.
 */

const fs = require("fs");
const path = require("path");
const dotenv = require("dotenv");
const axios = require("axios");
dotenv.config({ path: ".env" });

// Authorization: Basic <Your_API_Key Base64 encoded>
const cloudyKey = process.env.CLOUDABILITY_API_KEY;
let buff = new Buffer(cloudyKey);
let base64CloudyKey = buff.toString("base64"); // console.log({ base64CloudyKey });

let url = {
  get_cost_reports: "https://api.cloudability.com/v3/reporting/reports/cost",
  get_dimensions: "https://api.cloudability.com/v3/reporting/cost/measures",
  run_report: "https://api.cloudability.com/v3/reporting/cost/run",
};

let exampleQueryStr =
  "?start_date=2023-05-01&end_date=2023-06-01&dimensions=vendor&metrics=total_amortized_cost&sort=total_amortized_costASC&filters=transaction_type%3D%3Dusage";
//filters (amortized cost > 100, region contains ‘us-east-‘)
let exampleMultiFilter =
  "&filters=total_amortized_cost%3E100&filters=region%3D%40us-east-";

let reportQueryComponents = {
  start_date: "start_date", //2022-02-01
  end_date: "end_date", //inclusive of day, so use last day of current month if doing monthly rpt.
  dimensions: "dimensions",
  dimensionsList: ["vendor"],
  metrics: "metrics", //total_amortized_cost
  sort: "sort", //e.g. by total_amortized_costASC
  filters: "filters", //filters=transaction_type%3D%3Dusage
};

function saveFileToFolder(folderPath, fileName, fileData) {
  // Check if the folder exists, create it if it doesn't
  if (!fs.existsSync(folderPath)) {
    fs.mkdirSync(folderPath, { recursive: true });
  }

  // Save the file to the folder
  const filePath = path.join(folderPath, fileName);
  fs.writeFileSync(filePath, fileData);
  console.log(`File saved successfully at: ${filePath}`);
}

async function getDimensions() {
  const response = await axios.get(url.get_dimensions, config);
  saveFileToFolder(JSON.stringify(response.data), "get_dimensions.json");
}

function urlQueryBuilder(originalURL, reportQueryArray, token) {
  originalURL = originalURL + "?";
  let i = 0;
  while (i < reportQueryArray.length) {
    originalURL += reportQueryArray[i] + "&";
    i++;
  }
  originalURL = originalURL.slice(0, -1);
  if (token) originalURL += `&token=${token}`;
  return originalURL;
}

const config = {
  headers: {
    Authorization: `Basic ${base64CloudyKey}`,
  },
  //responseType: "blob" //for CSV
};

let nameLabelMap = {
  category1: "Environment",
  category3: "Department",
  tag2: "Service (tag)",
  tag5: "Tech Owner",
  tag6: "System Owner",
  account_name: "Payer Account Name",
  vendor_account_name: "Account Name",
  vendor_account_identifier: "Account ID",
  item_description: "Item Description",
  service_name: "Cloud Service Name",
  total_amortized_cost: "Cost (Amortised)",
  year_month: "Month",
};
function formatDate(year, month, getStart) {
  let date = new Date(year, month, 0);

  let monthStr = String(date.getMonth() + 1).padStart(2, "0");
  let dayStr = getStart ? "01" : String(date.getDate()).padStart(2, "0");

  return `${year}-${monthStr}-${dayStr}`;
}

async function main() {
  let reportName = `Service-Showback`,
    month = 5,
    startDateStr = formatDate(2023, month, true),
    endDateStr = formatDate(2023, month, false);

  //build query string
  let queryPartsArr = [
    `start_date=${startDateStr}`, // May = 5
    `end_date=${endDateStr}`,
    //see get_dimensions.json for a list of dimensions or run getDimensions();
    "dimensions=year_month,account_name,vendor_account_name,vendor_account_identifier,tag2,category1,category3,tag5,tag6,service_name,item_description", //item_description
    "metrics=total_amortized_cost",
    "filters=transaction_type%3D%3Dusage",
  ];

  // example report
  // const exampleResp = await axios.get(url.run_report + exampleQueryStr, config);

  let pagination = null,
    page = 0;
  while (true) {
    // get response
    let finalUrl = urlQueryBuilder(url.run_report, queryPartsArr, pagination); // console.log(finalUrl);
    const resp = await axios.get(finalUrl, config);
    let fileName = `${reportName}_${startDateStr}_${endDateStr}_${page}.json`;
    saveFileToFolder("./data", fileName, JSON.stringify(resp.data));
    pagination = resp.data.pagination.next; // console.log({ pagination });

    if (!pagination) {
      // Break the loop if no more data is available
      break;
    }

    // Increment the page number for the next iteration
    page++;
  }

  console.log(`---DOWNLOAD COMPLETE---`);
}

main();
