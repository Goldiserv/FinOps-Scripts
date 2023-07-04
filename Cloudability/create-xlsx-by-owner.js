// Call cloudy api
const fs = require("fs");
const path = require("path");
const xlsx = require("xlsx");

// Function to loop across JSON files
function aggregateJsonFiles(folderPath, fileNameFilter) {
  const fileNames = fs.readdirSync(folderPath);

  let aggregatedData = [];

  fileNames.forEach((fileName) => {
    if (fileName.includes(fileNameFilter)) {
      const filePath = path.join(folderPath, fileName);
      console.log({ filePath });
      const fileData = fs.readFileSync(filePath, "utf8");
      const jsonData = JSON.parse(fileData);

      // consolidate
      aggregatedData.push(...jsonData.results);
    }
  });

  return aggregatedData;
}

function getEmailAddresses(aggregatedData, tagObjStr) {
  let extractedValues = aggregatedData.map((obj) => obj[tagObjStr]);
  const uniqueValues = new Set(extractedValues);
  return Array.from(uniqueValues);
}

function cleanAndSplitEmails(uniqueValues) {
  // Use entry or sample list.
  const emails = uniqueValues || [
    "john@example.com",
    "mary@example.com",
    "john@example.com jane@example.com",
    "alice@example.com/tom@example.com",
    "john@example.com sam@example.com",
    "c@example.com ;d@example.com",
    "e@example.com ,f@example.com",
    "g@example.com|h@example.com",
  ];

  // Function to extract unique emails from the array
  function extractUniqueEmails(emails) {
    const uniqueEmails = new Set();

    emails.forEach((email) => {
      const compoundEmails = email.split(/[ \/\,\;]/);

      compoundEmails.forEach((compoundEmail) => {
        const trimmedEmail = compoundEmail.trim();

        if (trimmedEmail) {
          uniqueEmails.add(trimmedEmail);
        }
      });
    });
    return Array.from(uniqueEmails);
  }

  // Extract unique emails
  let uniqueEmailArray = extractUniqueEmails(emails);

  // Check for incomplete splitting
  const moreThanOneEmailErrors = uniqueEmailArray.filter(
    (email) => email.split("@").length > 2 //more than 3 parts in split
  );
  console.log({ moreThanOneEmailErrors });

  function removeInvalidEmails(array) {
    const validEmails = [];
    const invalidEmails = [];

    for (const entry of array) {
      if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(entry)) {
        validEmails.push(entry);
      } else {
        invalidEmails.push(entry);
      }
    }
    console.log({ invalidEmails });

    return validEmails;
  }
  uniqueEmailArray = removeInvalidEmails(uniqueEmailArray);

  return uniqueEmailArray;
}

function splitDataToOwners(
  aggregatedData,
  ownerEmails,
  ownerEmailStartIndex,
  ownerEmailEndIndex
) {
  let emails = ownerEmails.slice(ownerEmailStartIndex, ownerEmailEndIndex);
  let emailToData = { unallocated: [] };
  emails.forEach((e) => (emailToData[e] = [])); // console.log({ emailToData });

  aggregatedData.forEach((data) => {
    let ownerConcat = data[nameLabelMap.tag5] + data[nameLabelMap.tag6];

    emails.forEach((email) => {
      if (ownerConcat.includes(email)) {
        emailToData[email].push(data);
      }
    });
  });

  return emailToData;
}

function cleanFilename(filename) {
  return filename.replace(/[^a-zA-Z0-9.-]/g, "_");
}
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
function remapKeys(obj, mapping) {
  const remappedObj = {};
  for (const key in obj) {
    if (mapping.hasOwnProperty(key)) {
      remappedObj[mapping[key]] = obj[key];
    } else {
      remappedObj[key] = obj[key];
    }
  }
  return remappedObj;
}

function getHeaderRow(ws) {
  const header = [];
  const columnCount = xlsx.utils.decode_range(ws["!ref"]).e.c + 1;
  for (let i = 0; i < columnCount; ++i) {
    header[i] = ws[`${xlsx.utils.encode_col(i)}1`].v;
  }
  return header;
}
function getHeaderIndexes(ws, headerTexts) {
  const headerRow = getHeaderRow(ws);
  return headerTexts.map((headerText) =>
    headerRow.findIndex((header) => header === headerText)
  );
}
function saveArrayToXLSX(data, filePath) {
  const ws = xlsx.utils.json_to_sheet(data);

  // Format currency values
  const currencyFormat = "$#,##0.00";
  var currencyColumns = getHeaderIndexes(ws, ["Cost (Amortised)"]);
  console.log({ currencyColumns });

  const range = xlsx.utils.decode_range(ws["!ref"]);
  for (let i = range.s.r + 1; i <= range.e.r; ++i) {
    for (const colNum of currencyColumns) {
      let ref = xlsx.utils.encode_cell({ r: i, c: colNum });
      let cell = ws[ref];
      if (cell) {
        const cellValue = parseFloat(cell.v);
        if (!isNaN(cellValue)) {
          cell.t = "n"; // Set cell type to numeric
          cell.v = cellValue; // Update cell value
          cell.z = currencyFormat; // Apply currency format
        }
      }
    }
  }

  const workbook = xlsx.utils.book_new();
  xlsx.utils.book_append_sheet(workbook, ws, "cost_data");
  xlsx.writeFile(workbook, filePath);
}

function saveObjectValuesToXLSX(obj, folderPath) {
  for (const key in obj) {
    if (Object.hasOwnProperty.call(obj, key)) {
      const values = obj[key];
      const cleanedKey = cleanFilename(key);
      const filePath = path.join(folderPath, `${cleanedKey}.xlsx`);

      // Save the CSV data to a file in the 'data' folder
      if (values.length > 0) saveArrayToXLSX(values, filePath);
    }
  }
}

function main() {
  const folderPath = "./data";
  const fileNameFilter = "Service-Showback_2023-05-01";
  let aggregatedData = aggregateJsonFiles(folderPath, fileNameFilter);
  aggregatedData = aggregatedData.map((obj) => remapKeys(obj, nameLabelMap)); // console.log({aggregatedData});

  // get unique emails
  let ownerEmails = getEmailAddresses(aggregatedData, nameLabelMap.tag5); // tech owner
  let ownerEmails2 = getEmailAddresses(aggregatedData, nameLabelMap.tag6); // sys owner
  ownerEmails = cleanAndSplitEmails([...ownerEmails, ...ownerEmails2]); // console.log({ownerEmails});

  // group data to owners
  let result = splitDataToOwners(aggregatedData, ownerEmails, 0, 2); // console.log(JSON.stringify(result, null, 2));
  // save each owner's cost to Excel
  saveObjectValuesToXLSX(result, folderPath);
}

main();