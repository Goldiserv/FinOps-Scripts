const office = require("office-js");
const fs = require("fs");
const path = require("path");

// Function to run when the Office JavaScript API is ready
function initialize() {
  // Load the workbook from the existing XLSX file
  Excel.run(function (context) {
    const workbook = context.workbooks.open("existing_file.xlsx");

    // Get the worksheet containing the data for the pivot table
    const dataSheet = workbook.worksheets.getItem("cost_data");

    // Define the range for the data to be used in the pivot table
    const dataRange = dataSheet.getUsedRange();

    console.log({ dataRange });

    // Get the worksheet where the pivot table will be added
    const pivotSheet = workbook.worksheets.getActiveWorksheet();

    // Define the range for the pivot table
    const pivotRange = pivotSheet.getRange("A1");

    // Create the pivot table using the data range and pivot range
    const pivotTable = pivotSheet.pivotTables.add(
      dataRange.address,
      pivotRange.address
    );

    // Set the pivot table field options
    pivotTable.rowFields.add("Category");
    pivotTable.columnFields.add("Month");
    pivotTable.dataFields.add("Amount", "Sum");

    // Refresh the pivot table
    pivotTable.refresh();

    // Save the workbook
    return context.sync().then(function () {
      console.log("Pivot table added and workbook saved successfully.");
    });
  }).catch(function (error) {
    console.log("Error: " + error);
  });
}

function main() {
  const folderPath = "./data";
  const fileName = "joseph.davies_unsw.edu.au.xlsx";
  const filePath = path.join(folderPath, fileName)

  // Read the file and initialize the Office JavaScript API
  fs.readFile(filePath, function (err, data) {
    if (err) {
      console.log("Error reading file: " + err);
      return;
    }

    // Initialize the Office JavaScript API with the file data
    office.initialize = function (reason) {
      office
        .initialize({ file: data })
        .then(function () {
          initialize();
        })
        .catch(function (error) {
          console.log("Error initializing Office JavaScript API: " + error);
        });
    };
  });
}

main();
