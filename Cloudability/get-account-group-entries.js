
const dotenv = require("dotenv");
const axios = require("axios");
dotenv.config({ path: ".env" });

// Authorization: Basic <Your_API_Key Base64 encoded>
const cloudyKey = process.env.CLOUDABILITY_API_KEY;
let buff = new Buffer(cloudyKey);
let base64CloudyKey = buff.toString("base64");
console.log({ base64CloudyKey });

let url = [
  "https://api.cloudability.com/v3/reporting/cost/run?start_date=end+of+last+month&end_date=end+of+last+month&dimensions=vendor&metrics=unblended_cost",
  "https://api.cloudability.com/v3/account_groups",
  "https://api.cloudability.com/v3/account_group_entries",
];

const config = {
  headers: {
    Authorization: `Basic ${base64CloudyKey}`,
  },
};
async function main() {
  const resp = await axios.get(url[1], config);
  console.log(JSON.stringify(resp.data));
}

main();
