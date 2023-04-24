// Call cloudy api
const dotenv = require("dotenv");
const axios = require("axios");
const qs = require("qs");
dotenv.config({ path: ".env" });

const cloudyKey = process.env.CLOUDABILITY_API_KEY;
console.log("Starting script");

// Authorization: Basic <You_API_Key Base64 encoded>
let buff = new Buffer(cloudyKey);
let base64CloudyKey = buff.toString("base64");
console.log({ base64CloudyKey });
axios.defaults.headers.common = { Authorization: `Basic ${cloudyKey}` };

const config = {
  headers: {
    Authorization: `Basic ${base64CloudyKey}`,
  },
};
var data = {
  u: `${base64CloudyKey}`,
};
var options = {
  method: "GET",
  headers: {
    Authorization: `Basic ${base64CloudyKey}`,
  },
  data: qs.stringify(data),
  url: `https://api.cloudability.com/v3/vendors`,
};

// axios(options).then((res) => console.log(res));
// .catch((err) => console.log(err));
const authOption = [
  `Basic ${base64CloudyKey}`,
  `${base64CloudyKey}`,
  `Basic ${cloudyKey}`,
  `${cloudyKey}`
]

const resp = axios.get("https://api.cloudability.com/v3/vendors", {
  auth: {
    username: authOption[3],
  },
});
console.log({ resp });
