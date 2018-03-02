const sync = require("sync");

module.exports = async config => {
  if (config.method === "POST") {
    await sync.sleep(5);
    let status = Math.floor(Math.random() * 10) == 0 ? 429 : 201;
    return {
      status: status,
      data: {
        result: [
          {
            status: "created",
            status_message: "Fake Send Successful",
            transform: "T1"
          },
          {
            status: "ignored",
            status_message: "No values changed - Fake",
            transform: "T2"
          }
        ]
      }
    };
  } else if (config.method === "GET") {
    return [];
  } else {
    throw `not supported`;
  }
};
