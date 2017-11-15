const express = require("express");
const app = express();
const bodyParser = require('body-parser');

app.use(function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

app.use(bodyParser.json());

require("./routes/routes.js")(app);

var server = app.listen(8080, function () {
    console.log("Listening on port %s...", server.address().port);
});
