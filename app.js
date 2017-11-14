var express = require("express");
var app = express();


app.use(function (req, res, next) {
    res.header("Access-Control-Allow-Origin", "*");
    res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    next();
});

require("./routes/routes.js")(app);

var server = app.listen(8080, function () {
    console.log("Listening on port %s...", server.address().port);
});
