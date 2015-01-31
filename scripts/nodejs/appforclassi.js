
// var qReq = {serverName: 'SAMPLESVR1', processes: ['proc1', 'proc2', 'proc3']};
// var qRes = {serverName: 'SAMPLESVR1', applications: [{app: 'appl1', conf: '50'}, {app: 'appl2', conf: '10'}]}

// var jsonQReq = JSON.stringify(qReq);
// var jsonQRes = JSON.stringify(qRes);


var express = require('express');
var app = express();



app.post('/classify', function (req, res) { 
    res.contentType('application/json');
    var  qRes = formatResponse(req);
    var jsonQRes = JSON.stringify(qRes);
    res.send(jsonQRes);
});


function formatResponse(req) {
    return {serverName: 'SAMPLESVR1', applications: [{app: 'appl1', conf: '50'}, {app: 'appl2', conf: '10'}]};    
}


var server = app.listen(3000, function () {
  var host = server.address().address
  var port = server.address().port
  console.log('App for metrics listening at http://%s:%s', host, port)
})
	

