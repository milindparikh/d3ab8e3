var http = require('http');

var fs = require('fs'),
    readline = require('readline');
main();


function main() {
    
    server = process.argv[2];
    processes = process.argv[3];
    
    limit = process.argv[4];
    
    request = {serverName: server, processes: processes, limit: limit};
    requestJSON = JSON.stringify(request);
    
    console.log(requestJSON);
    processRequest(requestJSON);
}
    
function processRequest (requestJSON) {

    
    var headers = {
	'Content-Type': 'application/json',
	'Content-Length': requestJSON.length
    };
    
    var options = {
	host: '192.168.1.249',
	port: 3000,
	path: '/classify',
	method: 'POST',
	headers: headers
    };

    var req = http.request(options, function(res) {
	res.setEncoding('utf-8');
	
	var responseString = '';
	res.on('data', function(data) {
	    responseString += data;
	});
	res.on('end', function() {
	    var resultObject = JSON.parse(responseString);
	    console.log(resultObject);
	    
	});
    });

    req.write(requestJSON);
    req.end();
}
