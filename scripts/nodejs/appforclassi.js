
// var qReq = {serverName: 'SAMPLESVR1', processes: ['proc1', 'proc2', 'proc3'], limit: 25};
// var qRes = {serverName: 'SAMPLESVR1', applications: [{app: 'appl1', conf: '50'}, {app: 'appl2', conf: '10'}]}

// var jsonQReq = JSON.stringify(qReq);
// var jsonQRes = JSON.stringify(qRes);


var express = require('express');
var app = express();

var redis = require("redis"),
    client = redis.createClient();





app.post('/classify', function (req, res) { 

    
    var content = '';
    
    req.on('data', function (data) {
       // Append data.
	content += data;
    });
    
   req.on('end', function () {
       // Assuming, we're receiving JSON, parse the string into a JSON object to return.
       
       var data = JSON.parse(content);
       console.log(data.serverName);
       console.log(data.processes);
       console.log(data.limit);
       

       returnScoredApps (data.processes, data.limit, function (scoredApps) {


	   res.contentType('application/json');

	   scoredAppsJSON = JSON.stringify(scoredApps);

	   var hashResponse = {serverName: data.serverName, applications: scoredAppsJSON};
	   var jsonQRes = JSON.stringify(hashResponse);
	   res.send(jsonQRes);
	   
       });
       
       

   });
    
});







	    
function getQueryVector(inputprocs, cb) {
    
    var queryVector = {};
    var cnt = 0;
    
    inputprocs2 = inputprocs.split(",");

    inputprocs2.map(function (val) { 
	client.hget("idf", val, function (err, idfval) {
	    cnt++;
	    queryVector[val] = idfval/inputprocs2.length;

	    if (cnt == inputprocs2.length) {
		cb(queryVector);
		
	    }
	});
    });
}






function getRelevantApplications (queryVector, cb) {


    
    client.smembers("apps", function (err, apps) { 
	var newApps = [];
	var featureVals = {};

	var processApps = apps.length;
	
	apps.forEach( function (app, index)  {
	    var cnt = 0;
	    var pushed = 0;

	    var processFeatures = Object.keys(queryVector).length;
	    
	    Object.keys(queryVector).forEach (function (feature, index2) {
		var processedFeature = function() {
		    processFeatures -= 1;
		    if (processFeatures == 0) {
			processedApp();
		    }
		}
		var processedApp = function() {
		    processApps -= 1;
		    if (processApps == 0) {
			cb(newApps);
		    }
		}
		client.hget ("ntfidf:"+app, feature, function (err, val) {

		    if (pushed != 1) {
			if (val != null) {
			    newApps.push(app);
			    pushed =1;
			    processedFeature();
			}
			else {
			    processedFeature();
			}
		    }
		    else {
			processedFeature();
		    }
		});
	    });
	});
    });
}

function computeDistances (queryVector, apps, cb) {
    
    var applicationScores = {};
    var cnt = 0;

    var processedApplications = apps.length;
    
    var processApplication = function () {
	processedApplications -= 1;
	if (processedApplications == 0) {
	    cb (applicationScores);
	}
    }
    
    apps.forEach(function (app)  { 
	abs(queryVector, function (absqv) {
	    formApplicationVector (app, queryVector, function (applicationVector) {
		abs (applicationVector, function (absav) {
		    dotProduct (queryVector, applicationVector, function (dp) {
			simi = dp/Math.sqrt(absqv*absav);
			applicationScores[app] = simi;
			processApplication();
		    });
		});
	    });
	});
    });
}



function sortedApplicationsByDistance (distanceVectors, cb) {
    
    var tuples = [];

    for (var key in distanceVectors) tuples.push([key, distanceVectors[key]]);
    tuples.sort(function(a, b) { return a[1] < b[1] ? 1 : a[1] > b[1] ? -1 : 0 });
    cb(tuples);
}



function formApplicationVector (app, queryVector, cb) {
    var applicationVector = {};
    var cnt = 0;
    
    var processFeatures = Object.keys(queryVector).length;
    
    Object.keys(queryVector).forEach (function (feature) {
	
	var processedFeature = function () {
	    processFeatures -= 1;
	    if (processFeatures == 0) {
		cb(applicationVector);
	    }
	}
		
	client.hget ("ntfidf:"+app, feature, function (err, val) {

	    if (val != null) {
		applicationVector[feature] = val;
		processedFeature();
	    }
	    else {
		processedFeature();
	    }
	});
    });
}


function abs(d1, cb) {
    var val = 0;
    for (key in d1) {
	val = d1[key]*d1[key] + val;
    }
    cb(Math.sqrt(val));
}

function dotProduct (d1, d2, cb) {
    var val = 0;
    
    for(key in d1) {
	if (d2[key] != null) {
	    val = val+d1[key]*d2[key];
	}

    }
    cb(val);
    
}


function returnScoredApps (inputprocs, limit, cb) {
    var scoredApps = [];
    
    getQueryVector(inputprocs, function (queryVector) {
	getRelevantApplications (queryVector, function (applications) {
	    computeDistances (queryVector, applications, function(distanceVectors)  {
		sortedApplicationsByDistance (distanceVectors, function (sortedApplications) {
		    var length = sortedApplications.length;
		    for (var cnt = 0; cnt < limit;  cnt++) {
			scoredApps.push([sortedApplications[cnt][0], sortedApplications[cnt][1]]);
		    }
		    cb(scoredApps);
		});
	    });
	});
    });
}
		    

function formatResponse(req) {
    
    return {serverName: 'SAMPLESVR1', applications: [{app: 'appl1', conf: '50'}, {app: 'appl2', conf: '10'}]};    
}


var server = app.listen(3000, function () {
  var host = server.address().address
  var port = server.address().port
  console.log('App for metrics listening at http://%s:%s', host, port)
})
	

