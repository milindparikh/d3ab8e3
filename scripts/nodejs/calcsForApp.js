var fs = require('fs'),
    readline = require('readline');

var redis = require("redis"),
    client = redis.createClient();
var sha1 = require('sha1');


client.on("error", function (err) {
    console.log("Error " + err);
});



main();


function main() {
    step = process.argv[2];
    if (step == 1) {
	testiter (process.argv[3]);
	
    }

    if (step == 2) {
	queryVector = [];
	processQuery (process.argv[3]);
    }


    if (step == 3) {
	testQueryVector(process.argv[3]);
	
    }



    if (step == 4) {
	testWhichApplications(process.argv[3]);
	
    }


    if (step == 5) {
	testComputeDistances(process.argv[3], process.argv[4] );
	
    }


    if (step == 9) {
	iterateThroughApps (process.argv[3]);
	calcVectorSpace (process.argv[3]);
	
    }
  
}



function testWhichApplications (inputprocs) {
    getQueryVector(inputprocs, function (queryVector) {
	getRelevantApplications (queryVector, function (applications) {
	    applications.forEach(function (app)  { 
		console.log(app);
	    });
	    
	});
	
    });
    
}



function testComputeDistances (inputprocs, limit) {
    getQueryVector(inputprocs, function (queryVector) {
	getRelevantApplications (queryVector, function (applications) {
	    computeDistances (queryVector, applications, function(distanceVectors)  {
		sortedApplicationsByDistance (distanceVectors, function (sortedApplications) {
		    
		    var length = sortedApplications.length;
		    
		    for (var cnt = 0; cnt < limit;  cnt++) {
			
			console.log( sortedApplications[cnt][0] + "  -- " +  sortedApplications[cnt][1]);
		    }
		    
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
	   

function testQueryVector(inputprocs) {
    
    getQueryVector(inputprocs, function (queryVector) {
	console.log(queryVector);
	client.quit();
    });
}


//   *************************************************************************************
//   ********************************  MAIN FUNCTIONS ************************************
//   *************************************************************************************


function processQuery(inputprocs) {
    getQueryVector(inputprocs, function (queryVector) {
	processQueryVector(queryVector, function (applicationScores) {
	    applicationScoresInJSON = JSON.stringify(applicationScores);
	    console.log(applicationScoresInJSON);
	});
    });
}

	    
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






function processQueryVector(queryVector, cb) {
    
    var applicationScores = {};
    var cnt = 0;
    
    client.smembers("apps", function (err, apps) { 
	apps.forEach(function (app)  { 
	    formApplicationVector (app, queryVector, function (applicationVector) {
		abs(queryVector, function (absqv) {
		    abs (applicationVector, function (absav) {
			dotProduct (queryVector, applicationVector, function (dp) {

			    cnt++;
			    
			    
			    if ( (absqv == 0) | (absav == 0) ) {
			    }
			    else {
				simi = dp/Math.sqrt(absqv*absav);
				applicationScores[app] = simi;
			    }
			    
			    if (cnt == apps.length) {
				cb(applicationScores);
			    }
			});
		    });
		});
	    });
	});
    });
    
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


//   *************************************************************************************
//   **************************** END  MAIN FUNCTIONS ************************************
//   *************************************************************************************



function calcVectorSpace (inputprocs) {
    cntr = 1;
    
    client.scard("apps", function (err, appcount) {
	client.smembers("apps", function (err, apps) { 
	    apps.forEach(function (app)  { 

		cntr++;
		
		inpcntr = 1;
		
		inputprocs.split(",").map(function (val) { 
		    inpcntr++;
		    
		    client.hget("ntfidf:"+app, val, function (err3, ntfidf) {
			if (ntfidf != null) {
			    console.log(app+ "    " + val + "   " + ntfidf);
			}
		    });
		    if (inpcntr == inputprocs.length)  {
			if (cntr == appcount) {
			    client.quit();
			}
		    }
		});
	    });
	});

	
    });
    
}

	

function iterateThroughApps(inputprocs) {
    
    client.smembers("apps", function (err, apps) { 
	apps.forEach(function (app)  { 
	    client.smembers("procs:"+app, function (err2, procs)  {
		if (err2) {
		    console.log(err2) 
		}
		else {
		    
		    procs.forEach(function (proc) {
//			console.log(app + "  " + proc);
			
			client.hget("ntfidf:"+app, proc, function (err3, ntfidf) {
			    console.log(app+ "    " + proc + "   " + ntfidf);
			});

		    });
		}
	    });
	});
    });

}
