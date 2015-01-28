#!/usr/bin/perl

my $adfile = "/home/milind/Downloads/ad_svr_to_proc.csv";
my $cmdbfile = "/home/milind/Downloads/cmdb_svr_to_app.csv";

my $denormalizedapps = "/home/milind/Downloads/den_svr_to_app.csv";
my $denormalizedprocesses = "/home/milind/Downloads/den_svr_to_proc.csv";
my $linkeddata = "/home/milind/Downloads/linked_svr_apps_procs";


my $uniqpfile =  "/home/milind/Downloads/uniqpfile.csv";
my $uniqafile =  "/home/milind/Downloads/uniqafile.csv";

my $arfffile =  "/home/milind/Downloads/arffv1.arff";

    
    


#produce_uniq_processes();
#produce_uniq_apps();

# produce_denormalized_server_to_processes () ;

#produce_denormalized_server_to_apps();

#produce_arff();

produce_merged_app_to_processes();

sub produce_denormalized_server_to_apps {

    open (CMDBFILE, $cmdbfile) || die( "Unnable open file");
    $firstLine = <CMDBFILE>;

    open(my $fh, '>', $denormalizedapps) or die "Could not open file '$filename' $!";
    

    %hashApps = ();

    $aLine = <CMDBFILE>;

    @dataElems = split (',', $aLine);
    $dataElems[0] =~ s/\s//g;
    $dataElems[0] =~ s/[^a-zA-Z0-9\.]*//g;
    $dataElems[1] =~ s/\s//g;
    $dataElems[1] =~ s/[^a-zA-Z0-9\.]*//g;
    
    $currentServer =  $dataElems[0];
    if ($dataElems[1] =~ /NULL/) {
    }
    else {
	$hashApps{$dataElems[1]} = "";
    }
    
    
    while ($aLine = <CMDBFILE>) {
	@dataElems = split (',', $aLine);


	
	$dataElems[0] =~ s/\s//g;
	$dataElems[0] =~ s/[^a-zA-Z0-9\.]*//g;
	$dataElems[1] =~ s/\s//g;
	$dataElems[1] =~ s/[^a-zA-Z0-9\.]*//g;
	
        if ($currentServer =~ /$dataElems[0]/) {
	    if ($dataElems[1] =~ /NULL/) {
	    }
	    else {
		$hashApps{$dataElems[1]} = "";
	    } 
	}
	else {
		@hashSortedApps = sort (keys %hashApps);

		if (@hashSortedApps > 0 ) {
		    print $fh $currentServer;
		    print $fh ",";
		    for(0..(@hashSortedApps-2)) {
			print $fh $hashSortedApps[$_];
			print $fh ";";
		    }
		    print $fh $hashSortedApps[@hashSortedApps-1];
		    print $fh "\n";
		}
		
		$currentServer =  $dataElems[0];
		%hashApps = ();
		if ($dataElems[1] =~ /NULL/) {
		}
		else {
		    
		    $hashApps{$dataElems[1]} = "";
		}

		
	}
	
    }
    
    
    @hashSortedApps = sort (keys %hashApps);
    print $fh $currentServer;
    print $fh ",";
    for(0..(@hashSortedApps-2)) {
	print $hashSortedApps[$_];
	print $fh ";";
    }
    print $fh $hashSortedApps[@hashSortedApps-1];
    print $fh "\n";


    close (CMDBFILE);


    
}


sub produce_denormalized_server_to_processes {

    open (ADFILE, $adfile) || die( "Unnable open file");
    $firstLine = <ADFILE>;

    open(my $fh, '>', $denormalizedprocesses ) or die "Could not open file '$filename' $!";

    %hashProcesses = ();

    $aLine = <ADFILE>;

    @dataElems = split (',', $aLine);
    $dataElems[0] =~ s/\s//g;
    $dataElems[0] =~ s/[^a-zA-Z0-9\.]*//g;
    $dataElems[1] =~ s/\s//g;
    $dataElems[1] =~ s/[^a-zA-Z0-9\.]*//g;
    
    $currentServer =  $dataElems[1];
    $hashProcesses{$dataElems[0]} = "";

    
    while ($aLine = <ADFILE>) {
	@dataElems = split (',', $aLine);


	
	$dataElems[0] =~ s/\s//g;
	$dataElems[0] =~ s/[^a-zA-Z0-9\.]*//g;
	$dataElems[1] =~ s/\s//g;
	$dataElems[1] =~ s/[^a-zA-Z0-9\.]*//g;
	
        if ($currentServer =~ /$dataElems[1]/) {
	    $hashProcesses{$dataElems[0]} = "";
	}
	else {
		@hashSortedProcesses = sort (keys %hashProcesses);
		print $fh $currentServer;
		print $fh ",";
		for(0..(@hashSortedProcesses-2)) {
		    print $fh $hashSortedProcesses[$_];
		    print $fh ",";
		}
		print $fh $hashSortedProcesses[@hashSortedProcesses-1];
		print $fh "\n";
		
		$currentServer =  $dataElems[1];
		%hashProcesses = ();
		$hashProcesses{$dataElems[0]} = "";
		
	}
	
    }
    
    
    @hashSortedProcesses = sort (keys %hashProcesses);
    print $fh $currentServer;
    print $fh ",";
    for(0..(@hashSortedProcesses-2)) {
	print $fh $hashSortedProcesses[$_];
	print $fh ",";
    }
    print $fh $hashSortedProcesses[@hashSortedProcesses-1];
    print $fh "\n";


    close (ADFILE);


    
}


sub produce_uniq_apps {

    open(my $fh, '>', $uniqafile) or die "Could not open file '$filename' $!";

    my %hashApps = ();

    
    open (CMDBFILE, $cmdbfile) || die( "Unnable open file");
    $firstLine = <CMDBFILE>;

    while ($aLine = <CMDBFILE>) {
	@dataElems = split (',', $aLine);
	
	$dataElems[0] =~ s/\s//g;
	$dataElems[0] =~ s/[^a-zA-Z0-9\.]*//g;
	$dataElems[1] =~ s/\s//g;
	$dataElems[1] =~ s/[^a-zA-Z0-9\.]*//g;
	

	
	$hashApps{$dataElems[1]} = "";
    }



    close (CMDBFILE);

    @hshKeys = sort (keys %hashApps);
    
    for (0.. (@hshKeys -1) ) {
	print $fh $hshKeys[$_];
	print $fh  "\n";
    }



}

sub produce_uniq_processes {
    open(my $fh, '>', $uniqpfile) or die "Could not open file '$filename' $!";

    my $hashProcs = {};

    
    open (ADFILE, $adfile) || die( "Unnable open file");
    $firstLine = <ADFILE>;

    while ($aLine = <ADFILE>) {
	@dataElems = split (',', $aLine);
	
	$dataElems[0] =~ s/\s//g;
	$dataElems[0] =~ s/[^a-zA-Z0-9\.]*//g;
	$dataElems[1] =~ s/\s//g;
	$dataElems[1] =~ s/[^a-zA-Z0-9\.]*//g;
	
	
	$hashProcs{$dataElems[0]} = "";
    }



    close (ADFILE);

    @hshKeys = sort (keys %hashProcs);
    
    for (0.. (@hshKeys -1) ) {
	print $fh $hshKeys[$_];
	print $fh "\n";
    }
    

}

    


sub produce_merged_app_to_processes {
    open (SRVTOAPP, $denormalizedapps) || die( "Unnable open file");
    open (SRVTOPROCS, $denormalizedprocesses) || die( "Unnable open file");

    while ($dataLine = <SRVTOAPP> ) {
        chomp $dataLine;
	@dataElems = split(",", $dataLine);
	$hashSrvToApps{$dataElems[0]} = $dataElems[1];
    }

    while ($dataLine = <SRVTOPROCS> ) {
        chomp $dataLine;
	@dataElems = split(",", $dataLine);
	$hashSrvToProcs{$dataElems[0]} = join (",", @dataElems[1..$#dataElems]);
    }

    @sortedServers = sort (keys %hashSrvToApps);

    %appstoprocesses = ();

    
    
    for (0..$#sortedServers) {
	if (exists $hashSrvToProcs{$sortedServers[$_]}) {
	    @appsonserver = split(";", $hashSrvToApps{$sortedServers[$_]});
	    @procsonserver = split(",", $hashSrvToProcs{$sortedServers[$_]});
	    

	    for (0..$#appsonserver) {
		$currentApp = $appsonserver[$_];		
		if (not (exists $appstoprocesses{$currentApp})) {
		    $appstoprocesses{$currentApp} = ();
		}
		for (0..$#procsonserver) {
		    $appstoprocesses{$currentApp}{$procsonserver[$_]} = 1;
		}
	    }
	}
    }


    %processesonapps = ();    
    @sortedApps = sort (keys %appstoprocesses);

    for (0..$#sortedApps) {
	
	@sortedProcesses = 
	    sort (keys $appstoprocesses{$sortedApps[$_]});
	for (0..$#sortedProcesses) {
	    
	    if (not (exists $processesonapps{$sortedProcesses[$_]}) ) {
		
		$processesonapps{$sortedProcesses[$_]} = 1;
	    } 
	    else {
		$processesonapps{$sortedProcesses[$_]}++;
	    }
	}

    }


    @sortedProcessesOnApps = sort (keys %processesonapps);

    %idf = ();
    
    for (0..$#sortedProcessesOnApps) {
	
	$freqInCorpus = $processesonapps{$sortedProcessesOnApps[$_]};
	
	$iidf =  1 + log ($#sortedApps/$freqInCorpus);
	
         
        	
	$idf{$sortedProcessesOnApps[$_]} = $iidf ;
    }
    
    %ntfidf = ();
    
    for (0..$#sortedApps) {	
	$currentApp = $sortedApps[$_];
	
	print " SADD apps ";
	print $currentApp;
	print "\n\n";

	@numprocs = (keys $appstoprocesses{$currentApp});
	
	@sortedProcesses = 
	    sort (keys $appstoprocesses{$currentApp});
	for (0..$#sortedProcesses) {
	    
	    $currentProcess = $sortedProcesses[$_];
	    print " SADD procs:";
	    print $currentApp;
	    print "  " ;
	    print $currentProcess;
	    print "\n\n";
	    
	    
	    $currentIdf = $idf{$sortedProcesses[$_]};
	    $ntfidf = (1/$#numprocs) * $currentIdf;
	    
	    print "HSET ntfidf:";
	    print $currentApp;
	    print "  ";
	    print $sortedProcesses[$_];
			       
	    print "  "; 
	    
	    print  $ntfidf;
	    print "\n\n";
	}
    }
}


sub produce_arff  {
    open( $fh, '>', $arfffile) or die "Could not open file '$filename' $!";
	
    print $fh "\@RELATION orphan-serverdata\n\n";

    
    produce_application_attribute();
    produce_services_attributes();
   
    produce_clean_data();

    close $fh;
}
    


sub produce_clean_data() {

    open (UNIQP, $uniqpfile) || die( "Unnable open file");
    open (SRVTOAPP, $denormalizedapps) || die( "Unnable open file");
    open (SRVTOPROCS, $denormalizedprocesses) || die( "Unnable open file");
    
    %hashSrvToApps = ();
    

    while ($dataLine = <SRVTOAPP> ) {
        chomp $dataLine;
	
	@dataElems = split(",", $dataLine);
	$hashSrvToApps{$dataElems[0]} = $dataElems[1];
    }
    close (SRVTOAPP);
    
    $countP = 0;
    %hashPToNum = ();
    
    while ($dataLine = <UNIQP>) {
	chomp $dataLine;
	$hashPToNum{$dataLine} = $countP;
	$countP++;
    }

    @masterBlank = ('?') x $countP;
    

    close (UNIQP);
    

    $intCount = 0;
    
    while ($dataLine = <SRVTOPROCS> ) {
	$intCount++;
	
	if ($intCount == 100) {
	    last;
	}
	
	chomp $dataLine;
	@copyBlank = @masterBlank;
	@dataElems = split (",", $dataLine);
	
	for (1..$#dataElems) {
	    $copyBlank[$hashPToNum{$dataElems[$_]}] = 'present';
	}
	
	@splitApps = split (";", $hashSrvToApps{$dataElems[0]});
	
	for (0..$#splitApps) {
	    print $fh $splitApps[$_];
	    print $fh ",";
	    print $fh join (",", @copyBlank);
	    print $fh "\n";
	}
    }
    close(SRVTOPROCS);

}

sub produce_services_attributes() {
    open (CSVFILE, $uniqpfile) || die( "Unnable open file");

    while ($dataLine = <CSVFILE>) {
	chomp $dataLine;
	print $fh "\@ATTRIBUTE ";
	print $fh $dataLine;
	print $fh " {present} \n";
    }
    
    print $fh "\@DATA\n";    
    close (CSVFILE);

}


sub produce_application_attribute() {
    open (CSVFILE, $uniqafile) || die( "Unnable open file");

    my %hashApps = ();
    
    while ($dataLine = <CSVFILE>) {
	chomp $dataLine;
	$hashApps{$dataLine} = "";
    }
    
    print $fh "\@ATTRIBUTE APPLICATION { ";
    
    @hshKeys = sort (keys %hashApps);
    
    for (1..(@hshKeys -2) ) {

	    print $fh $hshKeys[$_];
	    print $fh ",";
    }
    
    print $fh $hshKeys[@hshKeys - 1];
    print $fh "} \n";

    close (CSVFILE);

}

