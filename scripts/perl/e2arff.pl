#!/usr/bin/perl

my $infilename = "/home/milind/Downloads/datacsv3.csv";


my $filename = 'report.arff';
open(my $fh, '>', $filename) or die "Could not open file '$filename' $!";



produce_arff();



sub produce_arff  {
    print $fh "\@RELATION orphan-serverdata\n\n";
    
    produce_application_attribute();
    produce_services_attributes();
   
    produce_clean_data();

    close $fh;
}
    


sub produce_clean_data() {

    open (CSVFILE, $infilename) || die( "Unnable open file");
    $firstLine = <CSVFILE>;

    print $fh "\@DATA\n";
    
    while ($dataLine = <CSVFILE>) {
	chomp $dataLine;
	
	@dataElems = split (',', $dataLine);
	@apps = split (";", $dataElems[1]);	
	
	for (2..$#dataElems) {
	    if ($dataElems[$_] =~ /1/) {
		$dataElems[$_] = "present";
	    }
	    else {
		$dataElems[$_] = "not_present";
	    }
	}
	for(0..$#apps) {
	    if ($apps[$_] =~ /NULL/) {
	    }
	    else {
		$apps[$_] =~ s/\s//g;
		
		print $fh join(",", $apps[$_], @dataElems[2..$#dataElems]);
		print $fh "\n";
	    } 
	}
    }
    
	
	close (CSVFILE);

}

sub produce_services_attributes() {
    open (CSVFILE, $infilename) || die( "Unnable open file");
    $firstLine = <CSVFILE>;
    chomp ($firstLine);
    @dataElems = split (',', $firstLine);
    
    for (2..($#dataElems - 1)) {
	
	$dataElems[$_] =~ s/\s//g;
	
	print $fh "\@ATTRIBUTE ";
	print $fh $dataElems[$_];
	print $fh " {present, not_present} \n";
    }

    print $fh "\n";
    
    
    close (CSVFILE);

}


sub produce_application_attribute() {
    open (CSVFILE, $infilename) || die( "Unnable open file");
    $firstLine = <CSVFILE>;


    my $hashApps = {};
    
    while ($dataLine = <CSVFILE>) {
	chomp $dataLine;
	
	@dataElems = split (',', $dataLine);
	@apps = split (";", $dataElems[1]);	
	
	for(0..$#apps) {
	    if ($apps[$_] =~ /NULL/) {
	    }
	    else {
		$apps[$_] =~ s/\s//g;
		$hashApps{$apps[$_]} = "";
	    } 
	}
    }
    
    
    print $fh "\@ATTRIBUTE APPLICATION { ";
    
    @hshKeys = sort (keys %hashApps);
    
    for (0..(@hshKeys -2) ) {
	print $fh $hshKeys[$_];
	print $fh ",";
    }
    
    print $fh $hshKeys[@hshKeys - 1];
    print $fh "} \n";

    close (CSVFILE);

}

