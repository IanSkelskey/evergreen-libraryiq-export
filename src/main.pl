#!/usr/bin/perl

# Copyright (C) 2022 MOBIUS
# Author: Blake Graham-Henderson <blake@mobiusconsortium.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# ---------------------------------------------------------------

use strict;

use Getopt::Long;
use DBD::Pg;
use Net::SFTP;
use Data::Dumper;
use XML::Simple;
use DateTime;
use utf8;
use Encode;
use Email::MIME;

our $configFile;
our $debug   = 0;
our $xmlconf = "/openils/conf/opensrf.xml";
our $reCreateDBSchema;
our $full;
our $dbHandler;
our $baseTemp;

our $log;
our $logWrites = 0;
our %conf;
our @includedOrgUnitIDs;
our $historyID;
our $lastUpdatePGDate;
our $jobtype = "diff";

GetOptions(
    "xmlconfig=s" => \$xmlconf,
    "config=s"    => \$configFile,
    "debug"       => \$debug,
    "recreatedb"  => \$reCreateDBSchema,
    "full"        => \$full,
) or printHelp();

checkCMDArgs();

setupDB();

start();

sub start {

    log_write( " ---------------- Script Starting ---------------- ", 1 );
    print "Executing job tail the log for information (" . $conf{"logfile"} . ")\n";

    $baseTemp = $conf{"tempdir"};
    $baseTemp =~ s/\/$//;
    $baseTemp .= '/';
    my $libraryname = trim( $conf{"libraryname"} );
    @includedOrgUnitIDs = @{ getOrgUnits($libraryname) };
    my $pgLibs = makeLibList( \@includedOrgUnitIDs );

    checkHistory($pgLibs);
    my @loops = (qw/bibs items circs patrons holds/);
    my %res   = ();
    $res{$_} = makeDataFile( $_, $pgLibs ) foreach @loops;
    log_write( Dumper( \%res ) );
    updateHistory();
    my $tarFile;
    my $tarFileBareFileName;
    $tarFile = makeTarGZ( \%res ) if ( $conf{"compressoutput"} );

    my $filecount   = 0;
    my $filedetails = '';
    my @files;
    $tarFileBareFileName = getBareFileName($tarFile);

    @files = ($tarFile) if ($tarFile);
    while ( ( my $key, my $val ) = each(%res) ) {
        my @thisRes  = @{$val};
        my $filename = @thisRes[0];
        my $rcount   = @thisRes[1];
        $filecount++;
        push( @files, $filename ) if ( !$tarFile );
        unlink $filename or warn "Could not remove $filename\n" if ($tarFile);
        $filename = getBareFileName($filename);
        $filedetails .= "$key [$filename]\t\t$rcount record(s)\n";
    }
    $filedetails .= "\nThese were compressed into '$tarFileBareFileName'"
      if $tarFile;

    my $body = email_body();
    $body =~ s/!!jobtype!!/$jobtype/g;
    $body =~ s/!!filedetails!!/$filedetails/g;
    $body =~ s/!!filecount!!/$filecount/g;
    $body =~ s/!!startdate!!/$lastUpdatePGDate/g;
    $body =~ s/!!trasnferhost!!/$conf{"ftphost"}/g;
    $body =~ s/!!remotedir!!/$conf{"remote_directory"}/g;
    $body =~ s/::TIMESTAMPTZ//g;

    my $ftpFail =
      send_sftp( $conf{"ftphost"}, $conf{"ftplogin"}, $conf{"ftppass"}, $conf{"remote_directory"}, \@files );

    if ( !$tarFile ) {
        foreach (@files) {
            my $destFile = getBareFileName($_);
            my $cmd      = "mv '$_' '" . $conf{"archive"} . "/$destFile'";
            log_write( "Running: $cmd", 1 );
            system($cmd);
        }
    }
    my $subject = trim( $conf{"emailsubjectline"} );
    $subject .= ' - FTP FAIL' if $ftpFail;
    $body = "$ftpFail" if $ftpFail;
    my @tolist = ( $conf{"alwaysemail"} );
    my $email;
    $email = email_setup( $conf{"fromemail"}, \@tolist, $ftpFail, !$ftpFail,
        $conf{"erroremaillist"}, $conf{"successemaillist"} );
    email_send( $email, $subject, $body );

    log_write( " ---------------- Script Ending ---------------- ", 1 );
    close($log);
}

sub makeDataFile {
    my $type    = shift;
    my $pgLibs  = shift;
    my $dt      = DateTime->now( time_zone => "local" );
    my $fdate   = $dt->ymd;
    my %funcMap = (
        'bibs'    => { "chunk" => "getSQLFunctionChunk", "ids" => "getBibIDs" },
        'items'   => { "chunk" => "getSQLFunctionChunk", "ids" => "getItemIDs" },
        'circs'   => { "chunk" => "getSQLFunctionChunk", "ids" => "getCircIDs" },
        'patrons' => { "chunk" => "getSQLFunctionChunk", "ids" => "getPatronIDs" },
        'holds'   => { "chunk" => "getSQLFunctionChunk", "ids" => "getHoldIDs" },
    );

    my $filenameprefix = trim( $conf{"filenameprefix"} ) . '_' . $jobtype . '_' . $type;
    my $limit          = $conf{'chunksize'} || 500;
    my $offset         = 0;
    my $file           = chooseNewFileName( $baseTemp, $filenameprefix . "_" . $fdate, "tsv" );
    print "Creating: $file\n";
    my $fileHandle = startFile($file);
    my @ids;
    my $count = 0;

    my $perlIDEval = '@ids = @{' . $funcMap{$type}{"ids"} . '($pgLibs, $limit, $offset)};';
    my $firstTime  = 1;
    eval $perlIDEval;
    while ( $#ids > -1 ) {
        my @data;
        my $perlChunkCode = '@data = @{' . $funcMap{$type}{"chunk"} . '(\@ids, "' . $type . '")};';
        eval $perlChunkCode;

        #last row has header info
        my $h = pop @data;

        if ($firstTime) {
            $firstTime = 0;
            my @head = ( [ @{$h} ] );
            writeData( \@head, $fileHandle );
        }
        $count += $#data;
        writeData( \@data, $fileHandle );
        $offset += $limit;

        eval $perlIDEval;
        undef $perlChunkCode;
        undef $h;
    }
    close($fileHandle);
    my @ret = ( $file, $count );
    return \@ret;
}

sub writeData {
    my $dataRef    = shift;
    my @data       = @{$dataRef};
    my $fileHandle = shift;
    my $output     = '';
    foreach (@data) {
        my @row = @{$_};
        $output .= join( "\t", @row );
        $output .= "\n";
    }
    print $fileHandle $output;
}

sub printHelp {
    my $help = "Usage: ./extract_libraryiq.pl [OPTION]...

    This program automates the process of exporting data from an Evergreen database and delivering
    the output to the LibraryIQ service. This software requires a config file that details the FTP
    connection information as well as notification email address(es) and participating library.

    --xmlconfig         path to Evergreen opensrf.xml file for DB connection details, default /openils/conf/opensrf.xml
    --debug             Set debug mode for more verbose output.
    ";

    print $help;
    exit 0;
}

sub checkHistory {
    my $libs    = shift;
    my $key     = makeLibKey($libs);
    # subtracting 5 hours for a little insurance that we cover any run time from the previous run
    my $query   = "SELECT id, (CASE WHEN last_run IS NOT NULL THEN (last_run - '5 hours'::INTERVAL) ELSE NULL END) FROM libraryiq.history WHERE key = \$1";
    log_write($query) if $debug;
    my @vals    = ($key);
    my @results = @{ dbhandler_query( $query, \@vals ) };
    pop @results;
    if ( $#results == -1 ) {
        log_write( "Creating new history entry: $key", 1 );
        my $q = "INSERT INTO libraryiq.history(key) VALUES(\$1)";
        dbhandler_update( $q, \@vals );
        $jobtype = "full";
    }
    @results = @{ dbhandler_query( $query, \@vals ) };
    pop @results;
    foreach (@results) {
        my @row = @{$_};
        $historyID        = @row[0];
        $lastUpdatePGDate = "'" . @row[1] . "'::TIMESTAMPTZ";
    }
    $lastUpdatePGDate = "'1000-01-01'::TIMESTAMPTZ" if ($full);
    $jobtype = "full" if ($full);
}

sub updateHistory {
    return if !$historyID;
    my $query = "UPDATE libraryiq.history SET last_run = now() WHERE id = \$1";
    my @vals  = ($historyID);
    dbhandler_update( $query, \@vals );
}

sub makeLibKey {
    my $libs = shift;
    $libs =~ s/\s//g;
    $libs = $conf{"filenameprefix"} . "_" . $libs;
    return $libs;
}

sub startFile {
    my $filename = shift;
    my $handle;
    open( $handle, '> ' . $filename );
    binmode( $log, ":utf8" );
    return $handle;
}


sub log_addLogLine {
    my $line = shift;

    my $dt   = DateTime->now( time_zone => "local" );
    my $date = $dt->ymd;
    my $time = $dt->hms;

    my $datetime = makeEvenWidth( "$date $time", 20 );
    print $log $datetime, ": $line\n";
}



sub readFile {
    my $file   = shift;
    my $trys   = 0;
    my $failed = 0;
    my @lines;

    #print "Attempting open\n";
    if ( -e $file ) {
        my $worked = open( inputfile, '< ' . $file );
        if ( !$worked ) {
            print "******************Failed to read file*************\n";
        }
        binmode( inputfile, ":utf8" );
        while ( !( open( inputfile, '< ' . $file ) ) && $trys < 100 ) {
            print "Trying again attempt $trys\n";
            $trys++;
            sleep(1);
        }
        if ( $trys < 100 ) {

            #print "Finally worked... now reading\n";
            @lines = <inputfile>;
            close(inputfile);
        }
        else {
            print "Attempted $trys times. COULD NOT READ FILE: $file\n";
        }
        close(inputfile);
    }
    else {
        print "File does not exist: $file\n";
    }
    return \@lines;
}



sub chooseNewFileName {
    my $path = shift;
    my $seed = shift;
    my $ext  = shift;
    my $ret  = "";

    $path = $path . '/' if ( substr( $path, length($path) - 1, 1 ) ne '/' );

    if ( -d $path ) {
        my $num = "";
        $ret = $path . $seed . $num . '.' . $ext;
        while ( -e $ret ) {
            if ( $num eq "" ) {
                $num = -1;
            }
            $num++;
            $ret = $path . $seed . $num . '.' . $ext;
        }
    }
    else {
        $ret = 0;
    }

    return $ret;
}

sub makeTarGZ {
    print "taring...\n";
    my $dataref = shift;
    my %res     = $dataref ? %{$dataref} : ();
    my @files   = ();
    while ( ( my $key, my $val ) = each(%res) ) {
        my @thisRes  = @{$val};
        my $filename = @thisRes[0];
        print $filename . "\n";
        $filename =~ s/$baseTemp//g;
        print $filename . "\n";
        push( @files, $filename );
    }
    my $dt             = DateTime->now( time_zone => "local" );
    my $fdate          = $dt->ymd;
    my $filenameprefix = trim( $conf{"filenameprefix"} );
    my $tarFileName    = chooseNewFileName( $conf{"archive"}, "$filenameprefix" . "_$fdate", "tar.gz" );
    my $systemCMD      = "cd '$baseTemp' && tar -zcvf '$tarFileName' ";
    $systemCMD .= "'$_' " foreach (@files);
    log_write( "Running\n$systemCMD", 1 );
    my $worked = system($systemCMD);
    return $tarFileName if $worked eq '0';
    return 0;
}

sub getBareFileName {
    my $fullFile = shift;
    my @s        = split( /\//, $fullFile );
    return pop @s;
}

sub getDBconnects {
    my $openilsfile = shift;
    my $xml         = new XML::Simple;
    my $data        = $xml->XMLin($openilsfile);
    my %conf;
    $conf{"dbhost"} = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
    $conf{"db"}     = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
    $conf{"dbuser"} = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
    $conf{"dbpass"} = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
    $conf{"port"}   = $data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
    ##print Dumper(\%conf);
    return \%conf;

}

exit;

