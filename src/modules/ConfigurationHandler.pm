package ConfigurationHandler;

use strict;
use warnings;

sub readConfFile {
    my $file = shift;

    my %ret = ();
    my $ret = \%ret;

    my @lines = @{ readFile($file) };

    foreach my $line (@lines) {
        $line =~ s/\n//;    #remove newline characters
        $line =~ s/\r//;    #remove newline characters
        my $cur = trim($line);
        if ( length($cur) > 0 ) {
            if ( substr( $cur, 0, 1 ) ne "#" ) {
                my @s     = split( /=/, $cur );
                my $Name  = shift @s;
                my $Value = join( '=', @s );
                $$ret{ trim($Name) } = trim($Value);
            }
        }
    }

    return \%ret;
}


sub log_init {
    open( $log, '> ' . $conf{"logfile"} )
      or die "Cannot write to: " . $conf{"logfile"};
    binmode( $log, ":utf8" );
}

sub checkCMDArgs {
    print "Checking command line arguments...\n" if ($debug);

    if ( !-e $xmlconf ) {
        print "$xmlconf does not exist.\nEvergreen database xml configuration "
          . "file does not exist. Please provide a path to the Evergreen opensrf.xml "
          . "database conneciton details. --xmlconf\n";
        exit 1;
    }

    if ( !-e $configFile ) {
        print "$configFile does not exist. Please provide a path to your configuration file: " . " --config\n";
        exit 1;
    }

    # Init config
    my $conf = readConfFile($configFile);
    %conf = %{$conf};

    my @reqs = (
        "logfile", "tempdir", "libraryname",      "ftplogin",
        "ftppass", "ftphost", "remote_directory", "emailsubjectline",
        "archive", "transfermethod"
    );
    my @missing = ();
    for my $i ( 0 .. $#reqs ) {
        push( @missing, @reqs[$i] ) if ( !$conf{ @reqs[$i] } );
    }

    if ( $#missing > -1 ) {
        print "Please specify the required configuration options:\n";
        print "$_\n" foreach (@missing);
        exit 1;
    }
    if ( !-e $conf{"tempdir"} ) {
        print "Temp folder: " . $conf{"tempdir"} . " does not exist.\n";
        exit 1;
    }

    if ( !-e $conf{"archive"} ) {
        print "Archive folder: " . $conf{"archive"} . " does not exist.\n";
        exit 1;
    }

    if ( lc $conf{"transfermethod"} ne 'sftp' ) {
        print "Transfer method: " . $conf{"transfermethod"} . " is not supported\n";
        exit 1;
    }

    # Init logfile
    log_init();

    log_write( "Valid Config", 1 );

    undef @missing;
    undef @reqs;

}