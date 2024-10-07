package Utils;

use strict;
use warnings;

sub dedupeArray {
    my $arrRef  = shift;
    my @arr     = $arrRef ? @{$arrRef} : ();
    my %deduper = ();
    $deduper{$_} = 1 foreach (@arr);
    my @ret = ();
    while ( ( my $key, my $val ) = each(%deduper) ) {
        push( @ret, $key );
    }
    @ret = sort @ret;
    return \@ret;
}

sub trim {
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}

sub makeEvenWidth    #line, width
{
    my $ret   = shift;
    my $width = shift;

    $ret = substr( $ret, 0, $width ) if ( length($ret) > $width );

    $ret .= " " while ( length($ret) < $width );
    return $ret;
}

sub log_write {
    my $line             = shift;
    my $includeTimestamp = shift;
    $logWrites++;
    log_addLogLine($line) if $includeTimestamp;
    print $log "$line\n"  if !$includeTimestamp;

    # flush logs to disk every 100 lines, speed issues if we flush with each write
    if ( $logWrites % 100 == 0 ) {
        close($log);
        open( $log, '>> ' . $conf{"logfile"} );
        binmode( $log, ":utf8" );
    }
}
