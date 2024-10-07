package SFTPHandler;

use strict;
use warnings;
use Net::SFTP::Foreign;

sub send_sftp {
    my $hostname  = shift;
    my $login     = shift;
    my $pass      = shift;
    my $remotedir = shift;
    my $fileRef   = shift;
    my @files     = @{$fileRef} if $fileRef;

    log_write( "**********SFTP starting -> $hostname with $login and $pass -> $remotedir", 1 );
    my $sftp = Net::SFTP->new(
        $hostname,
        debug    => 0,
        user     => $login,
        password => $pass
    ) or return "Cannot connect to " . $hostname;

    foreach my $file (@files) {
        my $dest = $remotedir . "/" . getBareFileName($file);
        log_write( "Sending file $file -> $dest", 1 );
        $sftp->put( $file, $dest )
          or return "Sending file $file failed";
    }
    log_write( "**********SFTP session closed ***************", 1 );
    return 0;
}