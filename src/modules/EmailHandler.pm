sub email_setup {
    my ( $from, $emailRecipientArrayRef, $errorFlag, $successFlag ) = @_;

    my $email = {
        fromEmailAddress    => $from,
        emailRecipientArray => $emailRecipientArrayRef,
        notifyError         => $errorFlag,                #true/false
        notifySuccess       => $successFlag,              #true/false
    };

    return email_setupFinalToList($email);
}

sub email_send {
    my ( $email, $subject, $body ) = @_;

    my $message = Email::MIME->create(
        header_str => [
            From    => $email->{fromEmailAddress},
            To      => [ @{ $email->{finalToEmailList} } ],
            Subject => $subject
        ],
        attributes => {
            encoding => 'quoted-printable',
            charset  => 'ISO-8859-1',
        },
        body_str => "$body\n"
    );

    use Email::Sender::Simple qw(sendmail);

    email_reportSummary( $email, $subject, $body );

    sendmail($message);

    print "Sent\n" if $debug;
}

sub email_reportSummary {
    my ( $email, $subject, $body, $attachmentRef ) = @_;
    my @attachments = ();
    @attachments = @{$attachmentRef} if ( ref($attachmentRef) eq 'ARRAY' );

    my $characters = length($body);
    my @lines      = split( /\n/, $body );
    my $bodySize   = $characters / 1024 / 1024;

    print "\n";
    print "From: " . $email->{fromEmailAddress} . "\n";
    print "To: ";
    print "$_, " foreach ( @{ $email->{finalToEmailList} } );
    print "\n";
    print "Subject: $subject\n";
    print "== BODY ==\n";
    print "$characters characters\n";
    print scalar(@lines) . " lines\n";
    print $bodySize . "MB\n";
    print "== BODY ==\n";

    my $fileSizeTotal = 0;
    if ( $#attachments > -1 ) {
        print "== ATTACHMENT SUMMARY == \n";

        foreach (@attachments) {
            $fileSizeTotal += -s $_;
            my $thisFileSize = ( -s $_ ) / 1024 / 1024;
            print "$_: ";
            printf( "%.3f", $thisFileSize );
            print "MB\n";

        }
        $fileSizeTotal = $fileSizeTotal / 1024 / 1024;

        print "Total Attachment size: ";
        printf( "%.3f", $fileSizeTotal );
        print "MB\n";
        print "== ATTACHMENT SUMMARY == \n";
    }

    $fileSizeTotal += $bodySize;
    print "!!!WARNING!!! Email (w/attachments) Exceeds Standard 25MB\n"
      if ( $fileSizeTotal > 25 );
    print "\n";

}

sub email_deDupeEmailArray {
    my $email         = shift;
    my $emailArrayRef = shift;
    my @emailArray    = @{$emailArrayRef};
    my %posTracker    = ();
    my %bareEmails    = ();
    my $pos           = 0;
    my @ret           = ();

    foreach (@emailArray) {
        my $thisEmail = $_;

        print "processing: '$thisEmail'\n" if $debug;

        # if the email address is expressed with a display name,
        # strip it to just the email address
        $thisEmail =~ s/^[^<]*<([^>]*)>$/$1/g if ( $thisEmail =~ m/</ );

        # lowercase it
        $thisEmail = lc $thisEmail;

        # Trim the spaces
        $thisEmail = trim($thisEmail);

        print "normalized: '$thisEmail'\n" if $debug;

        $bareEmails{$thisEmail} = 1;
        if ( !$posTracker{$thisEmail} ) {
            my @a = ();
            $posTracker{$thisEmail} = \@a;
            print "adding: '$thisEmail'\n" if $debug;
        }
        else {
            print "deduped: '$thisEmail'\n" if $debug;
        }
        push( @{ $posTracker{$thisEmail} }, $pos );
        $pos++;
    }
    while ( ( my $email, my $value ) = each(%bareEmails) ) {
        my @a = @{ $posTracker{$email} };

        # just take the first occurance of the duplicate email
        push( @ret, @emailArray[ @a[0] ] );
    }

    return \@ret;
}

sub email_body {
    my $ret = <<'splitter';
Dear staff,

This is a LibraryIQ Evergreen export.

This was a *!!jobtype!!* extraction. We gathered data starting from this date: !!startdate!!

Results:

!!filecount!! file(s)

!!filedetails!!

We transferred the data to:

!!trasnferhost!!!!remotedir!!

Yours Truely,
The Evergreen Server

splitter

}

sub email_setupFinalToList {
    my $email = shift;
    my @ret   = ();

    my @varMap = ( "successemaillist", "erroremaillist" );

    foreach (@varMap) {
        my @emailList = split( /,/, $conf{$_} );
        for my $y ( 0 .. $#emailList ) {
            @emailList[$y] = trim( @emailList[$y] );
        }
        $email->{$_} = \@emailList;
        print "$_:\n" . Dumper( \@emailList ) if $debug;
    }

    undef @varMap;

    push( @ret, @{ $email->{emailRecipientArray} } )
      if ( $email->{emailRecipientArray}->[0] );

    push( @ret, @{ $email->{successemaillist} } )
      if ( $email->{'notifySuccess'} );

    push( @ret, @{ $email->{erroremaillist} } ) if ( $email->{'notifyError'} );

    print "pre dedupe:\n" . Dumper( \@ret ) if $debug;

    # Dedupe
    @ret = @{ email_deDupeEmailArray( $email, \@ret ) };

    print "post dedupe:\n" . Dumper( \@ret ) if $debug;

    $email->{finalToEmailList} = \@ret;

    return $email;
}