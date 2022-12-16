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
use Net::FTP;
use Data::Dumper;
use XML::Simple;
use DateTime;
use utf8;
use Encode;

# use DateTime::Format::Duration;

our $configFile;
our $debug                 = 0;
our $xmlconf = "/openils/conf/opensrf.xml";
our $reCreateDBSchema;
our $dbHandler;
our $baseTemp;

our $log;
our $logWrites = 0;
our %conf;
our @includedOrgUnitIDs;


GetOptions(
    "xmlconfig=s"     => \$xmlconf,
    "config=s"       => \$configFile,
    "debug"           => \$debug,
    "recreatedb" => \$reCreateDBSchema,
) or printHelp();


checkCMDArgs();

setupDB();

start();



sub start {

    log_write(" ---------------- Script Starting ---------------- ", 1);
    print "Executing job tail the log for information (".$conf{"logfile"}.")\n";

    $baseTemp = $conf{"tempdir"};
    $baseTemp =~ s/\/$//;
    $baseTemp .= '/';
    my $libraryname = trim($conf{"libraryname"});
    @includedOrgUnitIDs = @{getOrgUnits($libraryname)};
    makeDataFile("circs");
    my $subject = trim($conf{"emailsubjectline"});


    # log_write("Received $count rows from database - writing to $file", 1);
    # log_write("finished $file", 1);
    # my @files = ($file);

    # sendftp($conf{"ftphost"},$conf{"ftplogin"},$conf{"ftppass"},$conf{"remote_directory"}, \@files, $log);

    # my @tolist = ($conf{"alwaysemail"});
    # my $email = new email($conf{"fromemail"},\@tolist,$valid,1,\%conf);
    # my $afterProcess = DateTime->now(time_zone => "local");
    # my $difference = $afterProcess - $dt;
    # my $format = DateTime::Format::Duration->new(pattern => '%M:%S');
    # my $duration =  $format->format_duration($difference);
    # my @s = split(/\//,$file);
    # my $displayFilename = @s[$#s];
    # $email->send("$subject","Duration: $duration\r\nTotal Extracted: $count\r\nFilename: $displayFilename\r\nFTP Directory: ".$conf{"remote_directory"}."\r\nThis is a full replacement\r\n-Evergreen Perl Squad-");
    # foreach(@files)
    # {
        # unlink $_ or warn "Could not remove $_\n";
    # }

    log_write(" ---------------- Script Ending ---------------- ", 1);
    close($log);
}

sub makeDataFile
{
    my $type = shift;
    my $dt = DateTime->now(time_zone => "local");
    my $fdate = $dt->ymd;
    my %funcMap =
    (
        'bibs' => {"chunk" => "getSQLFunctionChunk", "ids" => "getBibIDs"},
        'items' => {"chunk" => "getSQLFunctionChunk", "ids" => "getItemIDs"},
        'circs' => {"chunk" => "getSQLFunctionChunk", "ids" => "getCircIDs"},
    );
    

    my $filenameprefix = trim($conf{"filenameprefix"}) . '_' .$type;
    my $limit = $conf{'chunksize'} || 10000;
    my $offset = 0;
    my $file = chooseNewFileName($baseTemp, $filenameprefix . "_".$fdate, "tsv");
    print "Creating: $file\n";
    my $fileHandle = startFile($file);
    my @ids;
    my $perlIDEval = '@ids = @{' . $funcMap{$type}{"ids"} . '(\@includedOrgUnitIDs, $limit, $offset)};';
    my $firstTime = 1;
    eval $perlIDEval;
    while($#ids > -1)
    {
        my @data;
        my $perlChunkCode = '@data = @{' . $funcMap{$type}{"chunk"}  . '(\@ids, "' . $type.'")};';
        eval $perlChunkCode;

        #last row has header info
        my $h = pop @data;

        if($firstTime)
        {
            $firstTime = 1;
            my @head = ([@{$h}]);
            writeData(\@head, $fileHandle);
        }
        writeData(\@data, $fileHandle);
        $offset += $limit;
        eval $perlIDEval;
        undef $perlChunkCode;
        undef $h;
    }
    close($fileHandle);
}

sub writeData
{
    my $dataRef = shift;
    my @data = @{$dataRef};
    my $fileHandle = shift;
    my $output = '';
    foreach(@data)
    {
        my @row = @{$_};
        $output .= join("\t", @row);
        $output .= "\n";
    }
    print $fileHandle $output;
}

sub getSQLFunctionChunk
{
    my $idRef = shift;
    my $type = shift;
    my @ids = @{$idRef};
    my $pgArray = join(',', @ids);
    $pgArray = "'{$pgArray}'::BIGINT[]";
    my $query = "select * from libraryiq.get_$type($pgArray)";
    log_write($query) if $debug;
    return dbhandler_query($query);
}

sub getBibIDs
{
    my $libsRef = shift;
    my @libs = $libsRef ? @{$libsRef} : ();
    my $pgLibs = join(',', @libs);
    my $limit = shift;
    my $offset = shift;
    my @ret = ();
    my $query = "
    SELECT bre.id
    FROM
    biblio.record_entry bre
    JOIN asset.call_number acn on(acn.record=bre.id AND NOT acn.deleted)
    WHERE
    acn.owning_lib in( $pgLibs )
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{getDataChunk($query, $limit, $offset)};
    pop @results;
    foreach(@results)
    {
        my @row = @{$_};
        push (@ret, @row[0])
    }
    return \@ret;
}

sub getItemIDs
{
    my $libsRef = shift;
    my @libs = $libsRef ? @{$libsRef} : ();
    my $pgLibs = join(',', @libs);
    my $limit = shift;
    my $offset = shift;
    my @ret = ();
    my $query = "
    SELECT ac.id
    FROM
    asset.copy ac
    JOIN asset.call_number acn on(acn.id=ac.call_number AND NOT ac.deleted AND NOT acn.deleted)
    WHERE
    acn.owning_lib in( $pgLibs )
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{getDataChunk($query, $limit, $offset)};
    pop @results;
    foreach(@results)
    {
        my @row = @{$_};
        push (@ret, @row[0])
    }
    return \@ret;
}

sub getCircIDs
{
    my $libsRef = shift;
    my @libs = $libsRef ? @{$libsRef} : ();
    my $pgLibs = join(',', @libs);
    my $limit = shift;
    my $offset = shift;
    my @ret = ();
    my $query = "
    SELECT acirc.id
    FROM
    action.circulation acirc
    JOIN actor.usr au ON (acirc.usr=au.id)
    JOIN asset.copy ac ON (ac.id=acirc.target_copy)
    JOIN asset.call_number acn on(acn.id=ac.call_number AND NOT ac.deleted AND NOT acn.deleted)
    WHERE
    acn.owning_lib in( $pgLibs )
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{getDataChunk($query, $limit, $offset)};
    pop @results;
    foreach(@results)
    {
        my @row = @{$_};
        push (@ret, @row[0])
    }
    return \@ret;
}

sub getPatronIDs
{
    my $libsRef = shift;
    my @libs = $libsRef ? @{$libsRef} : ();
    my $pgLibs = join(',', @libs);
    my $limit = shift;
    my $offset = shift;
    my @ret = ();
    my $query = "
    SELECT au.id
    FROM
    actor.usr au
    JOIN asset.copy ac ON (ac.id=acirc.target_copy)
    JOIN asset.call_number acn on(acn.id=ac.call_number AND NOT ac.deleted AND NOT acn.deleted)
    WHERE
    au.home_ou in( $pgLibs )
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{getDataChunk($query, $limit, $offset)};
    pop @results;
    foreach(@results)
    {
        my @row = @{$_};
        push (@ret, @row[0])
    }
    return \@ret;
}

sub getDataChunk
{
    my $query = shift;
    my $limit = shift;
    my $offset = shift;
    $query .= "\nLIMIT $limit OFFSET $offset";
    log_write($query) if $debug;
    return dbhandler_query($query);
}

sub getOrgUnits
{
    my $libnames = lc(@_[0]);
    my @ret = ();

    # spaces don't belong here
    $libnames =~ s/\s//g;

    my @sp = split(/,/,$libnames);
    
    my $libs = join ( '$$,$$', @sp);
    $libs = '$$' . $libs . '$$';


    my $query = "
    select id
    from
    actor.org_unit
    where lower(shortname) in ($libs)
    order by 1";
    log_write($query) if $debug;
    my @results = @{dbhandler_query($query)};
    pop @results;
    foreach(@results)
    {
        my @row = @{$_};
        push (@ret, @row[0]);
        if($conf{"include_org_descendants"})
        {
            my @des = @{getOrgDescendants(@row[0])};
            push (@ret, @des);
        }
    }
    return dedupeArray(\@ret);
}

sub getOrgDescendants
{
    my $thisOrg = shift;
    my $query = "select id from actor.org_unit_descendants($thisOrg)";
    my @ret = ();
    log_write($query) if $debug;
    
    my @results = @{dbhandler_query($query)};
    pop @results;
    foreach(@results)
    {
        my @row = @{$_};
        push (@ret, @row[0]);
    }

    return \@ret;
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

sub checkCMDArgs {
    print "Checking command line arguments...\n" if ($debug);

    if ( !-e $xmlconf ) {
        print "$xmlconf does not exist.\nEvergreen database xml configuration " .
        "file does not exist. Please provide a path to the Evergreen opensrf.xml " .
        "database conneciton details. --xmlconf\n";
        exit 1;
    }

    if ( !-e $configFile ) {
        print "$configFile does not exist. Please provide a path to your configuration file: ".
        " --config\n";
        exit 1;
    }

    # Init config
    my $conf = readConfFile($configFile);
    %conf = %{$conf};

    my @reqs = ("logfile","tempdir","libraryname","ftplogin","ftppass","ftphost","remote_directory","emailsubjectline");
    my @missing = ();
    for my $i (0..$#reqs)
    {
        push (@missing, @reqs[$i]) if(!$conf{@reqs[$i]})
    }

    if($#missing > -1)
    {
        print "Please specify the required configuration options:\n";
        print "$_\n" foreach(@missing);
        exit 1;
    }
    if( !-e $conf{"tempdir"})
    {
        print "Temp folder: " . $conf{"tempdir"} . " does not exist.\n";
        exit 1;
    }

    # Init logfile
    log_init();

    log_write("Valid Config", 1);

    undef @missing;
    undef @reqs;

}

sub setupDB {

    my %dbconf = %{getDBconnects($xmlconf)};
    log_write("got XML db connections", 1);
    dbhandler_setupConnection($dbconf{"db"}, $dbconf{"dbhost"}, $dbconf{"dbuser"}, $dbconf{"dbpass"}, $dbconf{"port"});

    my $query = "DROP SCHEMA libraryiq CASCADE";
    dbhandler_update($query) if($reCreateDBSchema);
    my $query = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'libraryiq'";
    my @results = @{dbhandler_query($query)};
    pop @results; #don't care about headers
    if($#results==-1)
    {
        log_write("Creating Schema: libraryiq", 1);
        $query = "CREATE SCHEMA libraryiq";
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.flatten_array(TEXT[])
      RETURNS text AS
    $BODY$
      DECLARE
        arr ALIAS FOR $1;
        output TEXT;
        txt TEXT;
        count INT := 0;

      BEGIN

      output = '[';

      FOREACH txt IN ARRAY arr
      LOOP
        output = output || '"' || naco_normalize( txt ) || '",';
        count = count + 1;
      END LOOP;

      output = LEFT(output, -1) || ']';

      IF count = 0 then
        RETURN '';
      END IF;

      RETURN output;

      END;
    $BODY$
      LANGUAGE plpgsql STABLE STRICT;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.attempt_isbn(bigint)
      RETURNS text AS
    $BODY$
      DECLARE
        bib ALIAS FOR $1;
        output TEXT[];
        loopvar TEXT;
      BEGIN

      -- mine metabib.real_full_rec
        FOR loopvar IN
          SELECT regexp_replace(value,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g')
          FROM metabib.real_full_rec where
            tag='020' AND
            subfield in('a','z') AND
            length(regexp_replace(value,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g')) < 14 AND
            record = bib
            ORDER BY length(regexp_replace(value,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g')) DESC, regexp_replace(value,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g')
        LOOP
          output = array_append(output, loopvar);
        END LOOP;

      -- Fail over to reporter.materialized_simple_record
        IF array_length(output, 1) = 0
        THEN
          FOR loopvar IN
            SELECT regexp_replace(isbn,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g'),* FROM
            (
              SELECT unnest(isbn) AS "isbn" FROM
              reporter.materialized_simple_record
              WHERE
              id = bib
            ) AS a
            WHERE
            length(regexp_replace(isbn,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g')) < 14
            ORDER BY length(regexp_replace(isbn,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g')) DESC, regexp_replace(isbn,'^\s*([^\s\(\)\:\.]*)[\s\(\)\:\.]*.*$','\1','g')
          LOOP
            output = array_append(output, loopvar);
          END LOOP;
        END IF;
        IF array_length(output, 1) = 0
        THEN
        FOR loopvar IN
            EXECUTE E'SELECT \'\' AS a;'
          LOOP
            output = array_append(output, loopvar);
          END LOOP;
        END IF;

        RETURN libraryiq.flatten_array(output);

      EXCEPTION
        WHEN OTHERS THEN
          FOR loopvar IN
            EXECUTE E'SELECT \'\' AS a;'
          LOOP
            output = array_append(output, loopvar);
          END LOOP;
      RETURN libraryiq.flatten_array(output);
      END;
    $BODY$
      LANGUAGE plpgsql STABLE STRICT;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.attempt_upc(bigint)
      RETURNS text AS
    $BODY$
      DECLARE
        bib ALIAS FOR $1;
        upc TEXT;
        output TEXT[];
        count INT := 0;

      BEGIN

      FOR upc IN
        SELECT
        unnest(
        XPATH('//marc:datafield[@tag="024"]/marc:subfield[@code="a"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']])
        )
        FROM
        biblio.record_entry bre
        WHERE
        bre.id = bib
      LOOP
        output = array_append(output, upc);
      END LOOP;

      RETURN libraryiq.flatten_array(output);

      END;
    $BODY$
      LANGUAGE plpgsql STABLE STRICT;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.attempt_pub_year(bigint)
      RETURNS text AS
    $BODY$
      DECLARE
        bib ALIAS FOR $1;
        pub_year TEXT;
        output TEXT[];
        count INT := 0;

      BEGIN

      FOR pub_year IN
        SELECT
        unnest(
        XPATH('//marc:datafield[@tag="260"]/marc:subfield[@code="c"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']])
        )
        FROM
        biblio.record_entry
        WHERE
        id = bib
      LOOP
        IF btrim(regexp_replace(pub_year,'[^0-9]','','g')) ~ '^\d{4}$' THEN
            output = array_append( output, btrim(regexp_replace(pub_year,'[^0-9]','','g')) );
        END IF;
      END LOOP;

      IF array_length(output, 1) = 0 or output IS NULL THEN
      
          FOR pub_year IN
            SELECT
            unnest(
            XPATH('//marc:datafield[@tag="264"]/marc:subfield[@code="c"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']])
            )
            FROM
            biblio.record_entry
            WHERE
            id = bib
          LOOP
            IF btrim(regexp_replace(pub_year,'[^0-9]','','g')) ~ '^\d{4}$' THEN
                output = array_append( output, btrim(regexp_replace(pub_year,'[^0-9]','','g')) );
            END IF;
          END LOOP;

      END IF;

      RETURN libraryiq.flatten_array(output);

      END;
    $BODY$
      LANGUAGE plpgsql STABLE STRICT;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.attempt_publisher(bigint)
      RETURNS text AS
    $BODY$
      DECLARE
        bib ALIAS FOR $1;
        pub_year TEXT;
        output TEXT[];
        count INT := 0;

      BEGIN

      FOR pub_year IN
        SELECT
        unnest(
        XPATH('//marc:datafield[@tag="260"]/marc:subfield[@code="b"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']])
        )
        FROM
        biblio.record_entry
        WHERE
        id = bib
      LOOP
        IF length(btrim(pub_year)) > 2 THEN
            output = array_append( output, btrim(pub_year) );
        END IF;
      END LOOP;

      IF array_length(output, 1) = 0 or output IS NULL THEN

          FOR pub_year IN
            SELECT
            unnest(
            XPATH('//marc:datafield[@tag="264"]/marc:subfield[@code="b"]/text()', marc::XML, ARRAY[ARRAY['marc', 'http://www.loc.gov/MARC21/slim']])
            )
            FROM
            biblio.record_entry
            WHERE
            id = bib
          LOOP
            IF length(btrim(pub_year)) > 2 THEN
                output = array_append( output, btrim(pub_year) );
            END IF;
          END LOOP;

      END IF;

      RETURN libraryiq.flatten_array(output);

      END;
    $BODY$
      LANGUAGE plpgsql STABLE STRICT;

splitter
        dbhandler_update($query);

        $query = <<'splitter';
        
    CREATE OR REPLACE FUNCTION libraryiq.get_icon_format(bre_id bigint)
      RETURNS text AS
    $BODY$
    DECLARE
     format_text TEXT := 'unknown';
     
    BEGIN
        SELECT INTO format_text attrs -> 'icon_format' FROM metabib.record_attr WHERE id = bre_id;
        RETURN format_text;	
    END
    $BODY$
      LANGUAGE plpgsql VOLATILE;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.get_bibs(bigint[])
      RETURNS table ( id BIGINT, isbn TEXT, upc TEXT, mat_type TEXT, title TEXT, author TEXT, publication_date TEXT, publisher TEXT ) AS
    $BODY$

      DECLARE
        bib_id_array ALIAS FOR $1;
        bib_id BIGINT;

      BEGIN

        FOREACH bib_id IN ARRAY bib_id_array
        LOOP
            id := bib_id;
            SELECT
            libraryiq.attempt_upc(bre.id::BIGINT),
            libraryiq.attempt_isbn(bre.id::BIGINT),
            libraryiq.get_icon_format(bre.id::BIGINT),
            libraryiq.attempt_pub_year(bre.id::BIGINT),
            libraryiq.attempt_publisher(bre.id::BIGINT)
              INTO upc, isbn, mat_type, publication_date, publisher
              FROM
              biblio.record_entry bre
              WHERE
              bre.id = bib_id;

              SELECT
              regexp_replace(SUBSTRING(rmsr.title FROM 1 FOR 100), '\t', '', 'g'),
              regexp_replace(SUBSTRING(rmsr.author FROM 1 FOR 50), '\t', '', 'g')
              INTO title, author
              FROM reporter.materialized_simple_record rmsr
              WHERE rmsr.id = bib_id;

            RETURN NEXT;
        END LOOP;

      END;

    $BODY$
      LANGUAGE plpgsql VOLATILE;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.get_items(bigint[])
      RETURNS table ( copyid BIGINT, barcode TEXT, bibid BIGINT,
      isbn TEXT, upc TEXT, collection_code TEXT, mat_type TEXT,
      branch_location TEXT, owning_branch TEXT, call_number TEXT,
      shelf_location TEXT, create_date TEXT, status TEXT, last_checkout TEXT, last_checkin_date TEXT,
      due_date TEXT, ytd_circ_count BIGINT, circ_count BIGINT ) AS
    $BODY$

      DECLARE
        copy_id_array ALIAS FOR $1;
        cid BIGINT;

      BEGIN

        FOREACH cid IN ARRAY copy_id_array
        LOOP
            SELECT
            ac.id,
            ac.barcode,
            acn.record,
            libraryiq.attempt_isbn(acn.record::BIGINT),
            libraryiq.attempt_upc(acn.record::BIGINT),
            acl.name,
            ac.circ_modifier,
            aou_circ.shortname,
            aou_owner.shortname,
            btrim(regexp_replace(concat(acnp.label, ' ', acn.label, ' ', acns.label), '\s{2,}', ' ')),
            acl.name,
            ac.create_date,
            ccs.name,
            chkoutdate.lastcheckout,
            chkindate.lastcheckin,
            duedate.due,
            COALESCE(ytd.ytdcirccount, 0),
            COALESCE(circcount.tcirccount, 0)
            
              INTO
              copyid, barcode, bibid, isbn, upc, collection_code, mat_type,
              branch_location, owning_branch, call_number, shelf_location,
              create_date, status, last_checkout, last_checkin_date, due_date,
              ytd_circ_count, circ_count
              FROM
              asset.copy ac
              JOIN asset.call_number acn ON (acn.id=ac.call_number AND NOT ac.deleted AND NOT acn.deleted)
              LEFT JOIN asset.copy_location acl ON (acl.id=ac.location)
              JOIN actor.org_unit aou_owner ON (acn.owning_lib=aou_owner.id)
              JOIN actor.org_unit aou_circ ON (ac.circ_lib=aou_circ.id)
              JOIN asset.call_number_prefix acnp ON (acnp.id=acn.prefix)
              JOIN asset.call_number_suffix acns ON (acns.id=acn.suffix)
              JOIN config.copy_status ccs ON (ccs.id=ac.status)
              LEFT JOIN (SELECT COUNT(*) "ytdcirccount" FROM action.circulation acirc2 WHERE acirc2.target_copy=cid AND date_part('year', acirc2.xact_start) = date_part('year', now()) ) ytd ON (1=1)
              LEFT JOIN (SELECT MAX(acirc2.xact_start) "lastcheckout" FROM action.circulation acirc2 WHERE acirc2.target_copy=cid AND acirc2.xact_start IS NOT NULL) chkoutdate ON (1=1)
              LEFT JOIN (SELECT MAX(acirc2.xact_finish) "lastcheckin" FROM action.circulation acirc2 WHERE acirc2.target_copy=cid AND acirc2.xact_finish IS NOT NULL) chkindate ON (1=1)
              LEFT JOIN (SELECT MAX(acirc2.due_date) "due" FROM action.circulation acirc2 WHERE acirc2.target_copy=cid AND acirc2.xact_finish IS NULL) duedate ON (1=1)
              LEFT JOIN (SELECT COUNT(*) "tcirccount" FROM action.circulation acirc2 WHERE acirc2.target_copy=cid) circcount ON (1=1)
              WHERE
              ac.id=cid;
            RETURN NEXT;
        END LOOP;

      END;

    $BODY$
      LANGUAGE plpgsql VOLATILE;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.get_circs(bigint[])
      RETURNS table ( copyid BIGINT, barcode TEXT, bibid BIGINT,
      checkout_date TEXT, checkout_branch TEXT, patron_id BIGINT, due_date TEXT,
      checkin_date TEXT ) AS
    $BODY$

      DECLARE
        circ_id_array ALIAS FOR $1;
        cid BIGINT;

      BEGIN

        FOREACH cid IN ARRAY circ_id_array
        LOOP
            SELECT
            ac.id,
            ac.barcode,
            acn.record,
            acirc.xact_start,
            aou_circ.shortname,
            au.id,
            acirc.due_date,
            acirc.checkin_time
              INTO
              copyid, barcode, bibid,  checkout_date,
              checkout_branch, patron_id, due_date, checkin_date
              FROM
              action.circulation acirc
              JOIN asset.copy ac ON (ac.id=acirc.target_copy)
              JOIN asset.call_number acn ON (acn.id=ac.call_number AND NOT ac.deleted AND NOT acn.deleted)
              LEFT JOIN asset.copy_location acl ON (acl.id=ac.location)
              JOIN actor.usr au ON (acirc.usr=au.id)
              JOIN actor.org_unit aou_circ ON (acirc.circ_lib=aou_circ.id)
              WHERE
              acirc.id=cid;
            RETURN NEXT;
        END LOOP;

      END;

    $BODY$
      LANGUAGE plpgsql VOLATILE;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.get_patrons(bigint[])
      RETURNS table ( patronid BIGINT, exiration_date TEXT, patron_branch TEXT,
      patron_status TEXT, patron_circ_ytd BIGINT, patron_prev_year_circ_count BIGINT,
      patron_circ_count BIGINT, patron_last_active TEXT, patron_last_checkout_date TEXT,
      patron_create_date TEXT, street1 TEXT, street2 TEXT, city TEXT, zip TEXT) AS
    $BODY$

      DECLARE
        patron_id_array ALIAS FOR $1;
        pid BIGINT;

      BEGIN

        FOREACH pid IN ARRAY patron_id_array
        LOOP
            SELECT
            au.id,
            au.expire_date,
            aou.shortname,
            (CASE WHEN au.barred THEN 'barred' WHEN au.deleted THEN 'deleted' WHEN au.active THEN 'active' ELSE 'inactive' END),
            ytd.ytdcirccount,
            prevytd.prvyearcirccount,
            circcount.tcirccount,
            
              INTO
              patronid, exiration_date, patron_branch, patron_status,
              patron_circ_ytd, patron_prev_year_circ_count, patron_circ_count, patron_last_active,
              patron_last_checkout_date, patron_create_date, street1, street2, city, zip
              FROM
              actor.usr au
              JOIN actor.org_unit aou ON (aou.id=au.home_ou)
              LEFT JOIN (SELECT COUNT(*) "ytdcirccount" FROM action.circulation acirc2 WHERE usr=pid AND date_part('year', xact_start) = date_part('year', now()) ) ytd ON (1=1)
              LEFT JOIN (SELECT COUNT(*) "prvyearcirccount" FROM action.circulation acirc2 WHERE acirc2.usr=pid AND (date_part('year', xact_start)::INT - 1) = (date_part('year', now())::INT - 1) ) prevytd ON (1=1)
              LEFT JOIN (SELECT COUNT(*) "tcirccount" FROM action.circulation acirc2 WHERE acirc2.usr=pid ) circcount ON (1=1)
              LEFT JOIN (SELECT MAX(event_time) "lastact" FROM actor.usr_activity aua WHERE aua.usr=pid ) circcount ON (1=1)
              WHERE
              au.id=pid;
            RETURN NEXT;
        END LOOP;

      END;

    $BODY$
      LANGUAGE plpgsql VOLATILE;

splitter
        dbhandler_update($query);
    }
    undef $query;
}


sub dbhandler_query {
    my $querystring = shift;
    my $valuesRef   = shift;
    my @values      = $valuesRef ? @{$valuesRef} : ();
    my @ret;

    my $query;
    $query = $dbHandler->prepare($querystring);
    my $i = 1;
    foreach (@values) {
        $query->bind_param( $i, $_ );
        $i++;
    }
    $query->execute();

    while ( my $row = $query->fetchrow_arrayref() ) {
        push( @ret, [@{$row}] );
    }
    undef($querystring);
    push( @ret, $query->{NAME} );

    return \@ret;
}


sub dbhandler_update
{
    my $querystring = shift;
    my $valRef = shift;
    my @values = ();
    @values = @{$valRef} if $valRef;
    my $q = $dbHandler->prepare($querystring);
    my $i=1;
    foreach(@values)
    {
        my $param = $_;
        if(lc($param eq 'null'))
        {
            $param = undef;
        }
        $q->bind_param($i, $param);
        $i++;
    }
    my $ret = $q->execute();
    return $ret;
}

sub dbhandler_setupConnection {
    my $dbname = shift;
    my $host   = shift;
    my $login  = shift;
    my $pass   = shift;
    my $port   = shift;

    $dbHandler = DBI->connect(
        "DBI:Pg:dbname=$dbname;host=$host;port=$port",
        $login, $pass,
        {
            AutoCommit       => 1,
            post_connect_sql => "SET CLIENT_ENCODING TO 'UTF8'",
            pg_utf8_strings  => 1
        }
    );

}

sub dedupeArray
{
    my $arrRef = shift;
    my @arr = $arrRef ? @{$arrRef} : ();
    my %deduper = ();
    $deduper{$_} = 1 foreach(@arr);
    my @ret = ();
    while ((my $key, my $val) = each(%deduper))
    {
        push (@ret, $key);
    }
    return \@ret;
}

sub startFile
{
    my $filename = shift;
    my $handle;
    open($handle, '> ' . $filename );
    binmode($log, ":utf8");
    return $handle;
}

sub log_write
{
    my $line = shift;
    my $includeTimestamp = shift;
    $logWrites++;
    log_addLogLine($line) if $includeTimestamp;
    print $log "$line\n" if !$includeTimestamp;

    # flush logs to disk every 100 lines, speed issues if we flush with each write
    if( $logWrites % 100 == 0 )
    {
        close($log);
        open($log, '>> ' . $conf{"logfile"} );
        binmode($log, ":utf8");
    }
}

sub log_addLogLine
{
    my $line = shift;

    my $dt   = DateTime->now(time_zone => "local");
    my $date = $dt->ymd;
    my $time = $dt->hms;

    my $datetime = makeEvenWidth("$date $time", 20);
    print $log $datetime,": $line\n";
}

sub makeEvenWidth  #line, width
{
    my $ret = shift;
    my $width = shift;

    $ret = substr($ret, 0, $width) if(length($ret) > $width);

    $ret .= " " while(length($ret)<$width);
    return $ret;
}

sub log_init
{
    open($log, '> ' . $conf{"logfile"} ) or die "Cannot write to: " . $conf{"logfile"};
    binmode($log, ":utf8");
}

sub readConfFile
{
    my $file = shift;

    my %ret = ();
    my $ret = \%ret;


    my @lines = @{ readFile($file) };

    foreach my $line (@lines)
    {
        $line =~ s/\n//;  #remove newline characters
        $line =~ s/\r//;  #remove newline characters
        my $cur = trim($line);
        if( length($cur) > 0)
        {
            if(substr($cur,0,1)ne"#")
            {
                my @s = split (/=/, $cur);
                my $Name = shift @s;
                my $Value = join('=', @s);
                $$ret{ trim($Name)} = trim($Value);
            }
        }
    }

    return \%ret;
}

sub readFile
{
    my $file = shift;
    my $trys=0;
    my $failed=0;
    my @lines;
    #print "Attempting open\n";
    if( -e $file )
    {
        my $worked = open (inputfile, '< '. $file);
        if(!$worked)
        {
            print "******************Failed to read file*************\n";
        }
        binmode(inputfile, ":utf8");
        while (!(open (inputfile, '< '. $file)) && $trys<100)
        {
            print "Trying again attempt $trys\n";
            $trys++;
            sleep(1);
        }
        if($trys<100)
        {
            #print "Finally worked... now reading\n";
            @lines = <inputfile>;
            close(inputfile);
        }
        else
        {
            print "Attempted $trys times. COULD NOT READ FILE: $file\n";
        }
        close(inputfile);
    }
    else
    {
        print "File does not exist: $file\n";
    }
    return \@lines;
}

sub trim
{
    my $string = shift;
    $string =~ s/^\s+//;
    $string =~ s/\s+$//;
    return $string;
}

sub chooseNewFileName
{
    my $path = shift;
    my $seed = shift;
    my $ext = shift;
    my $ret = "";

    $path = $path . '/' if(substr($path, length($path)-1, 1) ne '/');

    if( -d $path)
    {
        my $num="";
        $ret = $path . $seed . $num . '.' . $ext;
        while(-e $ret)
        {
            if($num eq "")
            {
                $num = -1;
            }
            $num++;
            $ret = $path . $seed . $num . '.' . $ext;
        }
    }
    else
    {
        $ret = 0;
    }

    return $ret;
}

sub sendftp    #server,login,password,remote directory, array of local files to transfer, Loghandler object
{
    my $hostname = shift;
    my $login = shift;
    my $pass = shift;
    my $remotedir = shift;
    my $fileRef = shift;
    my @files = @{$fileRef} if $fileRef;

    log_write("**********FTP starting -> $hostname with $login and $pass -> $remotedir", 1);
    my $ftp = Net::FTP->new($hostname, Debug => 0, Passive=> 1)
    or die $log->addLogLine("Cannot connect to ".$hostname);
    $ftp->login($login,$pass)
    or die $log->addLogLine("Cannot login ".$ftp->message);
    $ftp->cwd($remotedir)
    or die $log->addLogLine("Cannot change working directory ", $ftp->message);
    foreach my $file (@files)
    {
        $log->addLogLine("Sending file $file");
        $ftp->put($file)
        or die $log->addLogLine("Sending file $file failed");
    }
    $ftp->quit
    or die $log->addLogLine("Unable to close FTP connection");
    $log->addLogLine("**********FTP session closed ***************");
}

sub getDBconnects
{
    my $openilsfile = shift;
    my $xml = new XML::Simple;
    my $data = $xml->XMLin($openilsfile);
    my %conf;
    $conf{"dbhost"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{host};
    $conf{"db"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{db};
    $conf{"dbuser"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{user};
    $conf{"dbpass"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{pw};
    $conf{"port"}=$data->{default}->{apps}->{"open-ils.storage"}->{app_settings}->{databases}->{database}->{port};
    ##print Dumper(\%conf);
    return \%conf;

}

exit;


