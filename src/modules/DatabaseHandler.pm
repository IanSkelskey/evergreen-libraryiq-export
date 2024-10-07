sub setupDB {

    my %dbconf = %{ getDBconnects($xmlconf) };
    log_write( "got XML db connections", 1 );
    dbhandler_setupConnection( $dbconf{"db"}, $dbconf{"dbhost"}, $dbconf{"dbuser"},
        $dbconf{"dbpass"}, $dbconf{"port"} );

    my $query = "DROP SCHEMA libraryiq CASCADE";
    dbhandler_update($query) if ($reCreateDBSchema);
    my $query   = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'libraryiq'";
    my @results = @{ dbhandler_query($query) };
    pop @results;    #don't care about headers
    if ( $#results == -1 ) {
        log_write( "Creating Schema: libraryiq", 1 );
        $query = "CREATE SCHEMA libraryiq";
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE TABLE libraryiq.history
    (
        id serial NOT NULL,
        key TEXT NOT NULL,
        last_run TIMESTAMP WITH TIME ZONE DEFAULT '1000-01-01'::TIMESTAMPTZ
    );

splitter
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
              LEFT JOIN (SELECT COUNT(*) "ytdcirccount" FROM action.all_circulation acirc2 WHERE acirc2.target_copy=cid AND date_part('year', acirc2.xact_start) = date_part('year', now()) ) ytd ON (1=1)
              LEFT JOIN (SELECT MAX(acirc2.xact_start) "lastcheckout" FROM action.all_circulation acirc2 WHERE acirc2.target_copy=cid AND acirc2.xact_start IS NOT NULL) chkoutdate ON (1=1)
              LEFT JOIN (SELECT MAX(acirc2.xact_finish) "lastcheckin" FROM action.all_circulation acirc2 WHERE acirc2.target_copy=cid AND acirc2.xact_finish IS NOT NULL) chkindate ON (1=1)
              LEFT JOIN (SELECT MAX(acirc2.due_date) "due" FROM action.all_circulation acirc2 WHERE acirc2.target_copy=cid AND acirc2.xact_finish IS NULL) duedate ON (1=1)
              LEFT JOIN (SELECT COUNT(*) "tcirccount" FROM action.all_circulation acirc2 WHERE acirc2.target_copy=cid) circcount ON (1=1)
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
              action.all_circulation acirc
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
      patron_create_date TEXT, street1 TEXT, street2 TEXT, city TEXT, state TEXT, zip TEXT) AS
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
            usr_activity.lastact,
            last_usr_checkout.lastcirc,
            au.create_date,
            addr.street1,
            addr.street2,
            addr.city,
            addr.state,
            addr.post_code
              INTO
              patronid, exiration_date, patron_branch, patron_status,
              patron_circ_ytd, patron_prev_year_circ_count, patron_circ_count, patron_last_active,
              patron_last_checkout_date, patron_create_date, street1, street2, city, state, zip
              FROM
              actor.usr au
              JOIN actor.org_unit aou ON (aou.id=au.home_ou)
              LEFT JOIN (SELECT COUNT(*) "ytdcirccount" FROM action.all_circulation acirc2 WHERE acirc2.usr=pid AND date_part('year', xact_start) = date_part('year', now()) ) ytd ON (1=1)
              LEFT JOIN (SELECT COUNT(*) "prvyearcirccount" FROM action.all_circulation acirc2 WHERE acirc2.usr=pid AND (date_part('year', xact_start)::INT - 1) = (date_part('year', now())::INT - 1) ) prevytd ON (1=1)
              LEFT JOIN (SELECT COUNT(*) "tcirccount" FROM action.all_circulation acirc2 WHERE acirc2.usr=pid ) circcount ON (1=1)
              LEFT JOIN (SELECT MAX(event_time) "lastact" FROM actor.usr_activity aua WHERE aua.usr=pid ) usr_activity ON (1=1)
              LEFT JOIN (SELECT MAX(xact_start) "lastcirc" FROM action.all_circulation acirc2 WHERE acirc2.usr=pid ) last_usr_checkout ON (1=1)
              LEFT JOIN (SELECT MAX(id) "id" FROM actor.usr_address auaddress WHERE auaddress.usr=pid AND auaddress.address_type='MAILING' ) auaa ON (1=1)
              LEFT JOIN (SELECT auadd.id,auadd.street1,auadd.street2,auadd.city,auadd.state,auadd.post_code FROM actor.usr_address auadd WHERE auadd.usr=pid ) addr ON (addr.id=auaa.id)
              WHERE
              au.id=pid;
            RETURN NEXT;
        END LOOP;

      END;

    $BODY$
      LANGUAGE plpgsql VOLATILE;

splitter
        dbhandler_update($query);

        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.attempt_hold_detail(bigint)
      RETURNS table ( bibid BIGINT, current_request_count BIGINT) AS
    $BODY$

      DECLARE
        hold_id ALIAS FOR $1;
        temp_add BIGINT := 0;
        temp_mmr BIGINT := -1;
        temp_pickup_lib INT := -1;
        temp_pickup_libs int[];

      BEGIN

        current_request_count := 0;
        SELECT
        (
            CASE
            WHEN ahr.hold_type='T' THEN ahr.target
            WHEN ahr.hold_type='C' THEN ac_hold.record
            WHEN ahr.hold_type='V' THEN acn_hold.record
            WHEN ahr.hold_type='P' THEN acp_hold.record
            WHEN ahr.hold_type='M' THEN mmr.master_record
            ELSE -1
            END
        ), ahr.pickup_lib
        INTO
        bibid, temp_pickup_lib
        FROM
        action.hold_request ahr
        LEFT JOIN biblio.record_entry bre ON(ahr.target=bre.id AND ahr.hold_type='T')
        LEFT JOIN (SELECT acn2.record,ac2.id FROM asset.call_number acn2 JOIN asset.copy ac2 ON ac2.call_number=acn2.id) ac_hold ON(ahr.target=ac_hold.id AND ahr.hold_type='C')
        LEFT JOIN (SELECT acn2.record,acn2.id FROM asset.call_number acn2 JOIN asset.copy ac2 ON ac2.call_number=acn2.id) acn_hold ON(ahr.target=acn_hold.id AND ahr.hold_type='V')
        LEFT JOIN (SELECT acn2.record,acp2.part as id FROM asset.call_number acn2 JOIN asset.copy ac2 ON ac2.call_number=acn2.id JOIN asset.copy_part_map acp2 ON acp2.target_copy=ac2.id) acp_hold ON(ahr.target=acp_hold.id AND ahr.hold_type='P')
        LEFT JOIN metabib.metarecord mmr ON(mmr.id=ahr.target AND ahr.hold_type='M')
        WHERE
        ahr.id=hold_id;

        SELECT
        metarecord
        INTO
        temp_mmr
        FROM
        metabib.metarecord_source_map
        WHERE source = bibid
        LIMIT 1;

        SELECT
        array_agg(id)
        INTO
        temp_pickup_libs
        FROM
        actor.org_unit_descendants(temp_pickup_lib,1);

        -- metarecord holds
        IF temp_mmr != -1 THEN
            temp_add := 0;
            SELECT COUNT(DISTINCT ahr.id)
            INTO
            temp_add
            FROM
            action.hold_request ahr
            JOIN (SELECT source FROM metabib.metarecord_source_map WHERE metarecord = temp_mmr) all_source ON (1=1)
            JOIN metabib.metarecord_source_map all_mmr ON (all_source.source=all_mmr.source AND ahr.hold_type='M' AND all_mmr.metarecord=ahr.target)
            WHERE
            ahr.cancel_time IS NULL AND
            (ahr.expire_time IS NULL OR ahr.expire_time > NOW()) AND
            ahr.fulfillment_time IS NULL AND
            ahr.pickup_lib = ANY (temp_pickup_libs);

            current_request_count = current_request_count + temp_add;
        END IF;

        -- part holds
        temp_add := 0;
        SELECT COUNT(DISTINCT ahr.id)
        INTO
        temp_add
        FROM
        action.hold_request ahr
        JOIN asset.copy_part_map acpm ON (acpm.part=ahr.target AND ahr.hold_type='P')
        JOIN asset.copy ac ON (ac.id=acpm.target_copy)
        JOIN asset.call_number acn ON(acn.id=ac.call_number AND acn.record=bibid)
        WHERE
        ahr.cancel_time is null AND
        (ahr.expire_time IS NULL OR ahr.expire_time > NOW()) AND
        ahr.fulfillment_time IS NULL AND
        ahr.pickup_lib = ANY (temp_pickup_libs);

        current_request_count = current_request_count + temp_add;

        -- volume holds
        temp_add := 0;
        SELECT COUNT(DISTINCT ahr.id)
        INTO
        temp_add
        FROM
        action.hold_request ahr
        JOIN asset.call_number acn ON(acn.id=ahr.target AND ahr.hold_type='V' AND acn.record=bibid)
        WHERE
        ahr.cancel_time is null AND
        (ahr.expire_time IS NULL OR ahr.expire_time > NOW()) AND
        ahr.fulfillment_time IS NULL AND
        ahr.pickup_lib = ANY (temp_pickup_libs);

        current_request_count = current_request_count + temp_add;

        -- title holds
        temp_add := 0;
        SELECT COUNT(DISTINCT ahr.id)
        INTO
        temp_add
        FROM
        action.hold_request ahr
        JOIN biblio.record_entry bre ON(bre.id=ahr.target AND ahr.hold_type='T' AND bre.id=bibid)
        WHERE
        ahr.cancel_time is null AND
        (ahr.expire_time IS NULL OR ahr.expire_time > NOW()) AND
        ahr.fulfillment_time IS NULL AND
        ahr.pickup_lib = ANY (temp_pickup_libs);

        current_request_count = current_request_count + temp_add;

        -- copy holds
        temp_add := 0;
        SELECT COUNT(DISTINCT ahr.id)
        INTO
        temp_add
        FROM
        action.hold_request ahr
        JOIN asset.copy ac ON(ac.id=ahr.target AND ahr.hold_type='C')
        JOIN asset.call_number acn ON(acn.id=ac.call_number AND acn.record=bibid)
        WHERE
        ahr.cancel_time is null AND
        (ahr.expire_time IS NULL OR ahr.expire_time > NOW()) AND
        ahr.fulfillment_time IS NULL AND
        ahr.pickup_lib = ANY (temp_pickup_libs);

        current_request_count = current_request_count + temp_add;

        RETURN NEXT;
      END;

    $BODY$
      LANGUAGE plpgsql VOLATILE;

splitter
        dbhandler_update($query);
        $query = <<'splitter';

    CREATE OR REPLACE FUNCTION libraryiq.get_holds(bigint[])
      RETURNS table ( bibid BIGINT, branch TEXT, current_request_count BIGINT, report_time TEXT) AS
    $BODY$

      DECLARE
        hold_id_array ALIAS FOR $1;
        hid BIGINT;

      BEGIN

        FOREACH hid IN ARRAY hold_id_array
        LOOP
            SELECT
            lahd.bibid,
            lahd.current_request_count
            INTO
            bibid, current_request_count
            FROM
            libraryiq.attempt_hold_detail(hid) as lahd;
            
            SELECT 
            aou.shortname,
            now()::TEXT
              INTO
              branch, report_time
              FROM
              action.hold_request ahr
              JOIN actor.org_unit aou ON (aou.id=ahr.pickup_lib)
              WHERE
              ahr.id=hid;
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
        push( @ret, [ @{$row} ] );
    }
    undef($querystring);
    push( @ret, $query->{NAME} );

    return \@ret;
}

sub dbhandler_update {
    my $querystring = shift;
    my $valRef      = shift;
    my @values      = ();
    @values = @{$valRef} if $valRef;
    my $q = $dbHandler->prepare($querystring);
    my $i = 1;
    foreach (@values) {
        my $param = $_;
        if ( lc( $param eq 'null' ) ) {
            $param = undef;
        }
        $q->bind_param( $i, $param );
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

sub getBibIDs {
    my $pgLibs = shift;
    my $limit  = shift;
    my $offset = shift;
    my @ret    = ();
    my $query  = "
    SELECT bre.id
    FROM
    biblio.record_entry bre
    JOIN asset.call_number acn on(acn.record=bre.id AND NOT acn.deleted)
    WHERE
    acn.owning_lib in( $pgLibs ) AND
    ( acn.edit_date > $lastUpdatePGDate OR bre.edit_date > $lastUpdatePGDate )
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{ getDataChunk( $query, $limit, $offset ) };
    pop @results;

    foreach (@results) {
        my @row = @{$_};
        push( @ret, @row[0] );
    }
    return \@ret;
}

sub getItemIDs {
    my $pgLibs = shift;
    my $limit  = shift;
    my $offset = shift;
    my @ret    = ();
    my $query  = "
    SELECT ac.id
    FROM
    asset.copy ac
    JOIN asset.call_number acn on(acn.id=ac.call_number AND NOT ac.deleted AND NOT acn.deleted)
    WHERE
    acn.owning_lib in( $pgLibs ) AND
    ( acn.edit_date > $lastUpdatePGDate OR ac.edit_date > $lastUpdatePGDate )
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{ getDataChunk( $query, $limit, $offset ) };
    pop @results;

    foreach (@results) {
        my @row = @{$_};
        push( @ret, @row[0] );
    }
    return \@ret;
}

sub getCircIDs {
    my $pgLibs = shift;
    my $limit  = shift;
    my $offset = shift;
    my @ret    = ();

    # libraryIQ only wants the last 3 years on a full load
    my $thisCompareDate = $lastUpdatePGDate;
    $thisCompareDate = "(now() - '3 years'::interval)" if ($full);
    my $query  = "
    SELECT acirc.id
    FROM
    action.all_circulation acirc
    JOIN actor.usr au ON (acirc.usr=au.id)
    JOIN asset.copy ac ON (ac.id=acirc.target_copy)
    JOIN asset.call_number acn on(acn.id=ac.call_number AND NOT ac.deleted AND NOT acn.deleted)
    WHERE
    acn.owning_lib in( $pgLibs ) AND
    acirc.xact_start > $thisCompareDate
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{ getDataChunk( $query, $limit, $offset ) };
    pop @results;

    foreach (@results) {
        my @row = @{$_};
        push( @ret, @row[0] );
    }
    return \@ret;
}

sub getPatronIDs {
    my $pgLibs = shift;
    my $limit  = shift;
    my $offset = shift;
    my @ret    = ();
    my $query  = "
    SELECT au.id
    FROM
    actor.usr au
    WHERE
    NOT au.deleted AND
    au.home_ou in( $pgLibs ) AND
    au.last_update_time > $lastUpdatePGDate
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{ getDataChunk( $query, $limit, $offset ) };
    pop @results;

    foreach (@results) {
        my @row = @{$_};
        push( @ret, @row[0] );
    }
    return \@ret;
}

sub getHoldIDs {
    my $pgLibs = shift;
    my $limit  = shift;
    my $offset = shift;
    my @ret    = ();
    my $query  = "
    SELECT ahr.id
    FROM
    action.hold_request ahr
    WHERE
    ahr.pickup_lib in( $pgLibs ) AND
    ahr.cancel_time IS NULL AND
    (ahr.expire_time IS NULL OR ahr.expire_time > NOW()) AND
    ahr.fulfillment_time IS NULL AND
    ahr.request_time > $lastUpdatePGDate
    GROUP BY 1
    ORDER BY 1";
    log_write($query) if $debug;
    my @results = @{ getDataChunk( $query, $limit, $offset ) };
    pop @results;

    foreach (@results) {
        my @row = @{$_};
        push( @ret, @row[0] );
    }
    return \@ret;
}

sub getSQLFunctionChunk {
    my $idRef   = shift;
    my $type    = shift;
    my @ids     = @{$idRef};
    my $pgArray = join( ',', @ids );
    $pgArray = "'{$pgArray}'::BIGINT[]";
    my $query = "select * from libraryiq.get_$type($pgArray)";
    log_write($query) if $debug;
    return dbhandler_query($query);
}

sub makeLibList {
    my $libsRef = shift;
    my @libs    = $libsRef ? @{$libsRef} : ();
    my $pgLibs  = join( ',', @libs );
    return -1 if $#libs == -1;
    return $pgLibs;
}

sub getDataChunk {
    my $query  = shift;
    my $limit  = shift;
    my $offset = shift;
    $query .= "\nLIMIT $limit OFFSET $offset";
    log_write($query) if $debug;
    return dbhandler_query($query);
}

sub getOrgUnits {
    my $libnames = lc( @_[0] );
    my @ret      = ();

    # spaces don't belong here
    $libnames =~ s/\s//g;

    my @sp = split( /,/, $libnames );

    my $libs = join( '$$,$$', @sp );
    $libs = '$$' . $libs . '$$';

    my $query = "
    select id
    from
    actor.org_unit
    where lower(shortname) in ($libs)
    order by 1";
    log_write($query) if $debug;
    my @results = @{ dbhandler_query($query) };
    pop @results;
    foreach (@results) {
        my @row = @{$_};
        push( @ret, @row[0] );
        if ( $conf{"include_org_descendants"} ) {
            my @des = @{ getOrgDescendants( @row[0] ) };
            push( @ret, @des );
        }
    }
    return dedupeArray( \@ret );
}

sub getOrgDescendants {
    my $thisOrg = shift;
    my $query   = "select id from actor.org_unit_descendants($thisOrg)";
    my @ret     = ();
    log_write($query) if $debug;

    my @results = @{ dbhandler_query($query) };
    pop @results;
    foreach (@results) {
        my @row = @{$_};
        push( @ret, @row[0] );
    }

    return \@ret;
}