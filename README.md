# Evergreen Library IQ Export
This software will extract data from an Evergreen server and SFTP the output to the specified host. It will send an email to a set of email addresses upon completion. If there is an error, it will send an email to a different set of email addresses. The output data will be kept locally inside the specified archive folder.

## Steps

```bash
git clone https://github.com/mcoia/evergreen-libraryiq-export.git
cd evergreen-libraryiq-export
cp library_config.conf.example library_config.conf
vi library_config.conf
# make tweaks for the log file, archive folder, temp folder, SFTP credentials, etc.
./extract_libraryiq.pl --config library_config.conf
```

## Command Line Options

### Alternate Path to the Evergreen Config File (opensrf.xml)

```bash
--xmlconfig /path/to/xml
```

Defaults to: `/openils/conf/opensrf.xml`

### Recreate Database

```bash
--recreatedb
```

This flag will cause the software to drop and recreate the database schema. It uses a schema named "libraryiq". It might be a good idea to use this flag if this script receives an update from git. There might be tweaks to the DB functions that will need to get applied to your system.

### Full Dataset Extraction

```bash
--full
```

Use this flag to cause the script to extract a full dataset for the specified libraries. By default, the script extracts the data from Evergreen that changed since the last time it ran (for this config file).
