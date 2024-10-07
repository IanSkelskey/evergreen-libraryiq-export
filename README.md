# Evergreen LibraryIQ Export

This software extracts data from an Evergreen server and SFTPs the output to the specified host. Upon completion, it sends an email to a set of specified email addresses. In the event of an error, it will notify a different set of addresses. The output data is stored locally in the specified archive folder.

## Steps:

~~~bash
git clone https://github.com/mcoia/evergreen-libraryiq-export.git
cd evergreen-libraryiq-export
cp library_config.conf.example library_config.conf
vi library_config.conf
# Make necessary tweaks for the log file, archive folder, temp folder, SFTP credentials, etc.
./extract_libraryiq.pl --config library_config.conf
~~~

## Command Line Options

### 1. Alternate Path to the Evergreen Config File (`opensrf.xml`)

~~~bash
--xmlconfig /path/to/xml
~~~

*Default: `/openils/conf/opensrf.xml`*

### 2. Recreate Database (`--recreatedb`)

~~~bash
--recreatedb
~~~

This flag drops and recreates the database schema, which uses a schema named `libraryiq`. You might need this flag if the script is updated from git, as there may be tweaks to the DB functions that need to be applied.

### 3. Full Dataset Extraction (`--full`)

~~~bash
--full
~~~

This flag forces the script to extract a full dataset for the specified libraries. By default, the script only extracts data that has changed since the last time it ran for the current configuration file.
