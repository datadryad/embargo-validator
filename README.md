embargo_validator
=================

Python script to check that embargoes are enforced on Dryad.

## Overview

This repository contains a python script that queries public Dryad interfaces to determine if any data files that should be embargoed are available for download.  The script uses two interfaces: The Dryad SOLR search index, and the "Recently Published Data" RSS/Atom feed.

The script examines metadata for Dryad Data Files.  It fetches all metadata values of `dc.date.embargoedUntil` and looks for download links.  If a data file has an embarogedUntil date in the future and a download link, the script attempts to download the file and records the result.

The script produces CSV files containing reports of the checks.  For every Data File examined, the report contains:

+-------------------------+---------------------------------------------+
| file                    | The Data File's DOI                         |
+-------------------------+---------------------------------------------+
| embargo\_dates          | Any embargoedUntil dates found for the file |
+-------------------------+---------------------------------------------+
| embargo\_active         | True if any dates are in the future         |
+-------------------------+---------------------------------------------+
| has\_bitstream\_links   | True if any files have download links       |
+-------------------------+---------------------------------------------+
| download\_results       | The results of download attempts            |
+-------------------------+---------------------------------------------+

## Assumptions

This script makes the following assumptions

1. Data in solr and the RSS feeds are up-to-date and correct.  This script only checks for embargo issues on files that it reads from these data sources.  The solr query includes a filter to only include embargoed items.  If other items are embargoed and not indexed in solr, they will not be considered (unless they happen to appear in the RSS feed)
2. Metadata is accurate.  This script trusts that the date in `dc.date.embargoedUntil` reflects the truth and the wishes of the submitter/journal.  If this metadata is changed in error, the script will not detect this.
3. Download links cannot be synthesized without METS metadata.  Under normal operation, Dryad does not expose file names or download links for embargoed items through the public interface shown here.  If a file is actually downloadable but not linked, this script will not know how to find it.


