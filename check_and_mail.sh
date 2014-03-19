#!/bin/bash

VALIDATOR_DIR=`dirname $0`

ADMIN_EMAIL="admin@datadryad.org"
VIRTUALENV_PATH="/home/dryad/embargo_validator/env"

OUTPUT_DIR=`mktemp -d /tmp/embargo_validator_XXXXX`
OUTPUT_FILE="$OUTPUT_DIR/embargo_validator.log"

# activate the virtualenv
source "${VIRTUALENV_PATH}/bin/activate"

# Remove CSV files before running
if [ -f "$VALIDATOR_DIR"/*.csv ]; then
	rm "$VALIDATOR_DIR"/*.csv
fi

# run the python script
"${VALIDATOR_DIR}/embargo_validator.py" > $OUTPUT_FILE

# mail the results
mail -s "Dryad Embargo Validator results" "$ADMIN_EMAIL" < $OUTPUT_FILE > /dev/null

# Check for leaks and mail them separately
SOLR_LEAKS_FILE="${VALIDATOR_DIR}/embargo_leaks_solr_index.csv"

if [ -f "$SOLR_LEAKS_FILE" ]; then
    mail -s "Embargo issue detected in solr-indexed data" "$ADMIN_EMAIL" < $SOLR_LEAKS_FILE > /dev/null
fi

RSS_LEAKS_FILE="${VALIDATOR_DIR}/embargo_leaks_rss_feed.csv"

if [ -f "$RSS_LEAKS_FILE" ]; then
    mail -s "Embargo issue detected in recently published data" "$ADMIN_EMAIL" < $RSS_LEAKS_FILE > /dev/null
fi
