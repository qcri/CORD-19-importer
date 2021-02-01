#!/bin/sh

if [ $# -le 0 ]; then
    echo "$0 <date_in_YYYY-MM-DD>"
    exit 1
fi

input_date=$1

cord_archive="cord-19_"$input_date".tar.gz"
url="https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/historical_releases/$cord_archive"

echo "Pulling CORD-19 archive from $url"

curl -o cord-19_$input_date.tar.gz $url
tar -xvf cord-19_$input_date.tar.gz

python3 transform_to_rayyan.py $input_date/metadata.csv CORD-19-$input_date-file-
