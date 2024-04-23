#!/bin/bash
##please do not leave any /r characters in this file!
#Script runs in database directory
# Build Core
pgdeploy="$(pwd)/pg_deploy.sql"
echo $pgdeploy
echo "--Built: $(date)" > $pgdeploy
cd schema
find -name "*.sql" -print0 | sort -k2 -t/ -n -z | xargs -r0n1 cat >> $pgdeploy
cd ../code
cat */*/*.sql */*/*/*.sql >> $pgdeploy
 