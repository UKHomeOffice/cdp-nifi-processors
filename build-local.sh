#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
OUT_DIR=$DIR/../pontus-dist/opt/pontus/pontus-nifi/current/lib
if [[ ! -d $OUT_DIR ]]; then
  printf "Failed to run; please ensure that pontus-nifi is built first";
  exit 0;
fi

CURDIR=`pwd`
cd $DIR

git pull; 
mvn -Dmaven.test.skip=true -DskipTests=true  clean install ; 
#scp -P 12222 */*/target/*.nar nifi-pontus-elastic-2.x-processor-bundle/nifi-pontus-elastic-2.x-processor/target/nifi-pontus-elastic-2.x-processor-1.0.jar root@localhost:/opt/pontus;
#scp -P 12222 */*/target/*gremlin*.nar */*/target/*service*.nar root@localhost:/opt/pontus;
#cp  */*/target/*gremlin*.nar */*/target/*service*.nar ../nifi-1.2.0.3.0.1.1-5/lib
cp */*/target/*office*.nar  */*/target/*gremlin*.nar */*/target/*service*.nar $OUT_DIR

cd $CURDIR


