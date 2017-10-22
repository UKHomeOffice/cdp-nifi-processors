#!/bin/bash
#git pull; 
mvn -Dmaven.test.skip=true -DskipTests=true -T 8 clean install ; 
#scp -P 12222 */*/target/*.nar nifi-pontus-elastic-2.x-processor-bundle/nifi-pontus-elastic-2.x-processor/target/nifi-pontus-elastic-2.x-processor-1.0.jar root@localhost:/opt/pontus;
#scp -P 12222 */*/target/*gremlin*.nar */*/target/*service*.nar root@localhost:/opt/pontus;
#cp  */*/target/*gremlin*.nar */*/target/*service*.nar ../nifi-1.2.0.3.0.1.1-5/lib
cp  */*/target/*gremlin*.nar */*/target/*service*.nar ../nifi-1.4.0-bin/lib
