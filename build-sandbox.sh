#!/bin/bash
git pull; mvn -Dmaven.test.skip=true -DskipTests=true clean install; scp -P 2222 */*/target/*.nar nifi-pontus-elastic-2.x-processor-bundle/nifi-pontus-elastic-2.x-processor/target/nifi-pontus-elastic-2.x-processor-1.0.jar localhost:/opt/pontus;
