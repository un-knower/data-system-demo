#!/bin/bash
rootPath=$(cd "$(dirname "$0")"; cd ..; pwd)
jar=${rootPath}"/collect/target/collect-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
java -cp ${jar} com.meitu.collect.Collector