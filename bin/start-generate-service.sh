#!/bin/bash
rootPath=$(cd "$(dirname "$0")"; cd ..; pwd)
jar=${rootPath}"/generate/target/generate-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
java -cp ${jar} com.meitu.generate.Main
#java -cp ${jar} com.meitu.generate2.DataFileSimulation