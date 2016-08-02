#!/bin/bash
rootPath=$(cd "$(dirname "$0")"; cd ..; pwd)
collectPath=${rootPath}"/collect"
generatePath=${rootPath}"/generate"
#build collect
echo "==================="
echo "===build collect==="
echo "==================="
cd ${collectPath}
mvn clean
mvn assembly:assembly
#build generate
echo "===================="
echo "===build generate==="
echo "===================="
cd ${generatePath}
mvn clean
mvn assembly:assembly
echo "=================="
echo "===build finish==="
echo "=================="