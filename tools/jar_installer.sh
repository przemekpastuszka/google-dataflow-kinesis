#!/usr/bin/env bash

group=org.apache.beam
artifact=$1
version=${2-0.1.0-incubating-SNAPSHOT}
build=${3-66}

ARTIFACT_FULL_NAME="${artifact}-${version}"
BASE_PATH="https://builds.apache.org/job/beam_MavenVerify/${build}/${group}\$${artifact}/artifact/${group}/${artifact}/${version}/${ARTIFACT_FULL_NAME}"

mkdir -p tmp

for ext in 'jar' 'pom'; do
    if [ ! -f "tmp/${ARTIFACT_FULL_NAME}.${ext}" ]; then
        wget "${BASE_PATH}.${ext}" -P tmp
    fi
done

if [ -f "tmp/${ARTIFACT_FULL_NAME}.jar" ]; then
    mvn install:install-file -Dfile=tmp/${ARTIFACT_FULL_NAME}.jar -DpomFile=tmp/${ARTIFACT_FULL_NAME}.pom
else
    mvn install:install-file -Dfile=tmp/${ARTIFACT_FULL_NAME}.pom -DpomFile=tmp/${ARTIFACT_FULL_NAME}.pom
fi