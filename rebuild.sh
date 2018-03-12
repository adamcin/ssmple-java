#!/bin/bash
BASEDIR="$(cd "$(dirname "$0")" && pwd)"

pushd "$BASEDIR"
./gradlew clean assemble && tar -xf build/distributions/ssmple.tar -C build/distributions
