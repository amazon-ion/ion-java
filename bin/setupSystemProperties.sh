#!/bin/sh

# Generate system.properties file for tests to load.
# When running tests from Ant, the build.xml defines these.
# But in eclipse we need to do it specially.

propsFile=$1
ionTestsPath=$2

cat > $propsFile <<END
com.amazon.iontests.iontestdata.path=$2
END
