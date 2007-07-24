PACKAGE_NAME = IonJava
BRAZIL_PACKAGE_VERSION?=1.0
PACKAGE_SUBDIRECTORY = shared/platform/IonJava

all:

BRAZIL_HOMEDIR := $(shell /apollo/env/SDETools/bin/findup make-support)

JAVA_HOME := $(shell /apollo/env/SDETools/bin/bootstrap-cache-package-version $(BRAZIL_HOMEDIR) JDK 1.5)/jdk1.5.0
ANT_HOME  := $(shell /apollo/env/SDETools/bin/bootstrap-cache-package-version $(BRAZIL_HOMEDIR) ApacheAnt 1.7)
JUNIT_HOME := $(shell /apollo/env/SDETools/bin/bootstrap-cache-package-version $(BRAZIL_HOMEDIR) junit 1.0)

ANT_LIBPATH := $(JUNIT_HOME)/lib/junit.jar
ANT_CMD   := $(ANT_HOME)/bin/ant -lib $(ANT_LIBPATH)
ANT_FLAGS :=


#
# PackageBuilder passes the full Brazil version to use
# through the BRAZIL_PACKAGE_VERSION environment variable;
# we pass it to the Ant file as a Java property value
#
export BRAZIL_VERSION  := $(if $(BRAZIL_PACKAGE_VERSION), -Damazon.ht2.thisPackage.version.str=$(BRAZIL_PACKAGE_VERSION))


#
# In PackageBuilder, the HappyTrails library should be
# bootstrapped using BrazilTools. Updated for HappyTrails 2.0!
#
export HT2_BOOTSTRAP_JAR := $(shell /apollo/env/SDETools/bin/bootstrap-cache-package-version $(BRAZIL_HOMEDIR) MiniTrails 2.0)/lib/miniTrails-2.0.jar


#
# The HappyTrails "HOME" environment variable should point
# directly to the directory containing the happyTrails-2.0.jar
#
export HAPPY_TRAILS2_HOME := $(HAPPY_TRAILS_BASE)/lib


#
# We use the special Makefile syntax "%" to forward all targets
# to Ant
#
release:
test:
build:
%:
	JAVA_HOME=$(JAVA_HOME) $(ANT_CMD) $(ANT_FLAGS) $(BRAZIL_VERSION) $@
