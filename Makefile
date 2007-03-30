PACKAGE_NAME = IonJava
BRAZIL_PACKAGE_VERSION?=1.0
PACKAGE_SUBDIRECTORY = shared/platform/IonJava

all:

# Determine global tools location
APOLLO_TOOLS_BASE=/apollo/env/SDETools
OPT_AMAZON_BASE=/opt/amazon/platform-services/etc/BrazilTools-2.1
GLOBAL_TOOLS_BASE=$(if $(wildcard $(APOLLO_TOOLS_BASE)/bin/findup),$(APOLLO_TOOLS_BASE),$(OPT_AMAZON_BASE))
GLOBAL_TOOLS_BIN:=$(if $(wildcard $(GLOBAL_TOOLS_BASE)/bin/findup),$(GLOBAL_TOOLS_BASE)/bin,$(GLOBAL_TOOLS_BASE)/bin/DEV.STD.PTHREAD)

BRAZIL_HOME := $(shell $(GLOBAL_TOOLS_BIN)/findup make-support)
include $(BRAZIL_HOME)/make-support/bootstrap.mk

