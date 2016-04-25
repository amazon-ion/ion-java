# Amazon Ion Java
A Java implementation of the [Ion data notation](http://amznlabs.github.io/ion-docs/index.html).

[![Build Status](https://travis-ci.org/amznlabs/ion-java.svg?branch=master)](https://travis-ci.org/amznlabs/ion-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/software.amazon.ion/ion-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/software.amazon.ion/ion-java)
[![Javadoc](https://javadoc-emblem.rhcloud.com/doc/software.amazon.ion/ion-java/badge.svg)](http://www.javadoc.io/doc/software.amazon.ion/ion-java)

## Setup
This repository contains a [git submodule](https://git-scm.com/docs/git-submodule)
called `ion-tests`, which holds test data used by `ion-java`'s unit tests.

The easiest way to clone the `ion-java` repository and initialize its `ion-tests`
submodule is to run the following command.

```
$ git clone --recursive https://github.com/amznlabs/ion-java.git ion-java
```

Alternatively, the submodule may be initialized independently from the clone
by running the following commands.

```
$ git submodule init
$ git submodule update
```

The submodule points to the tip of the branch of the `ion-tests` repository
specified in `ion-java`'s `.gitmodules` file.

`ion-java` may now be built and installed into the local Maven repository with
the following command.

```
$ mvn install
```
    
### Pulling in Upstream Changes
To pull upstream changes into `ion-java`, start with a simple `git pull`.
This will pull in any changes to `ion-java` itself (including any changes
to its `.gitmodules` file), but not any changes to the `ion-tests`
submodule. To make sure the submodule is up-to-date, use the following
command.

```
$ git submodule update --remote
```

This will fetch and update the ion-tests submodule from the `ion-tests` branch
currently specified in the `.gitmodules` file.

For detailed walkthroughs of git submodule usage, see the
[Git Tools documentation](https://git-scm.com/book/en/v2/Git-Tools-Submodules).

### Depending on the Library

To start using `ion-java` in your code with Maven, insert the following
dependency into your project's `pom.xml`:

```
<dependency>
  <groupId>software.amazon.ion</groupId>
  <artifactId>ion-java</artifactId>
  <version>1.0.0</version>
</dependency>
```    

## Using the Library
A great way to get started is to use the [Ion cookbook](http://amznlabs.github.io/ion-docs/cookbook.html).
The [API documentation](http://www.javadoc.io/doc/software.amazon.ion/ion-java) will give a lot
of detailed information about how to use the library.
