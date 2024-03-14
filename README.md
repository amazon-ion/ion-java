# Amazon Ion Java
A Java implementation of the [Ion data notation](https://amazon-ion.github.io/ion-docs).

[![Build Status](https://travis-ci.org/amazon-ion/ion-java.svg?branch=master)](https://travis-ci.org/amazon-ion/ion-java)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.amazon.ion/ion-java/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.amazon.ion/ion-java)
[![Javadoc](https://javadoc-badge.appspot.com/com.amazon.ion/ion-java.svg?label=javadoc)](http://www.javadoc.io/doc/com.amazon.ion/ion-java)

## Setup
This repository contains a [git submodule](https://git-scm.com/docs/git-submodule)
called `ion-tests`, which holds test data used by `ion-java`'s unit tests.

The easiest way to clone the `ion-java` repository and initialize its `ion-tests`
submodule is to run the following command.

```
$ git clone --recursive https://github.com/amazon-ion/ion-java.git ion-java
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
$ ./gradlew publishToMavenLocal
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

To start using `ion-java` in your code, refer to [`ion-java` on Maven Central](https://central.sonatype.com/artifact/com.amazon.ion/ion-java)
to find snippets for adding a dependency on the latest version of the library using your favorite build tool. 

#### Legacy group id

Originally ion-java was published using the group id `software.amazon.ion`. Since 1.4.0 the
official groupId was changed to `com.amazon.ion` to be consistent with other Amazon open
source libraries. We still maintain the legacy group id but strongly encourage users to migrate
to the official one.

## Using the Library
A great way to get started is to use the [Ion cookbook](https://amazon-ion.github.io/ion-docs/guides/cookbook.html).
The [API documentation](http://www.javadoc.io/doc/com.amazon.ion/ion-java) will give a lot
of detailed information about how to use the library.

### Alternatives

If you are looking for an in-memory Ion data model, this library provides `IonValue`, but you should consider 
using `IonElement` from [`ion-element-kotlin`](https://github.com/amazon-ion/ion-element-kotlin) instead.

`IonElement` is a better choice than `IonValue` as long as you can work within its limitations. `IonElement` has
significantly less memory overhead than `IonValue`. It is immutable and does not have references to parent values, so
it is always threadsafe, and unlike `IonValue` there is no need to make deep copies of `IonElement`.
The limitations of `IonElement` are that it does not support symbols with unknown text, it will bring a dependency
on the Kotlin Stdlib, and you may need to change some logic in your application if it relies on being able to access the
parent container of an Ion value.

The Ion maintainers recommend using `IonElement` instead of `IonValue` whenever possible. For more information, see 
"[Why is IonElement needed?](https://github.com/amazon-ion/ion-element-kotlin#user-content-why-is-ionelement-needed)"
