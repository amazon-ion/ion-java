== ion-java ==

ion-java is a Java implementation of the Ion data notation.

=== Getting Started ===

==== Cloning the Repository ====

ion-java contains a [git submodule](https://git-scm.com/docs/git-submodule)
called ion-tools, which holds test data used by ion-java's unit tests.

The easiest way to clone the ion-java repository and initialize its ion-tests
submodule is to run the following command.

    <b>$</b> git clone --recursive https://github.com/amznlabs/ion-java
    
Alternatively, the submodule may be initialized independently from the clone
by running the following commands.

    <b>$</b> git submodule init
    <b>$</b> git submodule update

Now the submodule points to the tip of the branch of the ion-tests repository
specified in ion-java's `.gitmodules` file.

ion-java may now be built and installed into the local Maven repository with
the following command.

    <b>$</b> mvn install
    
==== Pulling in Upstream Changes ====

To pull upstream changes into ion-java, start with a simple `git pull`.
This will pull in any changes to ion-java itself (including any changes
to its `.gitmodules` file), but not any changes to the ion-tests
submodule. To make sure the submodule is up-to-date, use the following
command.

    <b>$</b> git submodule update --remote

This will fetch and update the ion-tests submodule from the ion-tests branch
currently specified in the `.gitmodules` file.

For detailed walkthroughs of git submodule usage, see the
[Git Tools documentation](https://git-scm.com/book/en/v2/Git-Tools-Submodules).

==== Using the Library ====

To start using ion-java in your code with Maven, insert the following
dependency into your project's `pom.xml`:

    <dependency>
        <groupId>com.amazon</groupId>
        <artifactId>ion-java</artifactId>
        <version>0.1</version>
    </dependency>
    
=== Usage ===

**TODO**: Public links to Ion documentation.

