# scalabrad

[![Build Status](https://secure.travis-ci.org/labrad/scalabrad.png)](http://travis-ci.org/labrad/scalabrad)

A scala interface to labrad.

## Running Scalabrad

Binary distributions of scalabrad are distributed via [bintray](https://bintray.com/labrad/generic/scalabrad#files).
Simply download and install the archive file for the version you want and unpack it on your machine.
For example, if you download [scalabrad-0.5.0.tar.gz](https://bintray.com/artifact/download/labrad/generic/scalabrad-0.5.0.tar.gz) and untar it in some directory `$PATH/`, then you can run `$PATH/scalabrad-0.5.0/bin/labrad` to start the manager.
You'll need to have Java 8 installed on your system, but all other dependencies are included in the package.
Windows `.bat` files are also included in the distribution, though you may need a tool like 7-Zip to extract the `.tar.gz` archive.

Invoke the manager with the `--help` option to see documentation of the command line parameters and environment variables that can be used to configure the manager.
Note that environment variable names used for configuration must be uppercase, as specified in the help text, even if the underlying OS treats environment variable names as case insesitive (such as Windows).

The scala manager is API-compatible with the old delphi manager.
It can store registry data in a different formats to make the registry more efficient, and because the old registry format was ill-defined and had problems encoding and decoding some labrad data.
If you have existing registry data that was saved with the old manager, you can configure the manager to continue to read and write in that format (see the command line help for the manager on how to do this), or you can use the included migration tool to migrate the data to the new format.
The migration tool is in the bin directory with the manager, e.g. `$PATH/scalabrad-0.5.0/bin/labrad-migrate-registry`.

The manager supports using TLS to secure connections to labard for v0.5.0 and above.
If you need to allow old clients to continue to connect without TLS authentication, the manager must be called with the flag `--tls-required=false`.


## Contributing

For instructions on how to contribute to scalabrad, see [contributing.md](https://github.com/labrad/labrad/blob/master/contributing.md).


## Code Style

Code should follow the [scala style guide](http://docs.scala-lang.org/style/).

Code should be documented with [scaladoc](http://docs.scala-lang.org/style/scaladoc.html). Note that we prefer the indentation style common in java where asterisks in multiline doc comments are aligned to the left, rather than to the right:

```scala
/**
  * NO!!
  */

/**
 * YES!!
 */
```

The Google java style guide has some good [advice](http://google.github.io/styleguide/javaguide.html#s7-javadoc) about where comments should be included: 

    At the minimum, Javadoc is present for every public class, and every public or protected member
    of such a class.  Other classes and members still have Javadoc as needed. Whenever an
    implementation comment would be used to define the overall purpose or behavior of a class, method
    or field, that comment is written as Javadoc instead. (It's more uniform, and more tool-friendly.)

    Javadoc is optional for "simple, obvious" methods like getFoo, in cases where there really and
    truly is nothing else worthwhile to say but "Returns the foo".

In scala a method like `getFoo` would probably just be called `foo`, or preferably you would just have public immutable member `foo`, but the principle is the same: completely obvious comments distract more than they inform.
