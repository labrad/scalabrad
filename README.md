scalabrad
=========

[![Build Status](https://secure.travis-ci.org/labrad/scalabrad.png)](http://travis-ci.org/labrad/scalabrad)

A scala interface to labrad.

Contributing
------------

For instructions on how to contribute to scalabrad, see [contributing.md](https://github.com/labrad/scalabrad/blob/master/contributing.md).

Running Scalabrad
-----------------

Binary distributions of scalabrad are distributed via [bintray](https://bintray.com/labrad/generic/scalabrad).
Simply download and install the archive file for the version you want and unpack it on your machine.
For example, if you download [scalabrad-0.3.1.tar.gz](https://bintray.com/artifact/download/labrad/generic/scalabrad-0.3.1.tar.gz) and untar it in some directory `$PATH/`, then you can run `$PATH/scalabrad-0.3.1/bin/labrad` to start the manager. You'll need to have Java 8 installed on your system, but all other dependencies are included in the package. Windows `.bat` files are also included in the distribution, though you may need a tool like 7-Zip to extract the `.tar.gz` archive.

The scala manager is API-compatible with the old delphi manager, but it stores registry data in a different format because the old registry format was ill-defined and had problems encoding and decoding some labrad data. If you have existing registry data that was saved with the old manager, you can use the included migration tool to migrate the data to the new format. The migration tool is in the bin directory with the manager, e.g. `$PATH/scalabrad-0.3.1/bin/labrad-migrate-registry`. We do not currently provide a way to migrate data back to the old delphi format, so you should migrate when you are ready to switch to the scala manager exclusively.
