package org.labrad.util

import java.io.File


/**
 * Helpers for dealing with files and paths.
 *
 * Adds a method '/' to both strings and File objects, so that we can create file references
 * in a convenient notation by writing, for example: "foo" / "bar".
 */
object Paths {
  implicit class PathString(val path: String) extends AnyVal {
    def / (file: String): File = new File(path, file)
  }

  implicit class PathFile(val path: File) extends AnyVal {
    def / (file: String): File = new File(path, file)
  }
}
