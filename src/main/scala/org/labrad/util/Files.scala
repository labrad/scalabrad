package org.labrad.util

import java.io.File

object Files {

  /**
   * Call the given function with a newly-created temporary file, then delete
   * the file after the function returns.
   */
  def withTempFile[T](body: File => T) = {
    val f = File.createTempFile("sclabrad-tmp-", "")
    try {
      body(f)
    } finally {
      f.delete()
    }
  }

  /**
   * Call the given function with a newly-created temporary directory, then
   * delete the directory and all its contents after the function returns.
   */
  def withTempDir[T](body: File => T) = {
    val f = File.createTempFile("scalabrad-tmp-", "")
    f.delete()
    f.mkdir()
    try {
      body(f)
    } finally {
      def delete(f: File) {
        if (f.isDirectory) {
          for (child <- f.listFiles()) {
            delete(child)
          }
        }
        f.delete()
      }
      delete(f)
    }
  }
}
