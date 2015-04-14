package org.labrad.registry

import java.io.{ByteArrayOutputStream, File, FileInputStream, FileOutputStream}
import java.net.{URLDecoder, URLEncoder}
import java.nio.ByteOrder.BIG_ENDIAN
import java.nio.charset.StandardCharsets.UTF_8

import scala.annotation.tailrec

import org.labrad.data._
import org.labrad.types._

object FileStore {
  val DIR_EXT = ".dir"
  val KEY_EXT = ".key"
}

class FileStore(rootDir: File) extends RegistryStore {

  import FileStore._

  implicit val byteOrder = BIG_ENDIAN

  type Dir = File

  val root = rootDir.getAbsoluteFile

  /**
   * Convert the given directory into a registry path.
   */
  def pathTo(dir: File): Seq[String] = {
    @tailrec
    def fun(dir: File, rest: Seq[String]): Seq[String] = {
      if (dir == root)
        "" +: rest
      else
        fun(dir.getParentFile, decode(dir.getName.stripSuffix(DIR_EXT)) +: rest)
    }
    fun(dir, Nil)
  }

  def parent(dir: File): File = {
    if (dir == root) dir else dir.getParentFile
  }

  def dir(dir: File): (Seq[String], Seq[String]) = {
    val files = dir.listFiles()
    val dirs = for (f <- files if f.isDirectory; n = f.getName if n.endsWith(DIR_EXT)) yield decode(n.stripSuffix(DIR_EXT))
    val keys = for (f <- files if f.isFile; n = f.getName if n.endsWith(KEY_EXT)) yield decode(n.stripSuffix(KEY_EXT))
    (dirs, keys)
  }

  def child(parent: File, name: String, create: Boolean): (File, Boolean) = {
    val dir = new File(parent, encode(name) + DIR_EXT)
    val created = if (dir.exists) {
      false
    } else {
      if (!create) sys.error(s"directory does not exist: $name")
      dir.mkdir()
      true
    }
    (dir, created)
  }

  def rmDir(dir: File, name: String): Unit = {
    val (path, created) = child(dir, name, create = false)
    if (!path.exists) sys.error(s"directory does not exist: $name")
    if (path.isFile) sys.error(s"found file instead of directory: $name")
    path.delete()
  }

  def getValue(dir: File, key: String, default: Option[(Boolean, Data)]): Data = {
    val path = keyFile(dir, key)
    if (path.exists) {
      val bytes = readFile(path)
      decodeData(bytes)
    } else {
      default match {
        case None => sys.error(s"key does not exist: $key")
        case Some((set, default)) =>
          if (set) setValue(dir, key, default)
          default
      }
    }
  }

  def setValue(dir: File, key: String, value: Data): Unit = {
    val path = keyFile(dir, key)
    val bytes = encodeData(value)
    writeFile(path, bytes)
  }

  def delete(dir: File, key: String): Unit = {
    val path = keyFile(dir, key)
    if (!path.exists) sys.error(s"key does not exist: $key")
    if (path.isDirectory) sys.error(s"found directory instead of file: $key")
    path.delete()
  }

  private def readFile(file: File): Array[Byte] = {
    val is = new FileInputStream(file)
    try {
      val os = new ByteArrayOutputStream()
      val buf = new Array[Byte](10000)
      var done = false
      while (!done) {
        val read = is.read(buf)
        if (read >= 0) os.write(buf, 0, read)
        done = read < 0
      }
      os.toByteArray
    } finally {
      is.close
    }
  }

  private def writeFile(file: File, contents: Array[Byte]) {
    val os = new FileOutputStream(file)
    try
      os.write(contents)
    finally
      os.close
  }

  private def keyFile(dir: File, key: String) = new File(dir, encode(key) + KEY_EXT)

  /**
   * Encode arbitrary string in a format suitable for use as a filename.
   *
   * We use URLEncoder to encode special characters, which handles all the
   * special characters prohibited by most OSs. We must also manually
   * replace * by %2A as this is not replaced by the URLEncoder, but
   * it is properly decoded by the URLDecoder, so no special handling
   * is needed there.
   */
  private def encode(segment: String): String = {
    URLEncoder.encode(segment, UTF_8.name).replace("*", "%2A")
  }

  private def decode(segment: String): String = {
    URLDecoder.decode(segment, UTF_8.name)
  }

  /**
   * Encode and decode data for storage in individual key files.
   */
  private def encodeData(data: Data): Array[Byte] = {
    Cluster(Str(data.t.toString), Bytes(data.toBytes)).toBytes
  }

  private def decodeData(bytes: Array[Byte]): Data = {
    val (typ, data) = Data.fromBytes(Type("ss"), bytes).get[(String, Array[Byte])]
    Data.fromBytes(Type(typ), data)
  }
}
