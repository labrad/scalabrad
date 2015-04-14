package org.labrad.registry

import java.io.{ByteArrayInputStream, File}
import java.nio.ByteOrder.BIG_ENDIAN
import java.sql.{Connection, DriverManager, PreparedStatement}

import anorm._
import anorm.SqlParser._

import org.labrad.data.Data
import org.labrad.types.Type

trait RegistryStore {
  type Dir

  def root: Dir
  def pathTo(dir: Dir): Seq[String]
  def parent(dir: Dir): Dir
  def child(parent: Dir, name: String, create: Boolean): (Dir, Boolean)
  def dir(curDir: Dir): (Seq[String], Seq[String])
  def rmDir(dir: Dir, name: String): Unit

  def getValue(dir: Dir, key: String, default: Option[(Boolean, Data)]): Data
  def setValue(dir: Dir, key: String, value: Data): Unit
  def delete(dir: Dir, key: String): Unit
}

case class SqlDir(id: Long, name: String, parents: List[SqlDir]) {
  def parent: SqlDir = {
    parents match {
      case Nil => this
      case parent :: _ => parent
    }
  }

  def child(id: Long, name: String): SqlDir = {
    SqlDir(id, name, this :: parents)
  }

  lazy val path: Seq[String] = {
    parents.map(_.name).reverse :+ name
  }
}

object SQLiteStore {
  def apply(file: File): SQLiteStore = {
    Class.forName("org.sqlite.JDBC")
    val url = s"jdbc:sqlite:${file.getAbsolutePath}"
    implicit val conn = DriverManager.getConnection(url)

    // set up registry schema if it does not already exist
    SQL"""
      CREATE TABLE IF NOT EXISTS dirs (
        id INTEGER PRIMARY KEY,
        parent_id INTEGER NULL,
        name TEXT NOT NULL,

        UNIQUE (parent_id, name),
        FOREIGN KEY (parent_id) REFERENCES dirs(id) ON DELETE RESTRICT ON UPDATE CASCADE
      )
    """.execute()

    SQL"""
      CREATE INDEX IF NOT EXISTS idx_list_dirs ON dirs(parent_id)
    """.execute()

    SQL"""
      CREATE TABLE IF NOT EXISTS keys (
        id INTEGER PRIMARY KEY,
        dir_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        type TEXT NOT NULL,
        data BLOB NOT NULL,

        UNIQUE (dir_id, name),
        FOREIGN KEY (dir_id) REFERENCES dirs(id) ON DELETE RESTRICT ON UPDATE CASCADE
      )
    """.execute()

    SQL"""
      CREATE INDEX IF NOT EXISTS idx_list_keys ON keys(dir_id)
    """.execute()

    // turn on foreign keys
    SQL"""
      PRAGMA foreign_keys = ON
    """.execute()

    // add root directory
    SQL"""
      INSERT OR IGNORE INTO dirs(id, parent_id, name) VALUES (1, NULL, "")
    """.executeInsert()

    new SQLiteStore(conn)
  }

  // anorm helpers to interpolate byte arrays as Blobs in sql statements
  implicit val byteArrayToSql = new ToSql[Array[Byte]] {
    def fragment(value: Array[Byte]): (String, Int) = ("?", 1)
  }

  implicit val byteArrayToStatement = new ToStatement[Array[Byte]] {
    def set(s: PreparedStatement, index: Int, bytes: Array[Byte]): Unit = {
      s.setBinaryStream(index, new ByteArrayInputStream(bytes), bytes.length)
    }
  }
}

class SQLiteStore(cxn: Connection) extends RegistryStore {

  import SQLiteStore._

  type Dir = SqlDir

  implicit val byteOrder = BIG_ENDIAN
  implicit val connection = cxn

  val root = SqlDir(1, "", Nil)

  def pathTo(dir: SqlDir): Seq[String] = dir.path

  def parent(dir: SqlDir): SqlDir = dir.parent

  def child(dir: SqlDir, name: String, create: Boolean): (SqlDir, Boolean) = {
    val idOpt = SQL"""
      SELECT id FROM dirs WHERE parent_id = ${dir.id} AND name = $name
    """.as(long("id").singleOpt)

    val id = idOpt.getOrElse {
      if (!create) sys.error(s"directory does not exist: $name")

      SQL" INSERT INTO dirs(parent_id, name) VALUES (${dir.id}, $name) ".executeInsert().get
    }
    val created = idOpt.isEmpty

    (dir.child(id, name), created)
  }

  def dir(curDir: SqlDir): (Seq[String], Seq[String]) = {
    val dirs = SQL" SELECT name FROM dirs WHERE parent_id = ${curDir.id} ".as(str("name").*)
    val keys = SQL" SELECT name FROM keys WHERE dir_id = ${curDir.id} ".as(str("name").*)
    (dirs, keys)
  }

  def rmDir(dir: SqlDir, name: String): Unit = {
    SQL" DELETE FROM dirs WHERE parent_id = ${dir.id} AND name = $name ".execute()
  }

  def getValue(dir: SqlDir, key: String, default: Option[(Boolean, Data)]): Data = {
    // SQLite return NULL for empty BLOBs, so we have to treat the data
    // column as nullable, even though we specified it as NOT NULL
    val tdOpt = SQL"""
      SELECT type, data FROM keys WHERE dir_id = ${dir.id} AND name = $key
    """.as((str("type") ~ byteArray("data").?).singleOpt)

    tdOpt match {
      case Some(typ ~ data) =>
        Data.fromBytes(Type(typ), data.getOrElse(Array.empty))

      case None =>
        default match {
          case None => sys.error(s"key does not exist: $key")
          case Some((set, default)) =>
            if (set) setValue(dir, key, default)
            default
        }
    }
  }

  def setValue(dir: SqlDir, key: String, value: Data): Unit = {
    val typ = value.t.toString
    val data = value.toBytes

    val n = SQL" UPDATE keys SET type = $typ, data = $data WHERE dir_id = ${dir.id} AND name = $key ".executeUpdate()

    if (n == 0) {
      SQL" INSERT INTO keys(dir_id, name, type, data) VALUES (${dir.id}, $key, $typ, $data) ".executeInsert()
    }
  }

  def delete(dir: SqlDir, key: String): Unit = {
    // TODO: do we need to look at boolean return value here?
    SQL" DELETE FROM keys WHERE dir_id = ${dir.id} AND name = $key ".execute()
  }
}
