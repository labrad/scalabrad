package org.labrad.manager.auth

import anorm._
import anorm.SqlParser._
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.http.javanet.NetHttpTransport
import java.io.{ByteArrayInputStream, File}
import java.sql.{Connection, DriverManager}
import org.labrad.data._
import org.labrad.errors._
import org.labrad.registry.RegistryStore
import org.labrad.util.Logging
import org.mindrot.jbcrypt.BCrypt
import scala.collection.JavaConverters._

trait AuthStore {
  def listUsers(): Seq[(String, Boolean)]
  def addUser(username: String, isAdmin: Boolean, passwordOpt: Option[String]): Unit
  def changePassword(
    username: String,
    oldPassword: Option[String],
    newPassword: Option[String],
    isAdmin: Boolean
  ): Unit
  def checkUser(username: String): Boolean
  def checkUserPassword(username: String, password: String): Boolean
  def isAdmin(username: String): Boolean
  def setAdmin(username: String, isAdmin: Boolean): Unit
  def removeUser(username: String): Unit
}

object AuthStore {
  def apply(file: File, globalPassword: Array[Char]): AuthStore = {
    Class.forName("org.sqlite.JDBC")
    val url = s"jdbc:sqlite:${file.getAbsolutePath}"
    implicit val conn = DriverManager.getConnection(url)

    // set up registry schema if it does not already exist
    SQL"""
      CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        is_admin BOOLEAN NOT NULL,
        password_hash TEXT,

        UNIQUE (name)
      )
    """.execute()

    SQL"""
      CREATE INDEX IF NOT EXISTS idx_list_users ON users(name)
    """.execute()

    val globalPasswordHash = BCrypt.hashpw(new String(globalPassword), BCrypt.gensalt())

    new AuthStoreImpl(conn, globalPasswordHash)
  }
}

class AuthStoreImpl(
  cxn: Connection,
  globalPasswordHash: String
) extends AuthStore with Logging {

  private implicit val connection = cxn

  def listUsers(): Seq[(String, Boolean)] = synchronized {
    // get name and is_admin; handle SQLite storing bool as int
    val parser = (get[String]("name") ~ get[Int]("is_admin")) map {
      case name ~ isAdmin => (name, isAdmin != 0)
    }
    SQL"""
      SELECT name, is_admin FROM users
    """.as(parser.*)
  }

  def addUser(
    username: String,
    isAdmin: Boolean,
    passwordOpt: Option[String]
  ): Unit = synchronized {
    require(username.nonEmpty, "Cannot add user with empty username")
    passwordOpt match {
      case None | Some("") =>
        SQL"""
          INSERT INTO users(name, is_admin) VALUES ($username, $isAdmin)
        """.executeInsert()

      case Some(password) =>
        val hashed = BCrypt.hashpw(password, BCrypt.gensalt())
        SQL"""
          INSERT INTO users(name, password_hash, is_admin) VALUES ($username, $hashed, $isAdmin)
        """.executeInsert()
    }
  }

  def changePassword(
    username: String,
    oldPasswordOpt: Option[String],
    newPasswordOpt: Option[String],
    isAdmin: Boolean
  ): Unit = synchronized {
    val oldHashedOpt = SQL"""
      SELECT password_hash FROM users WHERE name = $username
    """.as(get[Option[String]]("password_hash").singleOpt)
       .getOrElse { sys.error(s"username $username does not exist") }

    if (!isAdmin) {
      // Check that provided oldPassword matches stored hash, if there is one.
      for (oldHashed <- oldHashedOpt) {
        val oldPassword = oldPasswordOpt.getOrElse { sys.error(s"incorrect password") }
        if (!BCrypt.checkpw(oldPassword, oldHashed)) sys.error(s"incorrect password")
      }
    }

    // Set or clear the password.
    newPasswordOpt match {
      case None | Some("") =>
        SQL"""
          UPDATE users SET password_hash = NULL WHERE name = $username
        """.executeUpdate()

      case Some(newPassword) =>
        val newHashed = BCrypt.hashpw(newPassword, BCrypt.gensalt())
        SQL"""
          UPDATE users SET password_hash = $newHashed WHERE name = $username
        """.executeUpdate()
    }
  }

  def checkUser(username: String): Boolean = synchronized {
    val n = SQL"""
      SELECT count(*) AS n FROM users WHERE name = $username
    """.as(get[Int]("n").single)
    n > 0
  }

  def checkUserPassword(username: String, password: String): Boolean = synchronized {
    val hashed = if (username.isEmpty) {
      globalPasswordHash
    } else {
      SQL"""
        SELECT password_hash FROM users WHERE name = $username AND password_hash IS NOT NULL
      """.as(get[String]("password_hash").singleOpt)
         .getOrElse { sys.error(s"username $username does not exist or has no password") }
    }

    BCrypt.checkpw(password, hashed)
  }

  def isAdmin(username: String): Boolean = synchronized {
    SQL"""
      SELECT is_admin FROM users WHERE name = $username
    """.as(get[Int]("is_admin").singleOpt) // SQLite stores bools as ints
       .map(_ != 0)
       .getOrElse { sys.error(s"username $username does not exist") }
  }

  def setAdmin(username: String, isAdmin: Boolean): Unit = synchronized {
    val nRows = SQL"""
      UPDATE users SET is_admin = $isAdmin WHERE name = $username
    """.executeUpdate()
    if (nRows == 0) sys.error(s"username $username does not exist")
  }

  def removeUser(username: String): Unit = synchronized {
    SQL" DELETE FROM users WHERE name = $username ".execute()
  }
}

