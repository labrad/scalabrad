package org.labrad.manager.auth

import org.labrad._
import org.labrad.annotations._
import org.labrad.data._
import org.labrad.errors._
import org.labrad.manager.{Hub, LocalServer, MultiheadServer}
import org.labrad.registry.RegistryStore
import org.labrad.types._
import org.labrad.util.{AsyncSemaphore, Logging}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class AuthServer(
  val id: Long,
  val name: String,
  hub: Hub,
  auth: AuthStore,
  oauth: Option[OAuthVerifier],
  registry: RegistryStore,
  externalConfig: ServerConfig
)(implicit ec: ExecutionContext) extends LocalServer with Logging {

  val doc = "Provides various methods of authenticating users who wish to log in."
  private val (_settings, bind) = Reflect.makeHandler[AuthServer]
  private val handler = bind(this)

  val settings = _settings

  val srcKey = RequestContext.Key[String]("source")

  // start multi-headed connections to other managers
  private val multihead = new MultiheadServer(name, registry, this, externalConfig)

  def expireContext(src: String, ctx: Context)(implicit timeout: Duration): Future[Long] = {
    Future.successful(0)
  }

  def expireAll(src: String, high: Long)(implicit timeout: Duration): Future[Long] = {
    Future.successful(0)
  }

  def expireAll(src: String)(implicit timeout: Duration): Future[Unit] = {
    Future.successful(())
  }

  def message(src: String, packet: Packet): Unit = {}

  def request(src: String, packet: Packet, messageFunc: (Long, Packet) => Unit)(implicit timeout: Duration): Future[Packet] = {
    Future {
      Server.handle(packet) { req =>
        handler(req.withValue(srcKey, src))
      }
    }
  }

  // remote settings

  @Setting(id = 101,
           name = "auth_methods",
           doc = """Get a list of supported authentication methods.""")
  def authMethods(): Seq[String] = {
    oauth match {
      case None => Seq("username+password")
      case Some(_) => Seq("username+password", "oauth_token", "oauth_access_token")
    }
  }

  @Setting(id = 102,
           name = "auth_info",
           doc = """Get info needed for the give authentication methods.
               |
               |Supported authentication methods can be found by calling the auth_methods setting.
               |Info about the given authentication method will be returns as a cluster of
               |(name, value) pairs. These will include "credential_tag" with a string giving the
               |labrad type tag for credential data required by this auth method, and
               |"credential_doc" with an explanation of what credentials are required, along with
               |any other info needed to authenticate successfully.""")
  def authInfo(method: String): Data = {
    method match {
      case "username+password" =>
        (("credential_tag", "(ss)"),
         ("credential_doc", "(username, password)")).toData

      case "oauth_token" | "oauth_access_token" =>
        val clients = oauth.map(_.clients).getOrElse {
          sys.error("OAuth authentication is not configured")
        }
        val clientTypes = Seq(
          OAuthClientType.Desktop -> "",
          OAuthClientType.Web -> "web_"
        )
        val b = new DataBuilder
        b.clusterStart()
          b.add(("credential_tag", "s"))
          b.add(("credential_doc", "id_token string from OAuth login"))
          for ((clientType, prefix) <- clientTypes) {
            for (info <- clients.get(clientType)) {
              b.add((s"${prefix}client_id", info.clientId))
              b.add((s"${prefix}client_secret", info.clientSecret))
            }
          }
        b.clusterEnd()
        b.result()

      case method =>
        sys.error(s"Unknown authentication method: $method")
    }
  }

  @Setting(id = 103,
           name = "authenticate",
           doc = """Check credentials using the given authentication method.
               |
               |Called with a string indicating which authentication method to use, and data which
               |should be in the form given by the "credential_tag" in the auth_info for this
               |authentication method.""")
  def authenticateUser(method: String, credentials: Data): String = {
    method match {
      case "username+password" =>
        val (username, password) = credentials.get[(String, String)]
        if (!auth.checkUserPassword(username, password)) {
          sys.error("invalid username or password")
        }
        username

      case "oauth_token" | "oauth_access_token" =>
        val verifier = oauth.getOrElse { sys.error("OAuth authentication is not configured") }
        val token = credentials.get[String]
        val username = method match {
          case "oauth_token" => verifier.verifyIdToken(token)
          case "oauth_access_token" => verifier.verifyAccessToken(token)
        }
        if (!auth.checkUser(username)) {
          sys.error(s"Unknown username: $username")
        }
        username

      case method =>
        sys.error(s"Unknown authentication method: $method")
    }
  }

  // username/password management

  @Setting(id = 200,
           name = "users",
           doc = """Get list of users and their admin status.""")
  def usersList(): Seq[(String, Boolean)] = {
    auth.listUsers()
  }

  @Setting(id = 201,
           name = "users_add",
           doc = """Add a user with the given name, admin status, and optional password.
               |
               |If no password is given, only OAuth login will be supported for this user.""")
  def usersAdd(
    ctx: RequestContext,
    username: String,
    isAdmin: Boolean,
    passwordOpt: Option[String]
  ): Unit = {
    requireAdmin(ctx, "add user")
    auth.addUser(username, isAdmin, passwordOpt)
  }

  @Setting(id = 202,
           name = "users_change_password",
           doc = """Change a user password.
               |
               |Called with username, optional old password and optional new password. If no new
               |password is given, the password will be removed, in which case only OAuth login
               |will be supported for this user.
               |
               |Password can only be changed for the current user, unless the current user is an
               |admin.""")
  def usersChangePassword(
    ctx: RequestContext,
    username: String,
    oldPassword: Option[String],
    newPassword: Option[String]
  ): Unit = {
    val info = requestInfo(ctx)
    require(info.isLocalRequest, localErrorMsg("change password"))
    if (!info.isAdmin) {
      require(username == info.username, adminErrorMsg("change password"))
    }
    auth.changePassword(username, oldPassword, newPassword, info.isAdmin)
  }

  @Setting(id = 203,
           name = "users_set_admin",
           doc = """Change admin status of a user.
               |
               |Called with a username and flag indicating whether that user should be an admin.
               |Can only be called by admins.""")
  def usersSetAdmin(ctx: RequestContext, username: String, isAdmin: Boolean): Unit = {
    requireAdmin(ctx, "change admin status")
    auth.setAdmin(username, isAdmin)
  }

  @Setting(id = 204,
           name = "users_remove",
           doc = """Remove the given user.
               |
               |Called with username to remove. Can only be called by admins.""")
  def usersRemove(ctx: RequestContext, username: String): Unit = {
    requireAdmin(ctx, "remove user")
    auth.removeUser(username)
  }

  private case class RequestInfo(username: String, isAdmin: Boolean, isLocalRequest: Boolean)

  /**
   * Get information about the local request.
   *
   * We only set the isAdmin flag if the request is coming from a connection to the local
   * manager, not one of the remote managers, since we can not verify user information for
   * requests made from remote managers.
   */
  private def requestInfo(ctx: RequestContext): RequestInfo = {
    val username = hub.username(ctx.source)
    val isAdminUser = username == "" || auth.isAdmin(username)
    val isLocalRequest = ctx.get(srcKey) match {
      case Some("") => true
      case _ => false
    }
    RequestInfo(
      username,
      isAdmin = isAdminUser && isLocalRequest,
      isLocalRequest = isLocalRequest
    )
  }

  private def localErrorMsg(operation: String): String = {
    s"Must connect to manager where Auth server is running locally to $operation."
  }

  private def adminErrorMsg(operation: String): String = {
    s"Must be admin to $operation."
  }

  private def requireAdmin(ctx: RequestContext, operation: String): Unit = {
    val info = requestInfo(ctx)
    require(info.isLocalRequest, localErrorMsg(operation))
    require(info.isAdmin, adminErrorMsg(operation))
  }


  // remote managers

  @Setting(id = 1000,
           name = "managers",
           doc = """Get a list of managers we are connecting to as an external registry.
               |
               |The returned list is a sequence of clusters of the form (host, port, connected),
               |where host is the string hostname and port the integer port number of the manager,
               |and connected is a flag indicating whether we are currently connected to that
               |manager.""")
  def managersList(): Seq[(String, Int, Boolean)] = {
    multihead.list()
  }

  @Setting(id = 1001,
           name = "managers_refresh",
           doc = """Refresh the list of managers from the registry.""")
  def managersRefresh(): Unit = {
    multihead.refresh()
  }

  @Setting(id = 1002,
           name = "managers_add",
           doc = """Add a new manager to connect to as an external registry.
               |
               |Specified as a hostname, optional port number, and optional password. If port is
               |not given, use the default labrad port. If password is not given, use the same
               |password as configured on the manager where the registry is running locally.""")
  def managersAdd(host: String, port: Option[Int], password: Option[String]): Unit = {
    multihead.add(host, port, password)
  }

  @Setting(id = 1003,
           name = "managers_ping",
           doc = """Send a network ping to all external managers.
               |
               |Called with a string giving an optional regular expression to match against
               |manager names, and an optional port number. If no port is given, matches any port.
               |If no host regex is given, matches any hostname.""")
  def managersPing(hostPat: String = ".*", port: Int = 0): Unit = {
    multihead.ping(hostPat, port)
  }

  @Setting(id = 1004,
           name = "managers_reconnect",
           doc = """Disconnect from matching managers and reconnect.
               |
               |Hostname regular expression and port number are matched against external managers
               |as described for "Managers Ping".""")
  def managersReconnect(hostPat: String, port: Int = 0): Unit = {
    multihead.reconnect(hostPat, port)
  }

  @Setting(id = 1005,
           name = "managers_drop",
           doc = """Disconnect from matching managers and do not reconnect.
               |
               |Hostname regular expression and port number are matched against external managers
               |as described for "Managers Ping".""")
  def managersDrop(hostPat: String, port: Int = 0): Unit = {
    multihead.drop(hostPat, port)
  }
}

