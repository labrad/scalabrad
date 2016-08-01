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
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.duration._

class AuthServer(
  val id: Long,
  val name: String,
  hub: Hub,
  auth: AuthStore,
  registry: RegistryStore,
  externalConfig: ServerConfig
) extends LocalServer with Logging {

  // enforce doing one thing at a time using an async semaphore
  private val semaphore = new AsyncSemaphore(1)

  private val contexts = mutable.Map.empty[(String, Context), (AuthContext, RequestContext => Data)]

  val doc = "Provides various methods of authenticating users who wish to log in."
  private val (_settings, bind) = Reflect.makeHandler[AuthContext]

  val settings = _settings

  // start multi-headed connections to other managers
  private val multihead = new MultiheadServer(name, registry, this, externalConfig)

  def expireContext(src: String, ctx: Context)(implicit timeout: Duration): Future[Long] = semaphore.map {
    expire(if (contexts.contains((src, ctx))) Seq((src, ctx)) else Seq())
  }

  def expireAll(src: String, high: Long)(implicit timeout: Duration): Future[Long] = semaphore.map {
    expire(contexts.keys.filter { case (s, ctx) => s == src && ctx.high == high }.toSeq)
  }

  def expireAll(src: String)(implicit timeout: Duration): Future[Unit] = semaphore.map {
    expire(contexts.keys.filter { case (s, _) => s == src }.toSeq)
  }

  private def expire(ctxs: Seq[(String, Context)]): Long = ctxs match {
    case Seq() => 0L
    case _ =>
      log.debug(s"expiring contexts: ${ctxs.mkString(",")}")
      contexts --= ctxs
      1L
  }

  def message(src: String, packet: Packet): Unit = {}

  def request(src: String, packet: Packet, messageFunc: (Long, Packet) => Unit)(implicit timeout: Duration): Future[Packet] = semaphore.map {
    // TODO: handle timeout
    val response = Server.handle(packet, includeStackTrace = false) { req =>
      val (_, handler) = contexts.getOrElseUpdate((src, req.context), {
        val regCtx = new AuthContext(src, req.context, messageFunc)
        val handler = bind(regCtx)
        (regCtx, handler)
      })
      handler(req)
    }
    response
  }

  // contains context-specific state and settings
  class AuthContext(src: String, context: Context, messageFunc: (Long, Packet) => Unit) {

    @Setting(id=101,
             name="auth_methods",
             doc="Get a list of supported authentication methods")
    def authMethods(): Seq[String] = {
      auth.oauthClientInfo match {
        case None => Seq("username+password")
        case Some(_) => Seq("username+password", "oauth_token")
      }
    }

    @Setting(id=102,
             name="auth_info",
             doc="Get info needed for various auth methods")
    def authInfo(method: String): Data = {
      method match {
        case "username+password" =>
          (("credential_tag", "(ss)")).toData

        case "oauth_token" =>
          val clientInfo = auth.oauthClientInfo.getOrElse {
            sys.error("OAuth authentication is not configured")
          }
          (("credential_tag", "s"),
           ("client_id", clientInfo.clientId),
           ("client_secret", clientInfo.clientSecret)).toData

        case method =>
          sys.error(s"Unkown authentication method: $method")
      }
    }

    @Setting(id=103,
             name="authenticate",
             doc="Check password for the given user")
    def authenticateUser(method: String, credentials: Data): String = {
      method match {
        case "username+password" =>
          val (username, password) = credentials.get[(String, String)]
          if (!auth.checkUserPassword(username, password)) {
            sys.error("invalid username or password")
          }
          username

        case "oauth_token" =>
          val idTokenString = credentials.get[String]
          auth.checkUserOAuth(idTokenString).getOrElse {
            sys.error("invalid oauth token")
          }

        case method =>
          sys.error(s"Unknown authentication method: $method")
      }
    }

    // username/password management

    private def requireAdmin(ctx: RequestContext, operation: String): Unit = {
      require(src == "", s"Must connect to manager where Auth server is running locally to $operation.")
      val requester = hub.username(ctx.source)
      require(requester == "" || auth.isAdmin(requester), s"Must be admin to $operation.")
    }

    @Setting(id=200, name="users", doc="Get list of users and their admin status.")
    def usersList(): Seq[(String, Boolean)] = {
      auth.listUsers()
    }

    @Setting(id=201, name="users_add",
        doc="""Add a user with the given name, admin status, and optional password.
            |
            |If no password is given, then only OAuth login will be supported for this user.""")
    def usersAdd(ctx: RequestContext, username: String, isAdmin: Boolean, passwordOpt: Option[String]): Unit = {
      requireAdmin(ctx, "add user")
      auth.addUser(username, isAdmin, passwordOpt)
    }

    @Setting(id=202, name="users_change_password", doc="Change a user password")
    def usersChangePassword(ctx: RequestContext, username: String, oldPassword: Option[String], newPassword: Option[String]): Unit = {
      val requester = hub.username(ctx.source)
      if (requester != username) {
        requireAdmin(ctx, "change password")
      }
      auth.changePassword(username, oldPassword, newPassword)
    }

    @Setting(id=203, name="users_set_admin", doc="Change admin status of a user")
    def usersSetAdmin(ctx: RequestContext, username: String, isAdmin: Boolean): Unit = {
      requireAdmin(ctx, "change admin status")
      auth.setAdmin(username, isAdmin)
    }

    @Setting(id=204, name="users_remove", doc="Remove the given user")
    def usersRemove(ctx: RequestContext, username: String): Unit = {
      requireAdmin(ctx, "remove user")
      auth.removeUser(username)
    }


    // remote managers

    @Setting(id=1000,
             name="managers",
             doc="Get a list of managers we are connecting to as an external registry")
    def managersList(): Seq[(String, Int, Boolean)] = {
      multihead.list()
    }

    @Setting(id=1001,
             name="managers_refresh",
             doc="Refresh the list of managers from the registry.")
    def managersRefresh(): Unit = {
      multihead.refresh()
    }

    @Setting(id=1002,
             name="managers_add",
             doc="Add a new manager to connect to as an external registry")
    def managersAdd(host: String, port: Option[Int], password: Option[String]): Unit = {
      multihead.add(host, port, password)
    }

    @Setting(id=1003,
             name="managers_ping",
             doc="Send a network ping to all external managers")
    def managersPing(hostPat: String = ".*", port: Int = 0): Unit = {
      multihead.ping(hostPat, port)
    }

    @Setting(id=1004,
             name="managers_reconnect",
             doc="Disconnect from matching managers and reconnect")
    def managersReconnect(hostPat: String, port: Int = 0): Unit = {
      multihead.reconnect(hostPat, port)
    }

    @Setting(id=1005,
             name="managers_drop",
             doc="Disconnect from matching managers and do not reconnect")
    def managersDrop(hostPat: String, port: Int = 0): Unit = {
      multihead.drop(hostPat, port)
    }
  }
}

