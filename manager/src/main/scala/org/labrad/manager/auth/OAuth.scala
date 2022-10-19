package org.labrad.manager.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.google.api.client.json.{GenericJson, JsonObjectParser}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.http.{GenericUrl, HttpHeaders, HttpRequest, HttpRequestInitializer}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.util.Key
import org.labrad.errors.LabradException
import org.labrad.util.Logging
import scala.annotation.meta.field
import scala.jdk.CollectionConverters._

sealed trait OAuthClientType
object OAuthClientType {
  case object Desktop extends OAuthClientType
  case object Web extends OAuthClientType
}

case class OAuthClientInfo(clientId: String, clientSecret: String)

/**
 * To authorize access to labrad, users must provide an access token which we
 * use to call an oauth endpoint to get user info, in particular the user's
 * email address. Documentation about the userinfo endpoint can be found in the
 * OpenID Connect standard:
 * http://openid.net/specs/openid-connect-core-1_0.html#UserInfo.
 */
object UserInfo {
  val URL = new GenericUrl("https://www.googleapis.com/oauth2/v1/userinfo")
}

/**
 * Class to hold user information as returned by the userinfo endpoint.
 * We only care about the email and verified_email fields of the response; the
 * corresponding fields here are annotated with @Key which is used by the
 * google http client json infrastructure when parsing the response to know
 * which fields to extract.
 */
class UserInfo extends GenericJson {
  @(Key @field) var email: String = ""
  @(Key @field)("verified_email") var verifiedEmail: Boolean = false
}

class OAuthVerifier(val clients: Map[OAuthClientType, OAuthClientInfo]) extends Logging {

  private val transport = new NetHttpTransport()
  private val jsonFactory = JacksonFactory.getDefaultInstance()
  private val requestFactory = transport.createRequestFactory(
    new HttpRequestInitializer() {
      override def initialize(request: HttpRequest): Unit = {
        request.setParser(new JsonObjectParser(jsonFactory))
      }
    }
  )

  def verifyIdToken(idTokenString: String): String = {
    val verifier = new GoogleIdTokenVerifier.Builder(transport, jsonFactory)
        .setAudience(clients.values.toSeq.map(_.clientId).asJava)
        .setIssuer("https://accounts.google.com")
        .build()

    val token = GoogleIdToken.parse(jsonFactory, idTokenString)
    if (token == null) throw LabradException(3, "Invalid id token")

    val idToken = verifier.verify(idTokenString)
    if (idToken == null) throw LabradException(3, "Invalid id token")

    val payload = idToken.getPayload
    if (!payload.getEmailVerified) throw LabradException(3, "Email not verified")
    payload.getEmail
  }

  def verifyAccessToken(accessTokenString: String): String = {
    val request = requestFactory.buildGetRequest(UserInfo.URL)
    request.setHeaders(new HttpHeaders().setAuthorization(s"Bearer $accessTokenString"))

    val response = request.execute()
    require((200 to 299).contains(response.getStatusCode),
      s"Failed to get userinfo: ${response.getStatusCode} ${response.getStatusMessage}")

    val userInfo = response.parseAs(classOf[UserInfo])
    if (!userInfo.verifiedEmail) throw LabradException(3, "Email not verified")
    userInfo.email
  }
}
