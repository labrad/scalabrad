package org.labrad.manager.auth

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.http.javanet.NetHttpTransport
import org.labrad.errors.LabradException
import org.labrad.util.Logging
import scala.collection.JavaConverters._

sealed trait OAuthClientType
object OAuthClientType {
  case object Desktop extends OAuthClientType
  case object Web extends OAuthClientType
}

case class OAuthClientInfo(clientId: String, clientSecret: String)

class OAuthVerifier(val clients: Map[OAuthClientType, OAuthClientInfo]) extends Logging {
  def verifyToken(idTokenString: String): String = {
    try {
      val transport = new NetHttpTransport()
      val jsonFactory = JacksonFactory.getDefaultInstance()
      val verifier = new GoogleIdTokenVerifier.Builder(transport, jsonFactory)
          .setAudience(clients.values.toSeq.map(_.clientId).asJava)
          .setIssuer("https://accounts.google.com")
          .build()

      val token = GoogleIdToken.parse(jsonFactory, idTokenString)
      if (token == null) throw LabradException(3, "Invalid id token")

      val idToken = verifier.verify(idTokenString)
      if (idToken == null) throw LabradException(3, "Invalid id token")

      idToken.getPayload.getEmail
    } catch {
      case e: Exception =>
        log.error("error validating id token", e)
        throw e
    }
  }
}
