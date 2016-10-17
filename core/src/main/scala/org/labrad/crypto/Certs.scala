package org.labrad.crypto

import io.netty.handler.ssl.{SslContext, SslContextBuilder}
import io.netty.handler.ssl.util.SelfSignedCert
import java.io.{File, StringReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.security.{MessageDigest, SecureRandom}
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.jcajce.provider.digest.{SHA1, SHA256}
import org.bouncycastle.util.io.pem.PemReader
import org.labrad.util.Logging
import org.labrad.util.Paths._
import org.labrad.util.Util

object Certs extends Logging {

  /**
   * Get the SHA1 fingerprint of a PEM-formatted certificate file.
   */
  def fingerprintSHA1(cert: File): String = fingerprintSHA1(read(cert))

  /**
   * Get the SHA1 fingerprint of a PEM-formatted certificate string.
   */
  def fingerprintSHA1(cert: String): String = fingerprint(cert, new SHA1.Digest())

  /**
   * Get the SHA256 fingerprint of a PEM-formatted certificate file.
   */
  def fingerprintSHA256(cert: File): String = fingerprintSHA256(read(cert))

  /**
   * Get the SHA256 fingerprint of a PEM-formatted certificate string.
   */
  def fingerprintSHA256(cert: String): String = fingerprint(cert, new SHA256.Digest())

  /**
   * Get the fingerprint of a PEM-formatted certificate using the given digest function.
   */
  def fingerprint(cert: String, m: MessageDigest): String = {
    val reader = new PemReader(new StringReader(cert))
    val pemObject = try { reader.readPemObject() } finally { reader.close() }
    val x509 = new X509CertificateHolder(pemObject.getContent)

    m.update(x509.getEncoded)
    val digest = m.digest()

    return digest.map(b => "%02X".format(b)).mkString(":")
  }

  private def read(file: File): String = {
    new String(Files.readAllBytes(file.toPath), UTF_8)
  }

  /**
   * Create an SSL/TLS context for the given host, using self-signed certs.
   * A new certificate will be generated and saved if one does not yet exist.
   */
  def sslContextForHost(
    host: String,
    certPath: File = CertConfig.Defaults.tlsCertPath,
    keyPath: File = CertConfig.Defaults.tlsKeyPath
  )(implicit secureRandom: SecureRandom): (File, SslContext) = {
    val certFile = certPath / s"${host}.cert"
    val keyFile = keyPath / s"${host}.key"

    if (!certFile.exists() || !keyFile.exists()) {
      // if one exists but not the other, we have a problem
      require(!certFile.exists(),
        s"found cert file $certFile but no matching key file $keyFile")
      require(!keyFile.exists(),
        s"found key file $keyFile but no matching cert file $certFile")

      log.info(s"Generating self-signed certificate for host '$host'. certFile=$certFile, keyFile=$keyFile")
      val ssc = SelfSignedCert(host, bits = 2048)(secureRandom)
      Util.copy(ssc.certificate, certFile)
      Util.copy(ssc.privateKey, keyFile)
    } else {
      log.info(s"Using saved certificate for host '$host'. certFile=$certFile, keyFile=$keyFile")
    }

    (certFile, SslContextBuilder.forServer(certFile, keyFile).build())
  }
}
