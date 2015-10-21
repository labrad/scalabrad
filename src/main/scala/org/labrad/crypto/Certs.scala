package org.labrad.crypto

import java.io.{File, StringReader}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.security.MessageDigest
import org.bouncycastle.cert.X509CertificateHolder
import org.bouncycastle.jcajce.provider.digest.{SHA1, SHA256}
import org.bouncycastle.util.io.pem.PemReader

object Certs {

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
}
