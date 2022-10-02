package io.netty.handler.ssl.util

import java.io.File
import java.security.{KeyPairGenerator, SecureRandom}
import java.util.Date


/**
 * A container for a self-signed certificate and its private key.
 *
 * We use netty's code for generating self-signed certificates, but specialized to always
 * use their bouncy castle generator, which uses SHA256 to sign the cert, instead of SHA1
 * as used by their jdk certificate generator.
 *
 * We put this in the io.netty.handler.ssl.util package so that we can access the package
 * protected BouncyCastleSelfSignedCertGenerator class.
 */
case class SelfSignedCert(certificate: File, privateKey: File)


object SelfSignedCert {
  // current time minus 1 year
  val DefaultNotBefore = new Date(System.currentTimeMillis - 86400000L*365)

  // maximum possible time value in the X.509 specification: 9999-12-31 23:59:59
  val DefaultNotAfter = new Date(253402300799000L)

  def apply(fqdn: String, bits: Int = 2048, notBefore: Date = DefaultNotBefore,
            notAfter: Date = DefaultNotAfter)(implicit random: SecureRandom = new SecureRandom): SelfSignedCert = {
    val keyGen = KeyPairGenerator.getInstance("RSA")
    keyGen.initialize(bits, random)
    val keypair = keyGen.generateKeyPair()

    val Array(cert, key) = BouncyCastleSelfSignedCertGenerator.generate(
        fqdn, keypair, random, notBefore, notAfter, "RSA")

    SelfSignedCert(new File(cert), new File(key))
  }
}
