package io.netty.handler.ssl.util

import java.io.File
import java.security.{KeyPairGenerator, SecureRandom}


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
  def apply(fqdn: String, bits: Int = 2048)(implicit random: SecureRandom = new SecureRandom): SelfSignedCert = {
    val keyGen = KeyPairGenerator.getInstance("RSA")
    keyGen.initialize(bits, random)
    val keypair = keyGen.generateKeyPair()

    val Array(cert, key) = BouncyCastleSelfSignedCertGenerator.generate(fqdn, keypair, random)

    SelfSignedCert(new File(cert), new File(key))
  }
}
