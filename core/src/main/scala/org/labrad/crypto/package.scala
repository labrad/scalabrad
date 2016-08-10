package org.labrad.crypto

import java.io.File
import org.labrad.util.Paths._

sealed trait CertConfig

object CertConfig {
  object Defaults {
    val tlsCertPath = sys.props("user.home") / ".labrad" / "manager" / "certs"
    val tlsKeyPath = sys.props("user.home") / ".labrad" / "manager" / "keys"
  }

  case object SelfSigned extends CertConfig
  case class Files(
    cert: File,
    key: File,
    intermediates: Option[File] = None
  ) extends CertConfig
}


