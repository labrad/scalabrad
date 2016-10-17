package org.labrad.annotations

object Matchers {

  object Accept {
    def unapply(obj: Object): Option[String] = obj match {
      case a: Accept => Some(a.value)
      case _ => None
    }
  }

  object Return {
    def unapply(obj: Object): Option[String] = obj match {
      case a: Return => Some(a.value)
      case _ => None
    }
  }

  object Setting {
    def unapply(obj: Object): Option[(Long, String, String)] = obj match {
      case a: Setting => Some((a.id, a.name, a.doc))
    }
  }
}
