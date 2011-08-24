package org.labrad.annotations

object Matchers {
  
  object Accepts {
    def unapply(obj: Object): Option[String] = obj match {
      //case a: Accepts => Some(a.value)
      case a: Accept => Some(a.value)
      case _ => None
    }
  }
  
  object Returns {
    def unapply(obj: Object): Option[String] = obj match {
      //case a: Returns => Some(a.value)
      case a: Return => Some(a.value)
      case _ => None
    }
  }
  
  object NamedMessageHandler {
    def unapply(obj: Object): Option[String] = obj match {
      case a: NamedMessageHandler => Some(a.value)
      case _ => None
    }
  }
  
  object ServerInfo {
    def unapply(obj: Object): Option[(String, String)] = obj match {
      case a: ServerInfo => Some((a.name, a.doc))
      case _ => None
    }
  }
}
