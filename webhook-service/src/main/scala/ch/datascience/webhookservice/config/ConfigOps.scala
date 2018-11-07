package ch.datascience.webhookservice.config

import com.typesafe.config.Config

object ConfigOps {

  object Implicits {

    implicit class ConfigOps(config: Config) {

      def get[T](key: String)
                (implicit findValue: Config => String => T): T =
        findValue(config)(key)
    }

    implicit object StringValueFinder extends (Config => String => String) {
      override def apply(config: Config): String => String = config.getString
    }

    implicit object IntValueFinder extends (Config => String => Int) {
      override def apply(config: Config): String => Int = config.getInt
    }
  }
}
