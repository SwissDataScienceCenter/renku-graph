package ch.datascience.webhookservice.config

import ch.datascience.tinytypes.TinyType
import com.typesafe.config.Config

case class BufferSize(value: Int) extends TinyType[Int]

object BufferSize {

  implicit object BufferSizeFinder extends (Config => String => BufferSize) {
    override def apply(config: Config): String => BufferSize = key => BufferSize(config.getInt(key))
  }
}
