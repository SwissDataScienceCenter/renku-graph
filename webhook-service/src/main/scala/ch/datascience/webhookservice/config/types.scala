package ch.datascience.webhookservice.config

import ch.datascience.tinytypes.TinyType
import com.typesafe.config.Config

case class BufferSize(value: Int) extends TinyType[Int]

object BufferSize {

  implicit object BufferSizeFinder extends (Config => String => BufferSize) {
    override def apply(config: Config): String => BufferSize = key => BufferSize(config.getInt(key))
  }
}

case class TriplesFinderThreads(value: Int) extends TinyType[Int]

object TriplesFinderThreads {

  implicit object TriplesFinderThreadsFinder extends (Config => String => TriplesFinderThreads) {
    override def apply(config: Config): String => TriplesFinderThreads = key => TriplesFinderThreads(config.getInt(key))
  }
}
