package ch.datascience.webhookservice.queue

import ch.datascience.tinytypes.TinyType
import com.typesafe.config.Config

case class BufferSize(value: Int) extends TinyType[Int] {
  verify(value > 0, s"'$value' is not > 0")
}

object BufferSize {

  implicit object BufferSizeFinder extends (Config => String => BufferSize) {
    override def apply(config: Config): String => BufferSize = key => BufferSize(config.getInt(key))
  }
}

case class TripletsFinderThreads(value: Int) extends TinyType[Int] {
  verify(value > 0, s"'$value' is not > 0")
}

object TripletsFinderThreads {

  implicit object TriplesFinderThreadsFinder extends (Config => String => TripletsFinderThreads) {
    override def apply(config: Config): String => TripletsFinderThreads = key => TripletsFinderThreads(config.getInt(key))
  }
}

case class QueueConfig(bufferSize: BufferSize,
                       tripletsFinderThreads: TripletsFinderThreads)

object QueueConfig {

  import ch.datascience.config.ConfigOps.Implicits._

  def apply(config: Config): QueueConfig = QueueConfig(
    config.get[BufferSize]("queue.buffer-size"),
    config.get[TripletsFinderThreads]("queue.triplets-finder-threads")
  )
}
