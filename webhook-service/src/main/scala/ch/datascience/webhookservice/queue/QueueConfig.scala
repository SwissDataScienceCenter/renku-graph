package ch.datascience.webhookservice.queue

import ch.datascience.tinytypes.TinyType
import com.typesafe.config.Config

private case class BufferSize(value: Int) extends TinyType[Int] {
  verify(value > 0, s"'$value' is not > 0")
}

private object BufferSize {

  implicit object BufferSizeFinder extends (Config => String => BufferSize) {
    override def apply(config: Config): String => BufferSize = key => BufferSize(config.getInt(key))
  }
}

private case class TriplesFinderThreads(value: Int) extends TinyType[Int] {
  verify(value > 0, s"'$value' is not > 0")
}

private object TriplesFinderThreads {

  implicit object TriplesFinderThreadsFinder extends (Config => String => TriplesFinderThreads) {
    override def apply(config: Config): String => TriplesFinderThreads = key => TriplesFinderThreads(config.getInt(key))
  }
}

private case class FusekiUploadThreads(value: Int) extends TinyType[Int] {
  verify(value > 0, s"'$value' is not > 0")
}

private object FusekiUploadThreads {

  implicit object FusekiUploadThreadsFinder extends (Config => String => FusekiUploadThreads) {
    override def apply(config: Config): String => FusekiUploadThreads = key => FusekiUploadThreads(config.getInt(key))
  }
}

private case class QueueConfig(bufferSize: BufferSize,
                               triplesFinderThreads: TriplesFinderThreads,
                               fusekiUploadThreads: FusekiUploadThreads)

private object QueueConfig {

  import ch.datascience.config.ConfigOps.Implicits._

  def apply(config: Config): QueueConfig = QueueConfig(
    config.get[BufferSize]("queue.buffer-size"),
    config.get[TriplesFinderThreads]("queue.triples-finder-threads"),
    config.get[FusekiUploadThreads]("queue.fuseki-upload-threads")
  )
}
