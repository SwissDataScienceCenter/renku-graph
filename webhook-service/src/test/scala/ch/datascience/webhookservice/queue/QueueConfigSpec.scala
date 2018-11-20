package ch.datascience.webhookservice.queue

import ch.datascience.generators.Generators.Implicits._
import com.typesafe.config.ConfigFactory
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks

import scala.collection.JavaConverters._

class QueueConfigSpec extends WordSpec with PropertyChecks {

  private val positiveInts = Gen.choose(1, 1000)

  "apply" should {

    "read 'queue.buffer-size', 'queue.triples-finder-threads' and 'queue.fuseki-upload-threads' to instantiate the QueueConfig" in {
      forAll(positiveInts, positiveInts, positiveInts) { (bufferSizeValue, triplesFinderThreadsValue, fusekiUploadThreads) =>
        val config = ConfigFactory.parseMap(
          Map("queue" ->
            Map(
              "buffer-size" -> bufferSizeValue,
              "triples-finder-threads" -> triplesFinderThreadsValue,
              "fuseki-upload-threads" -> fusekiUploadThreads
            ).asJava
          ).asJava
        )

        QueueConfig(config) shouldBe QueueConfig(
          BufferSize(bufferSizeValue),
          TriplesFinderThreads(triplesFinderThreadsValue),
          FusekiUploadThreads(fusekiUploadThreads)
        )
      }
    }

    "throw an IllegalArgumentException if buffer-size is <= 0" in {
      val config = ConfigFactory.parseMap(
        Map("queue" ->
          Map(
            "buffer-size" -> 0,
            "triples-finder-threads" -> positiveInts.generateOne,
            "fuseki-upload-threads" -> positiveInts.generateOne
          ).asJava
        ).asJava
      )

      an[IllegalArgumentException] should be thrownBy QueueConfig(config)
    }

    "throw an IllegalArgumentException if triples-finder-threads is <= 0" in {
      val config = ConfigFactory.parseMap(
        Map("queue" ->
          Map(
            "buffer-size" -> positiveInts.generateOne,
            "triples-finder-threads" -> 0,
            "fuseki-upload-threads" -> positiveInts.generateOne
          ).asJava
        ).asJava
      )

      an[IllegalArgumentException] should be thrownBy QueueConfig(config)
    }

    "throw an IllegalArgumentException if fuseki-upload-threads is <= 0" in {
      val config = ConfigFactory.parseMap(
        Map("queue" ->
          Map(
            "buffer-size" -> positiveInts.generateOne,
            "triples-finder-threads" -> positiveInts.generateOne,
            "fuseki-upload-threads" -> 0
          ).asJava
        ).asJava
      )

      an[IllegalArgumentException] should be thrownBy QueueConfig(config)
    }
  }
}
