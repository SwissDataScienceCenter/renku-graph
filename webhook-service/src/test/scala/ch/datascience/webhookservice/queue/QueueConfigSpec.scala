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

    "read 'queue.buffer-size' and 'queue.triplets-finder-threads' to instantiate the QueueConfig" in {
      forAll(positiveInts, positiveInts) { (bufferSizeValue, tripletsFinderThreadsValue) =>
        val config = ConfigFactory.parseMap(
          Map("queue" ->
            Map(
              "buffer-size" -> bufferSizeValue,
              "triplets-finder-threads" -> tripletsFinderThreadsValue
            ).asJava
          ).asJava
        )

        QueueConfig(config) shouldBe QueueConfig(
          BufferSize(bufferSizeValue),
          TripletsFinderThreads(tripletsFinderThreadsValue)
        )
      }
    }

    "throw an IllegalArgumentException if buffer-size is <= 0" in {
      val config = ConfigFactory.parseMap(
        Map("queue" ->
          Map(
            "buffer-size" -> 0,
            "triplets-finder-threads" -> positiveInts.generateOne
          ).asJava
        ).asJava
      )

      an[IllegalArgumentException] should be thrownBy QueueConfig(config)
    }

    "throw an IllegalArgumentException if triplets-finder-threads is <= 0" in {
      val config = ConfigFactory.parseMap(
        Map("queue" ->
          Map(
            "buffer-size" -> positiveInts.generateOne,
            "triplets-finder-threads" -> 0
          ).asJava
        ).asJava
      )

      an[IllegalArgumentException] should be thrownBy QueueConfig(config)
    }
  }
}
