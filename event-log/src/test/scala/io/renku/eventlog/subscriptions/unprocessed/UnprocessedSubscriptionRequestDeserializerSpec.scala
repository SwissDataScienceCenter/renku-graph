package io.renku.eventlog.subscriptions.unprocessed

import ch.datascience.generators.Generators._
import ch.datascience.graph.model.EventsGenerators._
import io.circe.DecodingFailure
import io.circe.literal._
import io.renku.eventlog.subscriptions.Generators._
import io.renku.eventlog.subscriptions.unprocessed.UnprocessedSubscriptionRequestDeserializer.UrlAndStatuses
import io.renku.jsonld.generators.Generators.Implicits.GenOps
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success, Try}

class UnprocessedSubscriptionRequestDeserializerSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "deserialize" should {
    "deserialize a payload" in new TestCase {

      deserializer.deserialize(payload) shouldBe Success(UrlAndStatuses(subscriberUrl, eventStatusez))
    }

    "fail with DecodingFailure if deserialization fails" in new TestCase {

      val unsupportedPayload = json"""
          {
          ${nonEmptyStrings().generateOne}: ${nonEmptyStrings().generateOne}
          }
            """

      val Failure(error) = deserializer.deserialize(unsupportedPayload)
      error shouldBe a[DecodingFailure]
    }
  }

  class TestCase {
    val subscriberUrl = subscriberUrls.generateOne
    val eventStatusez = eventStatuses.generateNonEmptyList().toList.toSet

    val deserializer = UnprocessedSubscriptionRequestDeserializer[Try]()

    val payload =
      json"""
             {
               "subscriberUrl": ${subscriberUrl.value},
               "statuses": ${eventStatusez.map(_.value)}
             }
          """
  }
}
