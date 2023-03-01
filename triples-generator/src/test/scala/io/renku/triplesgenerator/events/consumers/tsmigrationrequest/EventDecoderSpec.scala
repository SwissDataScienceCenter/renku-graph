package io.renku.triplesgenerator.events.consumers.tsmigrationrequest

import cats.syntax.all._
import io.circe.literal._
import io.circe.DecodingFailure
import io.renku.events.EventRequestContent
import io.renku.generators.CommonGraphGenerators.serviceVersions
import io.renku.generators.Generators.Implicits._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues

class EventDecoderSpec extends AnyWordSpec with should.Matchers with EitherValues {

  private val tgServiceVersion = serviceVersions.generateOne

  "decode" should {

    "succeed if event version and tg version matches" in {

      val eventServiceVersion = tgServiceVersion
      lazy val event = json"""{
        "categoryName": "TS_MIGRATION_REQUEST",
        "subscriber": {
          "version": $eventServiceVersion
        }
      }"""

      EventDecoder.decode(tgServiceVersion)(EventRequestContent.NoPayload(event)).value shouldBe ()
    }

    "fail if event version and tg version do not match" in {

      val eventServiceVersion = serviceVersions.generateOne
      lazy val event =
        json"""{
        "categoryName": "TS_MIGRATION_REQUEST",
        "subscriber": {
          "version": $eventServiceVersion
        }
      }"""

      EventDecoder
        .decode(tgServiceVersion)(EventRequestContent.NoPayload(event))
        .left
        .value
        .getMessage shouldBe show"Service in version '$tgServiceVersion' but event for '$eventServiceVersion'"
    }

    "fail if decoding fails" in {
      EventDecoder
        .decode(tgServiceVersion)(EventRequestContent.NoPayload(json"{}"))
        .left
        .value shouldBe a[DecodingFailure]
    }
  }
}
