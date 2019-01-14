package ch.datascience.graph.events

import java.time.LocalDateTime
import java.time.ZoneOffset.ofHours

import io.circe._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class CommittedDateSpec extends WordSpec {

  "apply" should {

    "be able to instantiate from date time with zone json string" in {
      val Right(decoded) = CommittedDate.decoder.decodeJson(Json.fromString("2012-09-20T09:06:12+03:00"))

      decoded shouldBe CommittedDate(LocalDateTime.of(2012, 9, 20, 9, 6, 12).atOffset(ofHours(3)).toInstant)
    }
  }
}
