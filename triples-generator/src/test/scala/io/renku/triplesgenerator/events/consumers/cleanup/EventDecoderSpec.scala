package io.renku.triplesgenerator.events.consumers.cleanup

import io.circe._
import io.circe.literal._
import io.renku.events.EventRequestContent
import io.renku.events.consumers.ConsumersModelGenerators.consumerProjects
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.jsons
import org.scalatest.EitherValues
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class EventDecoderSpec extends AnyWordSpec with should.Matchers with EitherValues {

  "decode" should {

    "produce a CleanUpEvent if the Json string can be successfully deserialized" in {

      val project = consumerProjects.generateOne
      lazy val event = json"""{
        "categoryName": "CLEAN_UP",
        "project": {
          "id":   ${project.id},
          "path": ${project.path}
        }
      }"""

      EventDecoder
        .decode(EventRequestContent.NoPayload(event))
        .value shouldBe CleanUpEvent(project)
    }

    "fail if decoding fails" in {

      val event = jsons.generateOne
      val result = EventDecoder.decode(EventRequestContent.NoPayload(event))

      result.left.value                                       shouldBe a[DecodingFailure]
      result.left.value.asInstanceOf[DecodingFailure].message shouldBe s"CleanUpEvent cannot be decoded: '$event'"
    }
  }
}
