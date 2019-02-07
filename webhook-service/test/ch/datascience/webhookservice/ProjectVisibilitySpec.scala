package ch.datascience.webhookservice

import ch.datascience.webhookservice.model.ProjectVisibility
import io.circe.{DecodingFailure, Json}
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class ProjectVisibilitySpec extends WordSpec {

  "projectVisibilityDecoder" should {

    ProjectVisibility.all foreach { visibility =>
      s"deserialize $visibility" in {
        Json.fromString(visibility.value).as[ProjectVisibility] shouldBe Right(visibility)
      }
    }

    "fail for unknown value" in {
      Json.fromString("unknown").as[ProjectVisibility] shouldBe Left(
        DecodingFailure(
          s"'unknown' is not a valid project visibility. Allowed values are: ${ProjectVisibility.all.mkString(", ")}",
          Nil
        )
      )
    }
  }
}
