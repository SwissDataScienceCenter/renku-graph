package ch.datascience.webhookservice.hookcreation

import cats.effect.IO
import eu.timepit.refined.api.{RefType, Refined}
import eu.timepit.refined.string.Url
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.Configuration

class HookCreationConfigProviderSpec extends WordSpec {

  "get" should {

    "return HookCreationConfig object with proper values" in {
      val config = Configuration.from(
        Map(
          "services" -> Map(
            "gitlab" -> Map(
              "url" -> "http://gitlab:1100"
            ),
            "self" -> Map(
              "url" -> "http://self:9001"
            )
          )
        )
      )

      new HookCreationConfigProvider[IO](config).get().unsafeRunSync() shouldBe HookCreationConfig(
        url("http://gitlab:1100"),
        url("http://self:9001")
      )
    }

    "fail if there are no 'services.gitlab.url' and 'services.self.url' in the config" in {
      val config = Configuration.empty

      a[RuntimeException] should be thrownBy new HookCreationConfigProvider[IO](config).get().unsafeRunSync()
    }
  }

  private def url(value: String) =
    RefType
      .applyRef[String Refined Url]( value )
      .getOrElse(throw new IllegalArgumentException( "Invalid IPv4 value" ))
}
