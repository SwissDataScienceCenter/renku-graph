package ch.datascience.compression

import cats.effect.IO
import cats.syntax.all._
import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators.nonEmptyStrings
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class ZipSpec extends AnyWordSpec with should.Matchers {

  "zip and unzip" should {
    "zip and unzip the content from String to Byte Array " in {
      val content = nonEmptyStrings().generateOne
      (Zip.zip[IO](content) >>= Zip.unzip[IO]).unsafeRunSync() shouldBe content
    }
  }

}
