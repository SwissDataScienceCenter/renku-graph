package ch.datascience.webhookservice.generators

import ch.datascience.generators.Generators._
import ch.datascience.webhookservice.{FilePath, GitRef}
import org.scalacheck.Gen

object ServiceTypesGenerators {

  val shas: Gen[String] = for {
    length <- Gen.choose(5, 40)
    chars <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'f').map(_.toString)))
  } yield chars.mkString("")

  implicit val filePaths: Gen[FilePath] = relativePaths map FilePath
  implicit val gitRefs: Gen[GitRef] = shas map GitRef
}
