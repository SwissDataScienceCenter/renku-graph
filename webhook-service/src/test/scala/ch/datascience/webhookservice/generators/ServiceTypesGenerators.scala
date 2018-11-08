package ch.datascience.webhookservice.generators

import org.scalacheck.Gen

object ServiceTypesGenerators {

  implicit val shas: Gen[String] = for {
    length <- Gen.choose(5, 40)
    chars <- Gen.listOfN(length, Gen.oneOf((0 to 9).map(_.toString) ++ ('a' to 'f').map(_.toString)))
  } yield chars.mkString("")
}
