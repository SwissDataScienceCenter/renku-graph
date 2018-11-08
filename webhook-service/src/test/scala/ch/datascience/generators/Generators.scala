package ch.datascience.generators

import org.scalacheck.Gen
import org.scalacheck.Gen._

import scala.language.implicitConversions

object Generators {

  implicit def nonEmptyStrings(maxLength: Int = 10): Gen[String] = {
    require(maxLength > 0)

    for {
      length <- choose(1, maxLength)
      chars <- listOfN(length, alphaChar)
    } yield chars.mkString("")
  }

  object Implicits {

    implicit class GenOps[T](generator: Gen[T]) {
      def generateOne: T = generator.sample getOrElse generateOne
    }
  }
}
