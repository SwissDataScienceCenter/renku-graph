/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
 * A partnership between École Polytechnique Fédérale de Lausanne (EPFL) and
 * Eidgenössische Technische Hochschule Zürich (ETHZ).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.datascience.generators

import org.scalacheck.Gen
import org.scalacheck.Gen._

import scala.language.implicitConversions

object Generators {

  implicit def nonEmptyStrings( maxLength: Int = 10 ): Gen[String] = {
    require( maxLength > 0 )

    for {
      length <- choose( 1, maxLength )
      chars <- listOfN( length, alphaChar )
    } yield chars.mkString( "" )
  }

  val relativePaths: Gen[String] = for {
    partsNumber <- Gen.choose( 1, 10 )
    parts <- Gen.listOfN( partsNumber, nonEmptyStrings() )
  } yield parts.mkString( "/" )

  object Implicits {

    implicit class GenOps[T]( generator: Gen[T] ) {
      def generateOne: T = generator.sample getOrElse generateOne
    }
  }
}
