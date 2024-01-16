/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore.client
package sparql

import io.renku.generators.Generators.{localDates, nonEmptyStrings, timestamps}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.jsonld.JsonLDGenerators.{entityIds, properties}
import org.scalacheck.Arbitrary
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import TriplesStoreGenerators._
import syntax._

class StringInterpolatorSpec extends AnyWordSpec with should.Matchers {

  "fr / sparql interpolator" should {

    "encode a LuceneQuery if used" in {
      val value = LuceneQuery(s"{${nonEmptyStrings(minLength = 3).generateOne}")
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a String if used" in {
      val value = nonEmptyStrings().generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a Char if used" in {
      val value = Arbitrary.arbChar.arbitrary.generateOne
      fr"$value" shouldBe value.toString.asTripleObject.asSparql
    }

    "encode a Float if used" in {
      val value = Arbitrary.arbFloat.arbitrary.generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a Long if used" in {
      val value = Arbitrary.arbLong.arbitrary.generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a Double if used" in {
      val value = Arbitrary.arbDouble.arbitrary.generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a Boolean if used" in {
      val value = Arbitrary.arbBool.arbitrary.generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode an Instant if used" in {
      val value = timestamps.generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a LocalDate if used" in {
      val value = localDates.generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a EntityId if used" in {
      val value = entityIds.generateOne
      fr"$value" shouldBe value.asTripleObject.asSparql
    }

    "encode a Triple if used" in {
      val value = triples.generateOne
      fr"$value" shouldBe value.asSparql
    }

    "encode a Quad if used" in {
      val value = quads.generateOne
      fr"$value" shouldBe value.asSparql
    }

    "encode a TripleObject if used" in {
      val value = tripleObjects.generateOne
      fr"$value" shouldBe value.asSparql
    }

    "encode a Property if used" in {
      val value = properties.generateOne
      fr"$value" shouldBe value.asSparql
    }

    "encode a Fragment if used" in {
      val value = fr"${properties.generateOne}"
      fr"$value" shouldBe value
    }

    "encode a VarName if used" in {
      val value = VarName(s"?${nonEmptyStrings().generateOne}")
      fr"$value" shouldBe Fragment(value.name)
    }

    "encode an Iterable if used in a context of VALUES clause - case with variable wrapped in brackets" in {
      val col = nonEmptyStrings().generateNonEmptyList().toList
      fr"VALUES (?v) { $col }" shouldBe Fragment(
        s"VALUES (?v) { ${col.map(v => fr"$v".sparql).map(s => s"($s)").mkString(" ")} }"
      )
    }

    "encode an Iterable if used in a context of VALUES clause - case with variable without wrapping" in {
      val col = nonEmptyStrings().generateNonEmptyList().toList
      fr"VALUES ?v { $col }" shouldBe Fragment(
        s"VALUES ?v { ${col.map(v => fr"$v".sparql).mkString(" ")} }"
      )
    }

    "encode an Iterable if used in a context of VALUES clause where the variable is in a placeholder too" in {

      val variable = nonEmptyStrings().map(VarName(_)).generateOne
      val col      = nonEmptyStrings().generateNonEmptyList().toList

      fr"VALUES ($variable) { $col }" shouldBe Fragment(
        s"VALUES (${variable.name}) { ${col.map(v => fr"$v".sparql).map(s => s"($s)").mkString(" ")} }"
      )

      fr"VALUES $variable { $col }" shouldBe Fragment(
        s"VALUES ${variable.name} { ${col.map(v => fr"$v".sparql).mkString(" ")} }"
      )
    }

    "encode an Iterable if used in a context of IN clause" in {
      val col = nonEmptyStrings().generateNonEmptyList().toList
      fr"IN ($col)" shouldBe Fragment(
        s"IN (${col.map(v => fr"$v".sparql).mkString(", ")})"
      )
    }

    "encode an Iterable in case of query with new lines" in {
      val col = nonEmptyStrings().generateNonEmptyList().toList
      val actual = fr"""|VALUES (?v)
                        |{ $col }""".stripMargin
      val expected = Fragment {
        s"""|VALUES (?v)
            |{ ${col.map(v => fr"$v".sparql).map(s => s"($s)").mkString(" ")} }"""
      }.stripMargin
      actual shouldBe expected
    }

    "encode an Iterable if used in a context of VALUES clause where IN clause is used before" in {
      val col = nonEmptyStrings().generateNonEmptyList().toList
      fr"IN ('value') VALUES (?v) { $col }" shouldBe Fragment(
        s"IN ('value') VALUES (?v) { ${col.map(v => fr"$v".sparql).map(s => s"($s)").mkString(" ")} }"
      )
    }

    "encode an Iterable if used in a context of IN clause where VALUES clause is used before" in {
      val col = nonEmptyStrings().generateNonEmptyList().toList
      fr"VALUES (?v) { ('value') } IN ($col)" shouldBe Fragment(
        s"VALUES (?v) { ('value') } IN (${col.map(v => fr"$v".sparql).mkString(", ")})"
      )
    }

    "fail if an Iterable is used in a context other than VALUES or IN" in {
      val col = nonEmptyStrings().generateNonEmptyList().toList
      intercept[Exception](fr"some ($col)").getMessage should startWith("Iterable cannot be resolved in this context")
    }

    "encode a non empty Option if used" in {
      val value = nonEmptyStrings().generateOne
      fr"${Option(value)}" shouldBe fr"$value"
    }

    "encode an empty Option if used" in {
      fr"${Option.empty}" shouldBe fr""
    }

    "fail if a value of unsupported type is used" in {
      intercept[Exception](fr"${new {}}").getMessage should startWith("Unsupported value type")
    }
  }
}
