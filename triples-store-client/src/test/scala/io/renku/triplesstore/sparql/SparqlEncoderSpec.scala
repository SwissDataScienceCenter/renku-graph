/*
 * Copyright 2023 Swiss Data Science Center (SDSC)
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

package io.renku.triplesstore
package sparql

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.triplesstore.TriplesStoreGenerators._
import io.renku.triplesstore.model.Triple
import org.apache.jena.atlas.lib.EscapeStr
import org.apache.jena.util.URIref
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class SparqlEncoderSpec extends AnyWordSpec with should.Matchers with ScalaCheckPropertyChecks {

  import syntax._

  "asSparql" should {

    "be able to encode TripleObject.Boolean as sparql" in {
      val obj = booleanTripleObjects.generateOne
      obj.asSparql.sparql shouldBe s"${obj.value}"
    }

    "be able to encode TripleObject.Int as sparql" in {
      val obj = intTripleObjects.generateOne
      obj.asSparql.sparql shouldBe s"${obj.value}"
    }

    "be able to encode TripleObject.Long as sparql" in {
      val obj = longTripleObjects.generateOne
      obj.asSparql.sparql shouldBe s"${obj.value}"
    }

    "be able to encode TripleObject.Float as sparql" in {
      val obj = floatTripleObjects.generateOne
      obj.asSparql.sparql shouldBe s"${obj.value}"
    }

    "be able to encode TripleObject.Double as sparql" in {
      val obj = doubleTripleObjects.generateOne
      obj.asSparql.sparql shouldBe s"${obj.value}"
    }

    "be able to encode TripleObject.String as sparql with Jena specific characters escaping" in {
      val obj = stringTripleObjects.generateOne
      obj.asSparql.sparql shouldBe s"'${EscapeStr.stringEsc(obj.value)}'"
    }

    "be able to encode TripleObject.Iri as sparql with RFC 2396 specific characters encoding" in {
      val obj = iriTripleObjects.generateOne
      obj.asSparql.sparql shouldBe s"<${URIref.encode(obj.show)}>"
    }

    "be able to encode a Triple as Fragment" in {
      forAll { (triple: Triple) =>
        triple.asSparql.sparql shouldBe
          s"<${URIref.encode(triple.subject.show)}> <${URIref.encode(triple.predicate.show)}> ${triple.obj.asSparql.sparql}"
      }
    }
  }
}
