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

package io.renku.triplesstore.client

import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{localDates, timestamps}
import io.renku.generators.jsonld.JsonLDGenerators._
import io.renku.triplesstore.client.model.{Quad, Triple, TripleObject}
import org.scalacheck.{Arbitrary, Gen}

object TriplesStoreGenerators {

  implicit val booleanTripleObjects: Gen[TripleObject.Boolean] = Arbitrary.arbBool.arbitrary map TripleObject.Boolean
  implicit val intTripleObjects:     Gen[TripleObject.Int]     = Arbitrary.arbInt.arbitrary map TripleObject.Int
  implicit val longTripleObjects:    Gen[TripleObject.Long]    = Arbitrary.arbLong.arbitrary map TripleObject.Long
  implicit val floatTripleObjects:   Gen[TripleObject.Float]   = Arbitrary.arbFloat.arbitrary map TripleObject.Float
  implicit val doubleTripleObjects:  Gen[TripleObject.Double]  = Arbitrary.arbDouble.arbitrary map TripleObject.Double
  implicit val stringTripleObjects:  Gen[TripleObject.String]  = Arbitrary.arbString.arbitrary map TripleObject.String
  implicit val instantTripleObjects: Gen[TripleObject.Instant] = timestamps map TripleObject.Instant
  implicit val localDateTripleObjects: Gen[TripleObject.LocalDate] = localDates map TripleObject.LocalDate
  implicit val iriTripleObjects:       Gen[TripleObject.Iri]       = entityIds map TripleObject.Iri

  implicit val tripleObjects: Gen[TripleObject] = Gen.oneOf(
    booleanTripleObjects,
    intTripleObjects,
    longTripleObjects,
    floatTripleObjects,
    doubleTripleObjects,
    stringTripleObjects,
    instantTripleObjects,
    iriTripleObjects
  )

  implicit val triples: Gen[Triple] =
    (entityIds, properties, tripleObjects).mapN(Triple)

  implicit val quads: Gen[Quad] =
    (entityIds, triples).mapN(Quad(_, _))
}
