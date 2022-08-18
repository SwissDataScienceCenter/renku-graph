/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

package io.renku.knowledgegraph

import eu.timepit.refined.auto._
import io.renku.generators.CommonGraphGenerators.sortBys
import io.renku.generators.Generators.nonBlankStrings
import io.renku.graph.model.testentities.Person
import io.renku.knowledgegraph.datasets.DatasetsSearchEndpoint.Query.Phrase
import org.scalacheck.Gen

package object datasets {

  val phrases: Gen[Phrase] = nonBlankStrings(minLength = 5) map (_.value) map Phrase.apply
  implicit val searchEndpointSorts: Gen[DatasetsSearchEndpoint.Sort.By] = sortBys(DatasetsSearchEndpoint.Sort)

  implicit lazy val personToCreator: Person => DatasetCreator =
    person => DatasetCreator(person.maybeEmail, person.name, person.maybeAffiliation)
}
