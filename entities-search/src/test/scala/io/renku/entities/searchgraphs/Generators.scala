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

package io.renku.entities.searchgraphs

import io.renku.triplesstore.SparqlQuery
import io.renku.triplesstore.client.TriplesStoreGenerators.quads
import io.renku.triplesstore.client.syntax._
import org.scalacheck.Gen

private object Generators {

  val insertUpdateCommands: Gen[UpdateCommand] =
    quads.map(UpdateCommand.Insert)
  val deleteUpdateCommands: Gen[UpdateCommand] =
    quads.map(UpdateCommand.Delete)
  val queryUpdateCommands: Gen[UpdateCommand] =
    quads.map(quad => UpdateCommand.Query(SparqlQuery.ofUnsafe("generated query", sparql"INSERT DATA {$quad}")))

  val updateCommands: Gen[UpdateCommand] =
    Gen.oneOf(insertUpdateCommands, deleteUpdateCommands, queryUpdateCommands)
}
