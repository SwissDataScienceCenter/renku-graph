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

package io.renku.knowledgegraph.ontology

import cats.data.NonEmptyList
import io.renku.entities.searchgraphs.SearchInfoOntology
import io.renku.entities.viewings.collector.{PersonViewingOntology, ProjectViewedTimeOntology}
import io.renku.graph.model.Schemas
import io.renku.graph.model.entities.{CompositePlan, Project}
import io.renku.jsonld.JsonLD
import io.renku.jsonld.ontology.generateOntology

private trait OntologyGenerator {
  def getOntology: JsonLD
}

private object OntologyGenerator {
  private val types =
    NonEmptyList.of(Project.Ontology.typeDef,
                    CompositePlan.Ontology.typeDef,
                    SearchInfoOntology.typeDef,
                    ProjectViewedTimeOntology.typeDef,
                    PersonViewingOntology.typeDef
    )

  private val instance = new OntologyGeneratorImpl(generateOntology(types, Schemas.renku))

  def apply(): OntologyGenerator = instance
}

private class OntologyGeneratorImpl(generate: => JsonLD) extends OntologyGenerator {
  override def getOntology: JsonLD = generate
}
