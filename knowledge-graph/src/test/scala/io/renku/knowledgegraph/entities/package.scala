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

import entities.model._
import eu.timepit.refined.auto._
import io.renku.generators.Generators.nonBlankStrings
import io.renku.graph.model.{RenkuBaseUrl, testentities}
import org.scalacheck.Gen

package object entities {
  import Endpoint._

  val queryParams: Gen[Filters.Query]      = nonBlankStrings(minLength = 5).map(v => Filters.Query(v.value))
  val typeParams:  Gen[Filters.EntityType] = Gen.oneOf(Filters.EntityType.all)

  private[entities] implicit def projectConverter[P <: testentities.Project]: P => Entity.Project = project =>
    Entity.Project(
      project.name,
      project.path,
      project.visibility,
      project.dateCreated,
      project.maybeCreator.map(_.name),
      project.keywords.toList.sorted,
      project.maybeDescription
    )

  private[entities] implicit def datasetConverter[P <: testentities.Project]
      : ((testentities.Dataset[testentities.Dataset.Provenance], P)) => Entity.Dataset = { case (dataset, project) =>
    Entity.Dataset(
      dataset.identification.name,
      project.visibility,
      dataset.provenance.date,
      dataset.provenance.creators.map(_.name).toList.sorted,
      dataset.additionalInfo.keywords.sorted,
      dataset.additionalInfo.maybeDescription
    )
  }

  private[entities] implicit class ProjectDatasetOps[PROV <: testentities.Dataset.Provenance,
                                                     +P <: testentities.Project
  ](datasetAndProject: (testentities.Dataset[PROV], P))(implicit renkuBaseUrl: RenkuBaseUrl) {
    def to[T](implicit convert: ((testentities.Dataset[PROV], P)) => T): T =
      convert(datasetAndProject)
  }
}
