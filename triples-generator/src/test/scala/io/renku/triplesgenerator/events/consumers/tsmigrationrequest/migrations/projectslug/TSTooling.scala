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

package io.renku.triplesgenerator.events.consumers.tsmigrationrequest.migrations.projectslug

import cats.effect.IO
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.graph.model.Schemas.renku
import io.renku.graph.model.testentities.{Project, ProjectOps}
import io.renku.graph.model.{GraphClass, RenkuUrl, projects}
import io.renku.jsonld.syntax._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore.client.syntax._
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQuery}

trait TSTooling {
  self: InMemoryJenaForSpec with ProjectsDataset =>

  protected def deleteRenkuSlugProps(projects: List[Project])(implicit ru: RenkuUrl): IO[Unit] =
    projects.traverse_(p => deleteRenkuSlugProp(p.resourceId))

  protected def deleteRenkuSlugProp(id: projects.ResourceId): IO[Unit] =
    deleteProjectRenkuSlug(id) >> deleteProjectsRenkuSlug(id)

  protected def deleteProjectRenkuSlug(id: projects.ResourceId): IO[Unit] =
    runUpdate(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test renku:slug delete from Project",
        Prefixes of renku -> "renku",
        sparql"""|DELETE {
                 |  GRAPH ?id { ?id renku:slug ?slg }
                 |}
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  GRAPH ?id { ?id renku:slug ?slg }
                 |}
                 |""".stripMargin
      )
    )

  protected def deleteProjectsRenkuSlug(id: projects.ResourceId): IO[Unit] =
    runUpdate(
      on = projectsDataset,
      SparqlQuery.ofUnsafe(
        "test renku:slug delete from Projects",
        Prefixes of renku -> "renku",
        sparql"""|DELETE {
                 |  GRAPH ?gid { ?id renku:slug ?slg }
                 |}
                 |WHERE {
                 |  BIND (${id.asEntityId} AS ?id)
                 |  BIND (${GraphClass.Projects.id} AS ?gid)
                 |  GRAPH ?gid { ?id renku:slug ?slg }
                 |}
                 |""".stripMargin
      )
    )
}
