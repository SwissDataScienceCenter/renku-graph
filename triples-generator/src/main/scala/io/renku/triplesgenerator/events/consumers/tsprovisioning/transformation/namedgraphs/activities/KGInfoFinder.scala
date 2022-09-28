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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.activities

import cats.effect.Async
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.circe.Decoder
import io.renku.graph.model.Schemas.{prov, schema}
import io.renku.graph.model.views.RdfResource
import io.renku.graph.model._
import io.renku.triplesstore.SparqlQuery.Prefixes
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private trait KGInfoFinder[F[_]] {
  def findActivityAuthors(projectId: projects.ResourceId, resourceId: activities.ResourceId): F[Set[persons.ResourceId]]
  def findAssociationPersonAgents(projectId:  projects.ResourceId,
                                  resourceId: activities.ResourceId
  ): F[Set[persons.ResourceId]]
}

private object KGInfoFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGInfoFinder[F]] =
    ProjectsConnectionConfig[F]().map(new KGInfoFinderImpl(_))
}

private class KGInfoFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](
    connectionConfig: ProjectsConnectionConfig
) extends TSClientImpl(connectionConfig)
    with KGInfoFinder[F] {

  override def findActivityAuthors(projectId:  projects.ResourceId,
                                   resourceId: activities.ResourceId
  ): F[Set[persons.ResourceId]] = queryExpecting[Set[persons.ResourceId]](
    SparqlQuery.of(
      name = "transformation - find activity author",
      Prefixes of (schema -> "schema", prov -> "prov"),
      s"""|SELECT DISTINCT ?personId
          |FROM <${GraphClass.Project.id(projectId)}>
          |FROM <${GraphClass.Persons.id}> {
          |  ${resourceId.showAs[RdfResource]} a prov:Activity;
          |                                    prov:wasAssociatedWith ?personId.
          |  ?personId a schema:Person
          |}
          |""".stripMargin
    )
  )

  override def findAssociationPersonAgents(projectId:  projects.ResourceId,
                                           resourceId: activities.ResourceId
  ): F[Set[persons.ResourceId]] = queryExpecting[Set[persons.ResourceId]](
    SparqlQuery.of(
      name = "transformation - find association agent",
      Prefixes of (schema -> "schema", prov -> "prov"),
      s"""|SELECT DISTINCT ?personId
          |FROM <${GraphClass.Project.id(projectId)}>
          |FROM <${GraphClass.Persons.id}> {
          |  ${resourceId.showAs[RdfResource]} a prov:Activity;
          |                                    prov:qualifiedAssociation/prov:agent ?personId.
          |  ?personId a schema:Person
          |}
          |""".stripMargin
    )
  )

  private implicit val authorsDecoder: Decoder[Set[persons.ResourceId]] = ResultsDecoder[Set, persons.ResourceId] {
    implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[persons.ResourceId]("personId")
  }
}
