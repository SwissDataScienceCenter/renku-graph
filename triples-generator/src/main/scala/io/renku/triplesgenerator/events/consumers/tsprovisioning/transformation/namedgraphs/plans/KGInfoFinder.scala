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

package io.renku.triplesgenerator.events.consumers.tsprovisioning.transformation.namedgraphs.plans

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.{plans, projects}
import io.renku.triplesstore.{ProjectsConnectionConfig, SparqlQueryTimeRecorder, TSClientImpl}
import org.typelevel.log4cats.Logger

private trait KGInfoFinder[F[_]] {
  def findCreatedDates(projectId: projects.ResourceId, planId: plans.ResourceId): F[List[plans.DateCreated]]
}

private object KGInfoFinder {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGInfoFinder[F]] =
    ProjectsConnectionConfig[F]().map(new KGInfoFinderImpl(_))
}

private class KGInfoFinderImpl[F[_]: Async: Logger: SparqlQueryTimeRecorder](storeConfig: ProjectsConnectionConfig)
    extends TSClientImpl(storeConfig)
    with KGInfoFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.Schemas.{prov, schema}
  import io.renku.graph.model._
  import io.renku.jsonld.syntax._
  import io.renku.tinytypes.json.TinyTypeDecoders._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import io.renku.triplesstore._
  import io.renku.triplesstore.client.syntax._

  override def findCreatedDates(projectId: projects.ResourceId,
                                planId:    plans.ResourceId
  ): F[List[plans.DateCreated]] = {
    implicit val decoder: Decoder[List[plans.DateCreated]] = ResultsDecoder[List, plans.DateCreated] { implicit cur =>
      extract[plans.DateCreated]("dateCreated")
    }

    queryExpecting[List[plans.DateCreated]](
      SparqlQuery.of(
        name = "transformation - find plan dateCreated",
        Prefixes of (schema -> "schema", prov -> "prov"),
        sparql"""|SELECT DISTINCT ?dateCreated
                 |FROM ${GraphClass.Project.id(projectId)} {
                 |  ${planId.asEntityId} a prov:Plan;
                 |                       schema:dateCreated ?dateCreated.
                 |}
                 |""".stripMargin
      )
    ).map(_.sorted)
  }
}
