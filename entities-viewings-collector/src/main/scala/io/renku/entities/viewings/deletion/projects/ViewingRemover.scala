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

package io.renku.entities.viewings.deletion.projects

import cats.syntax.all._
import cats.MonadThrow
import cats.effect.Async
import io.renku.triplesgenerator.api.events.ProjectViewingDeletion
import io.renku.triplesstore.{ProjectsConnectionConfig, ResultsDecoder, SparqlQueryTimeRecorder, TSClient}
import org.typelevel.log4cats.Logger

private trait ViewingRemover[F[_]] {
  def removeViewing(event: ProjectViewingDeletion): F[Unit]
}

private object ViewingRemover {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[ViewingRemover[F]] =
    ProjectsConnectionConfig[F]().map(TSClient[F](_)).map(new ViewingRemoverImpl[F](_))
}

private class ViewingRemoverImpl[F[_]: MonadThrow](tsClient: TSClient[F]) extends ViewingRemover[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.{projects, GraphClass}
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.SparqlQuery
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import tsClient._

  override def removeViewing(event: ProjectViewingDeletion): F[Unit] =
    findProjectId(event) >>= {
      case None            => ().pure[F]
      case Some(projectId) => deleteViewing(projectId)
    }

  private def findProjectId(event: ProjectViewingDeletion) = queryExpecting {
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: find id",
      Prefixes of (renku -> "renku", schema -> "schema"),
      s"""|SELECT DISTINCT ?id
          |WHERE {
          |  GRAPH ?id {
          |    ?id a schema:Project;
          |        renku:projectPath ${event.path.asObject.asSparql.sparql}
          |  }
          |}
          |""".stripMargin
    )
  }(idDecoder)

  private lazy val idDecoder: Decoder[Option[projects.ResourceId]] = ResultsDecoder[Option, projects.ResourceId] {
    Decoder.instance[projects.ResourceId] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.ResourceId]("id")
    }
  }

  private def deleteViewing(projectId: projects.ResourceId): F[Unit] = updateWithNoResult(
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: delete",
      Prefixes of renku -> "renku",
      s"""|DELETE { GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} { ?id ?p ?o } }
          |WHERE {
          |  GRAPH ${GraphClass.ProjectViewedTimes.id.sparql} {
          |    BIND (${projectId.asEntityId.sparql} AS ?id)
          |    ?id ?p ?o
          |  }
          |}
          |""".stripMargin
    )
  )
}
