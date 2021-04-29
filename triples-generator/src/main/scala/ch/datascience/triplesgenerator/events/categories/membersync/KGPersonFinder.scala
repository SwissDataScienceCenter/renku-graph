/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.events.categories.membersync

import cats.effect.{ContextShift, IO, Timer}
import ch.datascience.graph.Schemas.{rdf, schema}
import ch.datascience.graph.model.users
import ch.datascience.graph.model.users.{GitLabId, ResourceId}
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait KGPersonFinder[Interpretation[_]] {
  def findPersonIds(
      membersToAdd: Set[GitLabProjectMember]
  ): Interpretation[Set[(GitLabProjectMember, Option[ResourceId])]]
}

private class KGPersonFinderImpl(
    rdfStoreConfig:          RdfStoreConfig,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends IORdfStoreClient(rdfStoreConfig, logger, timeRecorder)
    with KGPersonFinder[IO] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  def findPersonIds(
      membersToAdd: Set[GitLabProjectMember]
  ): IO[Set[(GitLabProjectMember, Option[ResourceId])]] = for {
    gitLabIdsAndIds <- queryExpecting[Set[(GitLabId, ResourceId)]](using = query(membersToAdd)).map(_.toMap)
  } yield membersToAdd.map(member => member -> gitLabIdsAndIds.get(member.gitLabId))

  private implicit lazy val recordsDecoder: Decoder[Set[(GitLabId, ResourceId)]] = { cursor =>
    import Decoder._
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    val tuples: Decoder[(GitLabId, ResourceId)] = { cursor =>
      for {
        personId <- cursor.downField("personId").downField("value").as[users.ResourceId]
        gitLabId <- cursor.downField("gitLabId").downField("value").as[users.GitLabId]
      } yield gitLabId -> personId
    }

    cursor.downField("results").downField("bindings").as(decodeList(tuples)).map(_.toSet)
  }

  private def query(membersToAdd: Set[GitLabProjectMember]) = SparqlQuery.of(
    name = "persons by gitLabId",
    Prefixes.of(schema -> "schema", rdf -> "rdf"),
    s"""|SELECT DISTINCT ?personId ?gitLabId
        |WHERE {
        |  ?personId  rdf:type      schema:Person;
        |             schema:sameAs ?sameAsId.
        |             
        |  ?sameAsId  schema:additionalType  'GitLab';
        |             schema:identifier      ?gitLabId.
        |             
        |  FILTER (?gitLabId IN (${membersToAdd.map(_.gitLabId).mkString(", ")}))
        |}
        |""".stripMargin
  )
}

private object KGPersonFinder {
  def apply(logger:     Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[KGPersonFinderImpl] = for {
    rdfStoreConfig <- RdfStoreConfig[IO]()
  } yield new KGPersonFinderImpl(rdfStoreConfig, logger, timeRecorder)
}
