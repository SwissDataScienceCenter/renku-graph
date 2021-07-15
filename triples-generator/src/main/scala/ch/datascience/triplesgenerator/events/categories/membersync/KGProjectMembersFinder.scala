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
import ch.datascience.graph.config.RenkuBaseUrl
import ch.datascience.graph.model.{projects, users}
import ch.datascience.graph.model.projects.{Path, ResourceId}
import ch.datascience.graph.model.users.GitLabId
import ch.datascience.graph.model.views.RdfResource
import ch.datascience.rdfstore.SparqlQuery.Prefixes
import ch.datascience.rdfstore.{RdfStoreClientImpl, RdfStoreConfig, SparqlQuery, SparqlQueryTimeRecorder}
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait KGProjectMembersFinder[Interpretation[_]] {
  def findProjectMembers(path: projects.Path): Interpretation[Set[KGProjectMember]]

}

private class KGProjectMembersFinderImpl(
    rdfStoreConfig:          RdfStoreConfig,
    renkuBaseUrl:            RenkuBaseUrl,
    logger:                  Logger[IO],
    timeRecorder:            SparqlQueryTimeRecorder[IO]
)(implicit executionContext: ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder)
    with KGProjectMembersFinder[IO] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  def findProjectMembers(path: projects.Path): IO[Set[KGProjectMember]] =
    queryExpecting[Set[KGProjectMember]](using = query(path))

  private implicit lazy val recordsDecoder: Decoder[Set[KGProjectMember]] = { cursor =>
    import Decoder._
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    val member: Decoder[KGProjectMember] = { cursor =>
      for {
        memberId <- cursor.downField("memberId").downField("value").as[users.ResourceId]
        gitLabId <- cursor.downField("gitLabId").downField("value").as[GitLabId]
      } yield KGProjectMember(memberId, gitLabId)
    }

    cursor.downField("results").downField("bindings").as(decodeList(member)).map(_.toSet)
  }

  private def query(path: Path) = SparqlQuery.of(
    name = "members by project path",
    Prefixes.of(schema -> "schema", rdf -> "rdf"),
    s"""|SELECT DISTINCT ?memberId ?gitLabId
        |WHERE {
        |  ${ResourceId(renkuBaseUrl, path).showAs[RdfResource]} rdf:type      <http://schema.org/Project>;
        |                                                        schema:member ?memberId.                                                     
        |  ?sameAsId  schema:additionalType  'GitLab';
        |             schema:identifier      ?gitLabId ;
        |             ^schema:sameAs         ?memberId .
        |}
        |""".stripMargin
  )

}

private object KGProjectMembersFinder {
  def apply(logger:     Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[KGProjectMembersFinder[IO]] = for {
    rdfStoreConfig <- RdfStoreConfig[IO]()
    renkuBaseUrl   <- RenkuBaseUrl[IO]()

  } yield new KGProjectMembersFinderImpl(rdfStoreConfig, renkuBaseUrl, logger, timeRecorder)
}

private final case class KGProjectMember(resourceId: users.ResourceId, gitLabId: users.GitLabId)
