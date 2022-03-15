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

package io.renku.triplesgenerator.events.categories.membersync

import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.persons
import io.renku.graph.model.persons.{GitLabId, ResourceId}
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

private trait KGPersonFinder[F[_]] {
  def findPersonIds(
      membersToAdd: Set[GitLabProjectMember]
  ): F[Set[(GitLabProjectMember, Option[ResourceId])]]
}

private class KGPersonFinderImpl[F[_]: Async: Logger](
    rdfStoreConfig: RdfStoreConfig,
    timeRecorder:   SparqlQueryTimeRecorder[F]
) extends RdfStoreClientImpl(rdfStoreConfig, timeRecorder)
    with KGPersonFinder[F] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  def findPersonIds(membersToAdd: Set[GitLabProjectMember]): F[Set[(GitLabProjectMember, Option[ResourceId])]] = for {
    gitLabIdsAndIds <- queryExpecting[Set[(GitLabId, ResourceId)]](using = query(membersToAdd)).map(_.toMap)
  } yield membersToAdd.map(member => member -> gitLabIdsAndIds.get(member.gitLabId))

  private def query(membersToAdd: Set[GitLabProjectMember]) = SparqlQuery.of(
    name = "persons by gitLabId",
    Prefixes of schema -> "schema",
    s"""|SELECT DISTINCT ?personId ?gitLabId
        |WHERE {
        |  ?personId a schema:Person;
        |            schema:sameAs ?sameAsId.
        |             
        |  ?sameAsId schema:additionalType  'GitLab';
        |            schema:identifier      ?gitLabId.
        |             
        |  FILTER (?gitLabId IN (${membersToAdd.map(_.gitLabId).mkString(", ")}))
        |}
        |""".stripMargin
  )

  private implicit lazy val recordsDecoder: Decoder[Set[(GitLabId, ResourceId)]] = { cursor =>
    import Decoder._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val tuples: Decoder[(GitLabId, ResourceId)] = { cursor =>
      for {
        personId <- cursor.downField("personId").downField("value").as[persons.ResourceId]
        gitLabId <- cursor.downField("gitLabId").downField("value").as[persons.GitLabId]
      } yield gitLabId -> personId
    }

    cursor.downField("results").downField("bindings").as(decodeList(tuples)).map(_.toSet)
  }
}

private object KGPersonFinder {
  def apply[F[_]: Async: Logger](timeRecorder: SparqlQueryTimeRecorder[F]): F[KGPersonFinder[F]] = for {
    rdfStoreConfig <- RdfStoreConfig[F]()
  } yield new KGPersonFinderImpl(rdfStoreConfig, timeRecorder)
}
