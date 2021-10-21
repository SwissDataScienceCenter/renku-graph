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

package io.renku.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.data.OptionT
import cats.effect.{ContextShift, IO, Timer}
import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.Schemas.schema
import io.renku.graph.model.entities.Person
import io.renku.graph.model.users
import io.renku.graph.model.users.{Email, GitLabId, ResourceId}
import io.renku.graph.model.views.SparqlValueEncoder.sparqlEncode
import io.renku.rdfstore.SparqlQuery.Prefixes
import io.renku.rdfstore._
import org.typelevel.log4cats.Logger

import scala.concurrent.ExecutionContext

private trait KGPersonFinder[F[_]] {
  def find(person: Person): F[Option[Person]]
}

private object KGPersonFinder {
  def apply(logger:     Logger[IO], timeRecorder: SparqlQueryTimeRecorder[IO])(implicit
      executionContext: ExecutionContext,
      contextShift:     ContextShift[IO],
      timer:            Timer[IO]
  ): IO[KGPersonFinder[IO]] = for {
    rdfStoreConfig <- RdfStoreConfig[IO]()
  } yield new KGPersonFinderImpl(rdfStoreConfig, logger, timeRecorder)
}

private class KGPersonFinderImpl(rdfStoreConfig: RdfStoreConfig,
                                 logger:         Logger[IO],
                                 timeRecorder:   SparqlQueryTimeRecorder[IO]
)(implicit executionContext:                     ExecutionContext, contextShift: ContextShift[IO], timer: Timer[IO])
    extends RdfStoreClientImpl(rdfStoreConfig, logger, timeRecorder)
    with KGPersonFinder[IO] {

  import eu.timepit.refined.auto._
  import io.circe.Decoder

  override def find(person: Person): IO[Option[Person]] =
    findQueries(person)
      .foldLeft(OptionT.none[IO, Person]) { (maybePerson, query) =>
        maybePerson.orElseF(queryExpecting(using = query)(recordsDecoder(person)))
      }
      .value

  private def findQueries(person: Person) = List(
    person.maybeGitLabId.map(findByGitlabId),
    person.maybeEmail.map(findByEmail),
    findByResourceId(person.resourceId).some
  ).flatten

  private def findByGitlabId(gitLabId: GitLabId) = SparqlQuery.of(
    name = "transformation - find person by gitLabId",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?resourceId ?name ?maybeEmail ?maybeGitLabId ?maybeAffiliation
        |WHERE {
        |  ?sameAsId schema:additionalType  'GitLab';
        |            schema:identifier      $gitLabId.
        |  ?resourceId schema:sameAs ?sameAsId;
        |              a schema:Person;
        |              schema:name ?name.
        |  OPTIONAL { ?resourceId schema:email ?maybeEmail }            
        |  OPTIONAL { ?resourceId schema:affiliation ?maybeAffiliation }            
        |  BIND ($gitLabId AS ?maybeGitLabId)          
        |}
        |""".stripMargin
  )

  private def findByEmail(email: Email) = SparqlQuery.of(
    name = "transformation - find person by email",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?resourceId ?name ?maybeEmail ?maybeGitLabId ?maybeAffiliation
        |WHERE {
        |  ?resourceId schema:email '${sparqlEncode(email.value)}';
        |              a schema:Person;
        |              schema:name ?name.
        |  OPTIONAL {
        |    ?resourceId schema:sameAs ?sameAsId.
        |    ?sameAsId schema:additionalType  'GitLab';
        |              schema:identifier      ?maybeGitLabId.
        |  }
        |  OPTIONAL { ?resourceId schema:affiliation ?maybeAffiliation }
        |  BIND ('${sparqlEncode(email.value)}' AS ?maybeEmail)
        |}
        |""".stripMargin
  )

  private def findByResourceId(resourceId: ResourceId) = SparqlQuery.of(
    name = "transformation - find person by resourceId",
    Prefixes.of(schema -> "schema"),
    s"""|SELECT DISTINCT ?resourceId ?name ?maybeEmail ?maybeGitLabId ?maybeAffiliation
        |WHERE {
        |  BIND (<${sparqlEncode(resourceId.value)}> AS ?resourceId)
        |  ?resourceId a schema:Person;
        |              schema:name ?name.
        |  OPTIONAL { ?resourceId schema:email ?maybeEmail }
        |  OPTIONAL {
        |    ?resourceId schema:sameAs ?sameAsId.
        |    ?sameAsId schema:additionalType  'GitLab';
        |              schema:identifier      ?maybeGitLabId.
        |  }
        |  OPTIONAL { ?resourceId schema:affiliation ?maybeAffiliation }
        |}
        |""".stripMargin
  )

  private def recordsDecoder(person: Person): Decoder[Option[Person]] = { cursor =>
    import Decoder._
    import io.renku.tinytypes.json.TinyTypeDecoders._

    val personEntities: Decoder[Person] = { cursor =>
      for {
        resourceId       <- cursor.downField("resourceId").downField("value").as[users.ResourceId]
        name             <- cursor.downField("name").downField("value").as[users.Name]
        maybeEmail       <- cursor.downField("maybeEmail").downField("value").as[Option[users.Email]]
        maybeGitLabId    <- cursor.downField("maybeGitLabId").downField("value").as[Option[users.GitLabId]]
        maybeAffiliation <- cursor.downField("maybeAffiliation").downField("value").as[Option[users.Affiliation]]
      } yield Person(resourceId, name, maybeEmail, maybeAffiliation, maybeGitLabId)
    }

    cursor.downField("results").downField("bindings").as(decodeList(personEntities)).flatMap {
      case Nil           => Option.empty[Person].asRight
      case person :: Nil => Some(person).asRight
      case _             => DecodingFailure(s"Multiple Person entities found for ${person.resourceId}", Nil).asLeft
    }
  }
}
