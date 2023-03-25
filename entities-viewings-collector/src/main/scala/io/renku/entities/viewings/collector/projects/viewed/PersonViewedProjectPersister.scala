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

package io.renku.entities.viewings.collector.projects
package viewed

import cats.MonadThrow
import io.renku.graph.model.{persons, projects}
import io.renku.graph.model.entities.Person
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore.TSClient

private[viewings] trait PersonViewedProjectPersister[F[_]] {
  def persist(event: GLUserViewedProject): F[Unit]
}

private[viewings] object PersonViewedProjectPersister {
  def apply[F[_]: MonadThrow](tsClient: TSClient[F]): PersonViewedProjectPersister[F] =
    new PersonViewedProjectPersisterImpl[F](tsClient)
}

private class PersonViewedProjectPersisterImpl[F[_]: MonadThrow](tsClient: TSClient[F])
    extends PersonViewedProjectPersister[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.GraphClass
  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.syntax._
  import io.renku.triplesstore.{ResultsDecoder, SparqlQuery}
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import tsClient._
  import ProjectViewingEncoder._

  override def persist(event: GLUserViewedProject): F[Unit] =
    findPersonId(event.userId) >>= {
      case None           => ().pure[F]
      case Some(personId) => persistIfOlderOrNone(personId, event)
    }

  private def findPersonId(userId: UserId) =
    queryExpecting[Option[persons.ResourceId]] {
      userId.fold(
        userResourceIdByGLId,
        userResourceIdByEmail
      )
    }(idDecoder)

  private def userResourceIdByGLId(glId: persons.GitLabId) =
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: find user id by glid",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?id
               |WHERE {
               |  GRAPH ${GraphClass.Persons.id} {
               |    ?id schema:sameAs ?sameAsId.
               |    ?sameAsId schema:additionalType ${Person.gitLabSameAsAdditionalType.asTripleObject};
               |              schema:identifier ${glId.asObject}
               |  }
               |}
               |LIMIT 1
               |""".stripMargin
    )
  private def userResourceIdByEmail(email: persons.Email) =
    SparqlQuery.ofUnsafe(
      show"${categoryName.show.toLowerCase}: find user id by email",
      Prefixes of (renku -> "renku", schema -> "schema"),
      sparql"""|SELECT DISTINCT ?id
               |WHERE {
               |  GRAPH ${GraphClass.Persons.id} {
               |    ?id schema:email ${email.asObject}
               |  }
               |}
               |LIMIT 1
               |""".stripMargin
    )

  private lazy val idDecoder: Decoder[Option[persons.ResourceId]] = ResultsDecoder[Option, persons.ResourceId] {
    Decoder.instance[persons.ResourceId] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[persons.ResourceId]("id")
    }
  }

  private def persistIfOlderOrNone(personId: persons.ResourceId, event: GLUserViewedProject) =
    findStoredDate(personId, event.project.id) >>= {
      case None => insert(personId, event)
      case Some(date) if date < event.date =>
        deleteOldViewedDate(personId, event.project.id) >> insert(personId, event)
      case _ => ().pure[F]
    }

  private def findStoredDate(personId:  persons.ResourceId,
                             projectId: projects.ResourceId
  ): F[Option[projects.DateViewed]] =
    queryExpecting {
      SparqlQuery.ofUnsafe(
        show"${categoryName.show.toLowerCase}: find date",
        Prefixes of renku -> "renku",
        sparql"""|SELECT (MAX(?date) AS ?mostRecentDate)
                 |WHERE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    BIND (${personId.asEntityId} AS ?personId)
                 |    ?personId renku:viewedProject ?viewingId.
                 |    ?viewingId renku:project ${projectId.asEntityId};
                 |               renku:dateViewed ?date.
                 |  }
                 |}
                 |GROUP BY ?id
                 |""".stripMargin
      )
    }(dateDecoder)

  private lazy val dateDecoder: Decoder[Option[projects.DateViewed]] = ResultsDecoder[Option, projects.DateViewed] {
    Decoder.instance[projects.DateViewed] { implicit cur =>
      import io.renku.tinytypes.json.TinyTypeDecoders._
      extract[projects.DateViewed]("mostRecentDate")
    }
  }

  private def deleteOldViewedDate(personId: persons.ResourceId, projectId: projects.ResourceId): F[Unit] =
    updateWithNoResult(
      SparqlQuery.ofUnsafe(
        show"${categoryName.show.toLowerCase}: delete",
        Prefixes of renku -> "renku",
        sparql"""|DELETE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ?viewingId ?p ?o
                 |  }
                 |}
                 |WHERE {
                 |  GRAPH ${GraphClass.PersonViewings.id} {
                 |    ${personId.asEntityId} renku:viewedProject ?viewingId.
                 |    ?viewingId renku:project ${projectId.asEntityId};
                 |               ?p ?o.
                 |  }
                 |}
                 |""".stripMargin
      )
    )

  private def insert(personId: persons.ResourceId, event: GLUserViewedProject): F[Unit] =
    upload(
      encode(PersonViewedProject(personId, event.project, event.date))
    )
}
