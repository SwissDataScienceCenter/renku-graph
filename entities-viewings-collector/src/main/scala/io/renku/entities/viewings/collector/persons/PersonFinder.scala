/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.entities.viewings.collector.persons

import cats.MonadThrow
import io.renku.graph.model.entities.Person
import io.renku.graph.model.persons
import io.renku.triplesgenerator.api.events.UserId
import io.renku.triplesstore.TSClient
import io.renku.graph.model.views.TinyTypeToObject._

private trait PersonFinder[F[_]] {
  def findPersonId(userId: UserId): F[Option[persons.ResourceId]]
}

private object PersonFinder {
  def apply[F[_]: MonadThrow](tsClient: TSClient[F]): PersonFinder[F] =
    new PersonFinderImpl[F](tsClient)
}

private class PersonFinderImpl[F[_]](tsClient: TSClient[F]) extends PersonFinder[F] {

  import cats.syntax.all._
  import eu.timepit.refined.auto._
  import io.circe.Decoder
  import io.renku.graph.model.GraphClass
  import io.renku.graph.model.Schemas._
  import io.renku.triplesstore.{ResultsDecoder, SparqlQuery}
  import io.renku.triplesstore.ResultsDecoder._
  import io.renku.triplesstore.client.syntax._
  import io.renku.triplesstore.SparqlQuery.Prefixes
  import tsClient._

  override def findPersonId(userId: UserId): F[Option[persons.ResourceId]] =
    queryExpecting[Option[persons.ResourceId]] {
      userId.fold(
        userResourceIdByGLId,
        userResourceIdByEmail
      )
    }(idDecoder)

  private def userResourceIdByGLId(glId: persons.GitLabId) =
    SparqlQuery.ofUnsafe(
      show"${GraphClass.PersonViewings}: find user id by glid",
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
      show"${GraphClass.PersonViewings}: find user id by email",
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
}
