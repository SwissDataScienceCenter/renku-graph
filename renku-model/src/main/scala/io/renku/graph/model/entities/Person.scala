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

package io.renku.graph.model.entities

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.graph.model.users.{Affiliation, Email, GitLabId, Name, ResourceId}
import io.renku.graph.model.{GitLabApiUrl, RenkuBaseUrl}
import io.renku.jsonld._

final case class Person(
    resourceId:       ResourceId,
    name:             Name,
    maybeEmail:       Option[Email],
    maybeAffiliation: Option[Affiliation],
    maybeGitLabId:    Option[GitLabId]
)

object Person {

  def apply(
      resourceId:       ResourceId,
      name:             Name,
      maybeEmail:       Option[Email] = None,
      maybeAffiliation: Option[Affiliation] = None,
      maybeGitLabId:    Option[GitLabId] = None
  ): Person = new Person(resourceId, name, maybeEmail, maybeAffiliation, maybeGitLabId)

  import io.renku.graph.model.Schemas._
  import io.renku.jsonld.JsonLDDecoder.decodeOption
  import io.renku.jsonld.JsonLDEncoder.encodeOption
  import io.renku.jsonld.syntax._

  val entityTypes: EntityTypes = EntityTypes.of(prov / "Person", schema / "Person")

  implicit def encoder(implicit renkuBaseUrl: RenkuBaseUrl, gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[Person] =
    JsonLDEncoder.instance { person =>
      JsonLD.entity(
        person.maybeGitLabId.map(ResourceId(_)).getOrElse(person.resourceId).asEntityId,
        entityTypes,
        schema / "email"       -> person.maybeEmail.asJsonLD,
        schema / "name"        -> person.name.asJsonLD,
        schema / "affiliation" -> person.maybeAffiliation.asJsonLD,
        schema / "sameAs"      -> person.maybeGitLabId.asJsonLD(encodeOption(gitLabIdEncoder))
      )
    }

  private val gitLabSameAsTypes:          EntityTypes = EntityTypes.of(schema / "URL")
  private val gitLabSameAsAdditionalType: String      = "GitLab"

  def gitLabIdEncoder(implicit gitLabApiUrl: GitLabApiUrl): JsonLDEncoder[GitLabId] = JsonLDEncoder.instance {
    gitLabId =>
      JsonLD.entity(
        EntityId of (gitLabApiUrl / "users" / gitLabId).toString,
        gitLabSameAsTypes,
        schema / "identifier"     -> gitLabId.value.asJsonLD,
        schema / "additionalType" -> gitLabSameAsAdditionalType.asJsonLD
      )
  }

  implicit lazy val decoder: JsonLDDecoder[Person] = JsonLDDecoder.entity(entityTypes) { cursor =>
    import io.renku.graph.model.views.StringTinyTypeJsonLDDecoders._
    for {
      resourceId       <- cursor.downEntityId.as[ResourceId]
      names            <- cursor.downField(schema / "name").as[List[Name]]
      maybeEmail       <- cursor.downField(schema / "email").as[Option[Email]]
      maybeGitLabId    <- cursor.downField(schema / "sameAs").as[Option[GitLabId]](decodeOption(gitLabIdDecoder))
      maybeAffiliation <- cursor.downField(schema / "affiliation").as[Option[Affiliation]]
      name <- if (names.isEmpty) DecodingFailure(s"No name on Person $resourceId", Nil).asLeft
              else names.reverse.head.asRight
    } yield Person(resourceId, name, maybeEmail, maybeAffiliation, maybeGitLabId)
  }

  private lazy val gitLabIdDecoder: JsonLDDecoder[GitLabId] = JsonLDDecoder.entity(gitLabSameAsTypes) { cursor =>
    for {
      _ <- cursor.downField(schema / "additionalType").as[String].flatMap {
             case `gitLabSameAsAdditionalType` => ().asRight
             case additionalType => DecodingFailure(s"Unknown $additionalType type of Person SameAs", Nil).asLeft
           }
      gitLabId <- cursor.downField(schema / "identifier").as[GitLabId]
    } yield gitLabId
  }
}
