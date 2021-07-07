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

package ch.datascience.graph.model

import cats.syntax.all._
import ch.datascience.graph.model.views.SparqlValueEncoder.sparqlEncode
import ch.datascience.graph.model.views.{EntityIdJsonLdOps, RdfResource, TinyTypeJsonLDOps}
import ch.datascience.tinytypes._
import ch.datascience.tinytypes.constraints.{NonBlank, NonNegativeInt}
import io.renku.jsonld.EntityId

object users {

  final class ResourceId private (val value: String) extends AnyVal with StringTinyType
  implicit object ResourceId
      extends TinyTypeFactory[ResourceId](new ResourceId(_))
      with NonBlank
      with EntityIdJsonLdOps[ResourceId] {

    def apply(id: EntityId): ResourceId = ResourceId(id.value.toString)

    implicit object UsersResourceIdRdfResourceRenderer extends Renderer[RdfResource, ResourceId] {
      private val localPartExtractor = "^mailto:(.*)@.*$".r

      override def render(id: ResourceId): String = id.value match {
        case localPartExtractor(localPart) => s"<${id.value.replace(localPart, sparqlEncode(localPart))}>"
        case otherId                       => s"<$otherId>"
      }
    }
  }

  final class GitLabId private (val value: Int) extends AnyVal with IntTinyType
  implicit object GitLabId
      extends TinyTypeFactory[GitLabId](new GitLabId(_))
      with NonNegativeInt
      with TinyTypeJsonLDOps[GitLabId] {

    def parse(value: String): Either[IllegalArgumentException, GitLabId] =
      Either
        .fromOption(value.toIntOption, ifNone = new IllegalArgumentException(s"$value not a valid GitLabId"))
        .flatMap(GitLabId.from)
  }

  final class Email private (val value: String) extends AnyVal with StringTinyType
  implicit object Email extends TinyTypeFactory[Email](new Email(_)) with NonBlank with TinyTypeJsonLDOps[Email] {

    addConstraint(
      check = _.split('@').toList match {
        case head +: tail +: Nil => head.trim.nonEmpty && tail.trim.nonEmpty
        case _                   => false
      },
      message = value => s"'$value' is not a valid email"
    )

    implicit class EmailOps(email: Email) {
      lazy val extractName: users.Name = users.Name(email.value.split('@').head)
    }
  }

  final class Name private (val value: String) extends AnyVal with StringTinyType
  implicit object Name extends TinyTypeFactory[Name](new Name(_)) with NonBlank with TinyTypeJsonLDOps[Name]

  final class Username private (val value: String) extends AnyVal with StringTinyType
  implicit object Username extends TinyTypeFactory[Username](new Username(_)) with NonBlank

  final class Affiliation private (val value: String) extends AnyVal with StringTinyType
  implicit object Affiliation
      extends TinyTypeFactory[Affiliation](new Affiliation(_))
      with NonBlank
      with TinyTypeJsonLDOps[Affiliation]
}
