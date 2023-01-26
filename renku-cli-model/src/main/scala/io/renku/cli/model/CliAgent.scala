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

package io.renku.cli.model

import cats.syntax.all._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{JsonLDDecoder, JsonLDEncoder}

sealed trait CliAgent {
  def fold[A](fa: CliPerson => A, fb: CliSoftwareAgent => A): A
}

object CliAgent {

  final case class Person(person: CliPerson) extends CliAgent {
    def fold[A](fa: CliPerson => A, fb: CliSoftwareAgent => A): A = fa(person)
  }

  final case class Software(agent: CliSoftwareAgent) extends CliAgent {
    def fold[A](fa: CliPerson => A, fb: CliSoftwareAgent => A): A = fb(agent)
  }

  def apply(person: CliPerson): CliAgent.Person = Person(person)

  def apply(agent: CliSoftwareAgent): CliAgent.Software = Software(agent)

  implicit val jsonLDDecoder: JsonLDDecoder[CliAgent] =
    (CliSoftwareAgent.jsonLDCliModelDecoder orElse CliPerson.jsonLDCliModelDecoder).emap {
      case a: CliSoftwareAgent => Right(CliAgent(a))
      case a: CliPerson        => Right(CliAgent(a))
      case _ => "Cannot decode entity as CliAgent".asLeft
    }

  implicit def jsonLDEncoder[A <: CliAgent]: JsonLDEncoder[A] =
    JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD))
}
