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

package io.renku.triplesgenerator.events.categories.tsprovisioning.transformation.persondetails

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.entities.Person

private trait PersonMerger {

  def merge[F[_]: MonadThrow](modelPerson: Person, kgPerson: Person): F[Person] =
    validate(modelPerson, kgPerson) map { _ =>
      kgPerson match {
        case p: Person.WithGitLabId =>
          p.copy(
            name = modelPerson.name,
            maybeEmail = modelPerson.maybeEmail orElse kgPerson.maybeEmail,
            maybeAffiliation = modelPerson.maybeAffiliation orElse kgPerson.maybeAffiliation
          )
        case p: Person.WithEmail =>
          p.copy(
            name = modelPerson.name,
            maybeAffiliation = modelPerson.maybeAffiliation orElse kgPerson.maybeAffiliation
          )
        case p: Person.WithNameOnly =>
          p.copy(
            name = modelPerson.name,
            maybeAffiliation = modelPerson.maybeAffiliation orElse kgPerson.maybeAffiliation
          )
      }
    }

  private def validate[F[_]: MonadThrow](modelPerson: Person, kgPerson: Person): F[Unit] =
    MonadThrow[F].unlessA(modelPerson.resourceId == kgPerson.resourceId) {
      new IllegalArgumentException(
        s"Persons ${modelPerson.resourceId} and ${kgPerson.resourceId} do not have matching identifiers"
      ).raiseError[F, Unit]
    }
}

private object PersonMerger extends PersonMerger
