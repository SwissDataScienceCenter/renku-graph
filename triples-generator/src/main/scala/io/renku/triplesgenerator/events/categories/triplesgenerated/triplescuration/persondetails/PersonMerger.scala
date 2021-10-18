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

import cats.MonadThrow
import cats.syntax.all._
import io.renku.graph.model.entities.Person

private trait PersonMerger {

  def merge[Interpretation[_]: MonadThrow](modelPerson: Person, kgPerson: Person): Interpretation[Person] =
    validate(modelPerson, kgPerson) map { _ =>
      Person(
        kgPerson.resourceId,
        modelPerson.name,
        modelPerson.alternativeNames + kgPerson.name,
        modelPerson.maybeEmail orElse kgPerson.maybeEmail,
        modelPerson.maybeAffiliation orElse kgPerson.maybeAffiliation,
        modelPerson.maybeGitLabId orElse kgPerson.maybeGitLabId
      )
    }

  private def validate[Interpretation[_]: MonadThrow](modelPerson: Person, kgPerson: Person): Interpretation[Unit] =
    if (
      (modelPerson.maybeGitLabId -> kgPerson.maybeGitLabId).mapN(_ == _).getOrElse(false) ||
      (modelPerson.maybeEmail    -> kgPerson.maybeEmail).mapN(_ == _).getOrElse(false) ||
      (modelPerson.resourceId == kgPerson.resourceId)
    ) ().pure[Interpretation]
    else
      new IllegalArgumentException(
        s"Persons ${modelPerson.resourceId} and ${kgPerson.resourceId} do not have matching identifiers"
      ).raiseError[Interpretation, Unit]

}

private object PersonMerger extends PersonMerger
