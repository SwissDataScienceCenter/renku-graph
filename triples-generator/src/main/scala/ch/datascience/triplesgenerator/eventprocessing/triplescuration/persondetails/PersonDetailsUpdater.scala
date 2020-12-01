/*
 * Copyright 2020 Swiss Data Science Center (SDSC)
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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration
package persondetails

import cats.MonadError
import cats.data.NonEmptyList
import cats.syntax.all._
import ch.datascience.graph.model.users.{Email, Name, ResourceId}

private[triplescuration] trait PersonDetailsUpdater[Interpretation[_]] {
  def curate(curatedTriples: CuratedTriples[Interpretation]): Interpretation[CuratedTriples[Interpretation]]
}

private class PersonDetailsUpdaterImpl[Interpretation[_]](
    personExtractor: PersonExtractor[Interpretation],
    updatesCreator:  UpdatesCreator
)(implicit ME:       MonadError[Interpretation, Throwable])
    extends PersonDetailsUpdater[Interpretation] {

  import personExtractor._
  import updatesCreator._

  def curate(curatedTriples: CuratedTriples[Interpretation]): Interpretation[CuratedTriples[Interpretation]] =
    for {
      triplesAndPersons <- extractPersons(curatedTriples.triples)
    } yield {
      val (updatedTriples, persons) = triplesAndPersons
      val newUpdatesGroups          = persons map prepareUpdates[Interpretation]
      CuratedTriples(updatedTriples, curatedTriples.updatesGroups ++ newUpdatesGroups)
    }
}

private[triplescuration] object PersonDetailsUpdater {

  def apply[Interpretation[_]]()(implicit
      ME: MonadError[Interpretation, Throwable]
  ): PersonDetailsUpdater[Interpretation] = new PersonDetailsUpdaterImpl[Interpretation](
    new PersonExtractor[Interpretation],
    new UpdatesCreator
  )

  final case class Person(id: ResourceId, names: NonEmptyList[Name], emails: Set[Email])
}
