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

import cats.MonadError
import cats.effect.ContextShift
import cats.implicits._
import ch.datascience.http.client.AccessToken
import ch.datascience.rdfstore.JsonLDTriples
import ch.datascience.triplesgenerator.eventprocessing.Commit
import ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks.{ForkInfoUpdater, IOForkInfoUpdater}

import scala.language.higherKinds

class TriplesCurator[Interpretation[_]](
    personDetailsUpdater: PersonDetailsUpdater[Interpretation],
    forkInfoUpdater:      ForkInfoUpdater[Interpretation]
)(implicit ME:            MonadError[Interpretation, Throwable]) {

  import forkInfoUpdater._

  def curate(commit:  Commit,
             triples: JsonLDTriples)(implicit maybeAccessToken: Option[AccessToken]): Interpretation[CuratedTriples] =
    for {
      triplesWithPersonDetails <- personDetailsUpdater.curate(CuratedTriples(triples, updates = Nil))
      triplesWithForkInfo      <- updateForkInfo(commit, triplesWithPersonDetails)
    } yield triplesWithForkInfo
}

object IOTriplesCurator {

  import cats.effect.IO

  def apply()(implicit cs: ContextShift[IO]): IO[TriplesCurator[IO]] =
    for {
      forkInfoUpdater <- IOForkInfoUpdater()
    } yield new TriplesCurator[IO](
      new PersonDetailsUpdater[IO](),
      forkInfoUpdater
    )
}
