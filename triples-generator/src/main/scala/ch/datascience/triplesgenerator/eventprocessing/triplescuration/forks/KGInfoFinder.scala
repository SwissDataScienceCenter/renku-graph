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

package ch.datascience.triplesgenerator.eventprocessing.triplescuration.forks

import cats.effect.IO
import ch.datascience.graph.model.events.Project
import ch.datascience.graph.model.users

import scala.language.higherKinds

private trait KGInfoFinder[Interpretation[_]] {
  def findProject(project: Project):     Interpretation[Option[KGProject]]
  def findCreatorId(email: users.Email): Interpretation[Option[users.ResourceId]]
}

private class IOKGInfoFinder extends KGInfoFinder[IO] {
  override def findProject(project: Project):     IO[Option[KGProject]]        = IO.pure(None)
  override def findCreatorId(email: users.Email): IO[Option[users.ResourceId]] = IO.pure(None)
}

private object IOKGInfoFinder {
  def apply(): IO[KGInfoFinder[IO]] = IO.pure(new IOKGInfoFinder)
}
