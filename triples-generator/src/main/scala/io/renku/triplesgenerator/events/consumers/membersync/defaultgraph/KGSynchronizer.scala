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

package io.renku.triplesgenerator.events.consumers.membersync
package defaultgraph

import cats.MonadThrow
import cats.effect.Async
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.triplesstore._
import org.typelevel.log4cats.Logger

private[membersync] object KGSynchronizer {
  def apply[F[_]: Async: Logger: SparqlQueryTimeRecorder]: F[KGSynchronizer[F]] = for {
    kgProjectMembersFinder <- KGProjectMembersFinder[F]
    kgPersonFinder         <- KGPersonFinder[F]
    updatesCreator         <- UpdatesCreator[F]
    renkuConnectionConfig  <- RenkuConnectionConfig[F]()
    querySender <- MonadThrow[F].catchNonFatal(new TSClientImpl(renkuConnectionConfig) with QuerySender[F] {
                     override def send(query: SparqlQuery): F[Unit] = updateWithNoResult(query)
                   })
  } yield new KGSynchronizerImpl[F](kgProjectMembersFinder, kgPersonFinder, updatesCreator, querySender)

}

private class KGSynchronizerImpl[F[_]: MonadThrow](kgMembersFinder: KGProjectMembersFinder[F],
                                                   kgPersonFinder: KGPersonFinder[F],
                                                   updatesCreator: UpdatesCreator,
                                                   querySender:    QuerySender[F]
) extends KGSynchronizer[F] {

  override def syncMembers(path: projects.Path, membersInGL: Set[GitLabProjectMember]): F[SyncSummary] = for {
    membersInKG <- kgMembersFinder.findProjectMembers(path)
    membersToAdd = findMembersToAdd(membersInGL, membersInKG)
    membersToAddWithIds <- kgPersonFinder.findPersonIds(membersToAdd)
    insertionUpdates = updatesCreator.insertion(path, membersToAddWithIds)
    membersToRemove  = findMembersToRemove(membersInGL, membersInKG)
    removalUpdates   = updatesCreator.removal(path, membersToRemove)
    _ <- (insertionUpdates ::: removalUpdates).map(querySender.send).sequence
  } yield SyncSummary(membersAdded = membersToAdd.size, membersRemoved = membersToRemove.size)
}
