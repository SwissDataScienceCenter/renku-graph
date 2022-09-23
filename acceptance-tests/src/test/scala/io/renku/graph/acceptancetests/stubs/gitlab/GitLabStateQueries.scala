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

package io.renku.graph.acceptancetests.stubs.gitlab

import cats.Monad
import cats.syntax.all._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabApiStub.{CommitData, PushEvent, State, Webhook}
import io.renku.generators.Generators.Implicits._
import io.renku.graph.model.GraphModelGenerators
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.persons.GitLabId
import io.renku.graph.model.projects.{Id, Path, Visibility}
import io.renku.graph.model.testentities.Person
import io.renku.http.client.AccessToken
import io.renku.http.server.security.model.AuthUser

/** Collection of functions to query the state in [[GitLabApiStub]]. */
trait GitLabStateQueries {
  type StateQuery[A] = State => A

  implicit def stateQueryMonad: Monad[StateQuery] =
    new Monad[StateQuery] {
      override def pure[A](x: A): StateQuery[A] = _ => x

      override def flatMap[A, B](fa: StateQuery[A])(f: A => StateQuery[B]): StateQuery[B] =
        state => {
          val a = fa(state)
          f(a)(state)
        }

      override def tailRecM[A, B](a: A)(f: A => StateQuery[Either[A, B]]): StateQuery[B] = {
        @annotation.tailrec
        def loop(a: A, state: State): B =
          f(a)(state) match {
            case Right(b)    => b
            case Left(nextA) => loop(nextA, state)
          }

        state => loop(a, state)
      }
    }

  def projectCommits(projectId: Id): StateQuery[List[CommitData]] =
    _.commits.get(projectId).map(_.toList).getOrElse(Nil)

  def commitsFor(projectId: Id, user: Option[GitLabId]): StateQuery[List[CommitData]] =
    for {
      project <- findProject(projectId, user)
      commits <- project.traverse(p => projectCommits(p.id))
    } yield commits.getOrElse(Nil)

  def findCommit(projectId: Id, user: Option[GitLabId], sha: CommitId): StateQuery[Option[CommitData]] =
    commitsFor(projectId, user).andThen(_.find(_.commitId == sha))

  def findPushEvents(projectId: Id, user: Option[GitLabId]): StateQuery[List[PushEvent]] =
    commitsFor(projectId, user).andThen(_.map(_.toPushEvent(projectId)))

  def findUserByToken(token: AccessToken): StateQuery[Option[AuthUser]] =
    _.users.find(_._2 == token).map(AuthUser.tupled)

  def findPersonById(id: GitLabId): StateQuery[Option[Person]] =
    _.persons.find(_.maybeGitLabId == id.some)

  def projectsFor(user: Option[GitLabId]): StateQuery[List[Project]] =
    _.projects.filter { p =>
      p.entitiesProject.visibility == Visibility.Public ||
      user.exists(p.entitiesProject.members.flatMap(_.maybeGitLabId).contains) ||
      p.entitiesProject.maybeCreator.flatMap(_.maybeGitLabId) == user
    }

  def findProject(id: Id, user: Option[GitLabId]): StateQuery[Option[Project]] =
    projectsFor(user).andThen(_.find(p => p.id == id))

  def findProject(path: Path, user: Option[GitLabId]): StateQuery[Option[Project]] =
    for {
      all <- projectsFor(user)
      project = all.find(p => p.path == path)
    } yield project

  def findProjectById(id: Id): StateQuery[Option[Project]] =
    _.projects.find(_.id == id)

  def findProjectByPath(path: Path): StateQuery[Option[Project]] =
    _.projects.find(_.path == path)

  def findWebhooks(projectId: Id): StateQuery[List[Webhook]] =
    _.webhooks.filter(_.projectId == projectId)

  def isProjectBroken(id: Id): StateQuery[Boolean] =
    _.brokenProjects.contains(id)

  def isProjectBroken(path: Path): StateQuery[Boolean] =
    for {
      project <- findProjectByPath(path)
      broken  <- project.traverse(p => isProjectBroken(p.id))
    } yield broken.getOrElse(false)

  final implicit class CommitDataOps(self: CommitData) {
    def toPushEvent(projectId: Id): PushEvent =
      PushEvent(projectId, self.commitId, GraphModelGenerators.personGitLabIds.generateOne, self.author.name)
  }
}

object GitLabStateQueries extends GitLabStateQueries