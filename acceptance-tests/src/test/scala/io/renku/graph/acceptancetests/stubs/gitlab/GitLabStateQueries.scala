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

package io.renku.graph.acceptancetests.stubs.gitlab

import GitLabApiStub._
import cats.Monad
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.graph.acceptancetests.data.Project
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabAuth.AuthedReq
import io.renku.graph.acceptancetests.stubs.gitlab.GitLabAuth.AuthedReq.{AuthedProject, AuthedUser}
import io.renku.graph.model.{persons, projects, GraphModelGenerators}
import io.renku.graph.model.events.CommitId
import io.renku.graph.model.testentities.Person
import io.renku.http.client.AccessToken.ProjectAccessToken
import io.renku.http.client.UserAccessToken

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

  def projectCommits(projectId: projects.GitLabId): StateQuery[List[CommitData]] =
    _.commits.get(projectId).map(_.toList).getOrElse(Nil)

  def findProjectAccessTokens(projectId: projects.GitLabId): StateQuery[List[ProjectAccessTokenInfo]] =
    _.projectAccessTokens.get(projectId).toList

  def commitsFor(projectId: projects.GitLabId, maybeAuthedReq: Option[AuthedReq]): StateQuery[List[CommitData]] =
    for {
      project <- findProjectById(projectId, maybeAuthedReq)
      commits <- project.traverse(p => projectCommits(p.id))
    } yield commits.getOrElse(Nil)

  def findCommit(projectId:      projects.GitLabId,
                 maybeAuthedReq: Option[AuthedReq],
                 sha:            CommitId
  ): StateQuery[Option[CommitData]] =
    commitsFor(projectId, maybeAuthedReq).andThen(_.find(_.commitId == sha))

  def findPushEvents(projectId: projects.GitLabId, maybeAuthedReq: Option[AuthedReq]): StateQuery[List[PushEvent]] =
    commitsFor(projectId, maybeAuthedReq).andThen(_.map(_.toPushEvent(projectId)))

  def findAuthedProject(token: ProjectAccessToken): StateQuery[Option[AuthedProject]] =
    _.projectAccessTokens.find(_._2.token == token).map { case (projectId, tokenInfo) =>
      AuthedProject(projectId, tokenInfo.token)
    }

  def findAuthedUser(token: UserAccessToken): StateQuery[Option[AuthedUser]] =
    _.users.find(_._2 == token).map(AuthedUser.tupled)

  def findPersonById(id: persons.GitLabId): StateQuery[Option[Person]] =
    _.persons.find(_.maybeGitLabId == id.some)

  def projectsFor(userId: Option[persons.GitLabId]): StateQuery[List[Project]] =
    _.projects.filter { p =>
      p.entitiesProject.visibility == projects.Visibility.Public ||
      userId.exists(p.members.map(_.gitLabId).contains_) ||
      p.maybeCreator.map(_.gitLabId) == userId
    }

  def projectsWhereUserIsMember(userId: persons.GitLabId): StateQuery[List[Project]] =
    _.projects.filter { p =>
      p.members.map(_.gitLabId).contains_(userId) || p.maybeCreator.forall(_.gitLabId == userId)
    }

  def findCallerProjects: Option[AuthedReq] => StateQuery[List[Project]] = {
    case None                        => _ => Nil
    case Some(AuthedProject(_, _))   => _ => Nil
    case Some(AuthedUser(userId, _)) => projectsWhereUserIsMember(userId)
  }

  def findProject(id: projects.GitLabId, user: Option[persons.GitLabId]): StateQuery[Option[Project]] =
    projectsFor(user).andThen(_.find(p => p.id == id))

  def findProject(slug: projects.Slug, user: Option[persons.GitLabId]): StateQuery[Option[Project]] =
    for {
      all <- projectsFor(user)
      project = all.find(p => p.slug == slug)
    } yield project

  def findProject(id: projects.GitLabId, slug: projects.Slug): StateQuery[Option[Project]] =
    _.projects.find(p => p.slug == slug && p.id == id)

  def findProjectById(id: projects.GitLabId): StateQuery[Option[Project]] =
    _.projects.find(_.id == id)

  def findProjectById(id: projects.GitLabId, maybeAuthedReq: Option[AuthedReq]): StateQuery[Option[Project]] =
    maybeAuthedReq match {
      case Some(AuthedProject(`id`, _)) => findProjectById(id)
      case Some(AuthedProject(_, _))    => _ => None
      case Some(AuthedUser(userId, _))  => findProject(id, userId.some)
      case None                         => findProject(id, None)
    }

  def findProjectBySlug(slug: projects.Slug): StateQuery[Option[Project]] =
    _.projects.find(_.slug == slug)

  def findProjectByPath(path: projects.Slug, maybeAuthedReq: Option[AuthedReq]): StateQuery[Option[Project]] =
    maybeAuthedReq match {
      case Some(AuthedProject(id, _))  => findProject(id, path)
      case Some(AuthedUser(userId, _)) => findProject(path, userId.some)
      case None                        => findProject(path, None)
    }

  def findWebhooks(projectId: projects.GitLabId): StateQuery[List[Webhook]] =
    _.webhooks.filter(_.projectId == projectId)

  def isProjectBroken(id: projects.GitLabId): StateQuery[Boolean] =
    _.brokenProjects.contains(id)

  def isProjectBroken(path: projects.Slug): StateQuery[Boolean] =
    for {
      project <- findProjectBySlug(path)
      broken  <- project.traverse(p => isProjectBroken(p.id))
    } yield broken.getOrElse(false)

  final implicit class CommitDataOps(self: CommitData) {
    def toPushEvent(projectId: projects.GitLabId): PushEvent =
      PushEvent(projectId, self.commitId, GraphModelGenerators.personGitLabIds.generateOne, self.author.name)
  }
}

object GitLabStateQueries extends GitLabStateQueries
