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

package io.renku.knowledgegraph.projects.update

import ProvisioningStatusFinder.ProvisioningStatus.Unhealthy
import cats.syntax.all._
import eu.timepit.refined.auto._
import io.renku.data.Message
import io.renku.graph.model.{persons, projects}
import org.http4s.Status
import org.http4s.Status.{BadRequest, Conflict, Forbidden, InternalServerError}

private sealed trait Failure extends Exception {
  val status:  Status
  val message: Message
}

private object Failure {

  final case class Simple(status: Status, message: Message) extends Exception(message.show) with Failure
  final case class WithCause(status: Status, message: Message, cause: Throwable)
      extends Exception(message.show, cause)
      with Failure

  def apply(status: Status, message: Message): Failure =
    Failure.Simple(status, message)

  def apply(status: Status, message: Message, cause: Throwable): Failure =
    Failure.WithCause(status, message, cause)

  def badRequestOnGLUpdate(message: Message): Failure =
    Failure(BadRequest, message)

  def forbiddenOnGLUpdate(message: Message): Failure =
    Failure(Forbidden, message)

  def onGLUpdate(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Updating project $slug in GitLab failed"), cause)

  def onTGUpdatesFinding(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Finding TS updates for $slug failed"), cause)

  def onTSUpdate(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Updating project $slug in TS failed"), cause)

  val cannotPushToBranch: Failure =
    Failure(Conflict, Message.Error("Updating project not possible; the user cannot push to the default branch"))

  def onProvisioningNotHealthy(slug: projects.Slug, unhealthy: Unhealthy): Failure =
    Failure(
      Conflict,
      Message.Error.unsafeApply(
        show"Project $slug in unhealthy state: ${unhealthy.status}; Fix the project manually on contact administrator"
      )
    )

  def onProvisioningStatusCheck(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Check if project $slug in healthy state failed"), cause)

  def onBranchAccessCheck(slug: projects.Slug, userId: persons.GitLabId, cause: Throwable): Failure =
    Failure(InternalServerError,
            Message.Error.unsafeApply(show"Check if pushing to git for $slug and $userId failed"),
            cause
    )

  val cannotFindProjectGitUrl: Failure =
    Failure(InternalServerError, Message.Error("Cannot find project info"))

  def onFindingProjectGitUrl(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Finding project git url for $slug failed"), cause)

  def cannotFindUserInfo(userId: persons.GitLabId): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Cannot find info about user $userId"))

  def onFindingUserInfo(userId: persons.GitLabId, cause: Throwable): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Finding info about $userId failed"), cause)

  def onFindingCoreUri(cause: Throwable): Failure =
    Failure(Conflict, Message.Error.fromExceptionMessage(cause), cause)
}
