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

package io.renku.knowledgegraph.projects.create

import cats.syntax.all._
import io.renku.data.Message
import io.renku.graph.model.{persons, projects}
import io.renku.knowledgegraph.Failure
import org.http4s.Status.{BadRequest, Forbidden, InternalServerError}

private object CreationFailures {

  def badRequestOnGLCreate(message: Message): Failure =
    Failure(BadRequest, message)

  def forbiddenOnGLCreate(message: Message): Failure =
    Failure(Forbidden, message)

  def onFindingNamespace(namespaceId: NamespaceId, cause: Throwable): Failure =
    Failure(InternalServerError,
            Message.Error.unsafeApply(show"Finding namespace $namespaceId in GitLab failed.${toMessage(cause)}"),
            cause
    )

  def noNamespaceFound(namespace: Namespace): Failure =
    Failure(BadRequest, Message.Error.unsafeApply(show"No namespace with ${namespace.identifier} found"))

  def onFindingUserInfo(userId: persons.GitLabId, cause: Throwable): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Finding info about $userId failed"), cause)

  def noUserInfoFound(userId: persons.GitLabId): Failure =
    Failure(InternalServerError, Message.Error.unsafeApply(show"Cannot find info about user $userId"))

  def onGLCreation(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError,
            Message.Error.unsafeApply(show"Creating project $slug in GitLab failed.${toMessage(cause)}"),
            cause
    )

  def onCoreCreation(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError,
            Message.Error.unsafeApply(show"Creating project $slug in Core failed.${toMessage(cause)}"),
            cause
    )

  def onActivation(slug: projects.Slug, cause: Throwable): Failure =
    Failure(InternalServerError,
            Message.Error.unsafeApply(show"Activating project $slug for indexing failed.${toMessage(cause)}"),
            cause
    )

  def activationReturningNotFound(newProject: NewProject): Failure =
    Failure(InternalServerError,
            Message.Error.unsafeApply(show"Project ${newProject.slug} couldn't be activated as it did not exist")
    )

  private def toMessage(cause: Throwable): String =
    Option(cause)
      .flatMap(c => Option(c.getMessage))
      .fold(ifEmpty = "")(m => s" $m")
}
