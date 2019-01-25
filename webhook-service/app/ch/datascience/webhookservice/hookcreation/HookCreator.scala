/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.hookcreation

import cats.effect._
import cats.implicits._
import cats.{Monad, MonadError}
import ch.datascience.clients.AccessToken
import ch.datascience.graph.events.ProjectId
import ch.datascience.logging.IOLogger
import ch.datascience.webhookservice.crypto.{HookTokenCrypto, IOHookTokenCrypto}
import io.chrisdavenport.log4cats.Logger
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds
import scala.util.control.NonFatal

private class HookCreator[Interpretation[_]: Monad](
    gitLabHookCreation: HookCreationRequestSender[Interpretation],
    logger:             Logger[Interpretation],
    hookTokenCrypto:    HookTokenCrypto[Interpretation]
)(implicit ME:          MonadError[Interpretation, Throwable]) {

  def createHook(projectId: ProjectId, accessToken: AccessToken): Interpretation[Unit] = {
    for {
      hookAuthToken <- hookTokenCrypto.encrypt(projectId.toString)
      _             <- gitLabHookCreation.createHook(projectId, accessToken, hookAuthToken)
      _             <- logger.info(s"Hook created for project with id $projectId")
    } yield ()
  } recoverWith {
    case NonFatal(exception) =>
      logger.error(exception)(s"Hook creation failed for project with id $projectId")
      ME.raiseError(exception)
  }
}

@Singleton
private class IOHookCreator @Inject()(
    gitLabHookCreation: IOHookCreationRequestSender,
    logger:             IOLogger,
    hookTokenCrypto:    IOHookTokenCrypto
) extends HookCreator[IO](gitLabHookCreation, logger, hookTokenCrypto)
