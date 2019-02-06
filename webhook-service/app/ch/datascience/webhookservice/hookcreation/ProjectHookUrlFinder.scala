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

import cats.MonadError
import cats.effect.IO
import cats.implicits._
import ch.datascience.tinytypes.{TinyType, TinyTypeFactory}
import ch.datascience.webhookservice.eventprocessing.routes.WebhookEventEndpoint
import ch.datascience.webhookservice.hookcreation.ProjectHookUrlFinder.ProjectHookUrl
import eu.timepit.refined.api.{RefType, Refined}
import io.circe.Decoder
import javax.inject.{Inject, Singleton}

import scala.language.higherKinds

private class ProjectHookUrlFinder[Interpretation[_]](
    selfUrlConfig: SelfUrlConfig[Interpretation]
)(implicit ME:     MonadError[Interpretation, Throwable]) {

  def findProjectHookUrl: Interpretation[ProjectHookUrl] =
    for {
      selfUrl        <- selfUrlConfig.get()
      projectHookUrl <- ME.fromEither(ProjectHookUrl.from(s"$selfUrl${WebhookEventEndpoint.processPushEvent().url}"))
    } yield projectHookUrl
}

private object ProjectHookUrlFinder {
  import eu.timepit.refined.string.Url

  class ProjectHookUrl private (val value: String) extends AnyVal with TinyType[String]
  object ProjectHookUrl extends TinyTypeFactory[String, ProjectHookUrl](new ProjectHookUrl(_)) {
    private type ProjectHookUrlValue = String Refined Url

    addConstraint(
      check = RefType
        .applyRef[ProjectHookUrlValue](_)
        .isRight,
      message = (value: String) => s"'$value' is not a valid $typeName"
    )

    implicit lazy val projectHookUrlDecoder: Decoder[ProjectHookUrl] = Decoder.decodeString.map(ProjectHookUrl.apply)
  }
}

@Singleton
private class IOProjectHookUrlFinder @Inject()(
    selfUrlConfig: IOSelfUrlConfigProvider
) extends ProjectHookUrlFinder[IO](selfUrlConfig)
