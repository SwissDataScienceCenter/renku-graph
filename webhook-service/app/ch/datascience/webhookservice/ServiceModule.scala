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

package ch.datascience.webhookservice

import java.net.URL

import cats.effect.{ContextShift, IO}
import ch.datascience.config.ServiceUrl
import com.google.inject.AbstractModule
import com.google.inject.name.Names
import com.typesafe.config.Config
import javax.inject.{Inject, Singleton}
import play.api.{ConfigLoader, Configuration, Environment}

import scala.concurrent.ExecutionContext

class ServiceModule(
    environment:   Environment,
    configuration: Configuration
) extends AbstractModule {

  private implicit object ServiceUrlFinder extends ConfigLoader[ServiceUrl] {
    override def load(config: Config, path: String): ServiceUrl = ServiceUrl(config.getString(path))
  }
  override def configure(): Unit =
    bind(classOf[URL])
      .annotatedWith(Names.named("gitlabUrl"))
      .toInstance(configuration.get[ServiceUrl]("services.gitlab.url").value)
}

@Singleton
class IOContextShift @Inject()(executionContext: ExecutionContext) extends ContextShift[IO] {
  private val contextShift = IO.contextShift(executionContext)
  override def shift: IO[Unit] = contextShift.shift
  override def evalOn[A](ec: ExecutionContext)(fa: IO[A]): IO[A] = contextShift.evalOn(ec)(fa)
}
