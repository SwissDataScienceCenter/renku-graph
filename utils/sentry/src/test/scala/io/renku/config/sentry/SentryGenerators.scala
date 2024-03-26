/*
 * Copyright 2024 Swiss Data Science Center (SDSC)
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

package io.renku.config.sentry

import io.renku.config.ConfigGenerators
import io.renku.config.sentry.SentryConfig.{Dsn, Environment}
import io.renku.generators.Generators
import org.scalacheck.Gen

trait SentryGenerators {
  val sentryDsns: Gen[Dsn] = for {
    url         <- Generators.httpUrls()
    projectName <- Generators.nonEmptyList(Generators.nonEmptyStrings()).map(_.toList.mkString("."))
    projectId   <- Generators.positiveInts(max = 100)
  } yield Dsn(s"$url@$projectName/$projectId")

  val sentryEnvironments: Gen[Environment] = Generators.nonEmptyStrings() map Environment.apply

  val sentryConfigs: Gen[SentryConfig] = for {
    dsn            <- sentryDsns
    environment    <- sentryEnvironments
    serviceName    <- ConfigGenerators.serviceNames
    serviceVersion <- ConfigGenerators.serviceVersions
  } yield SentryConfig(dsn, environment, serviceName, serviceVersion)

}

object SentryGenerators extends SentryGenerators
