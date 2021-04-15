/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.db

import ch.datascience.db.DBConfigProvider.DBConfig
import ch.datascience.db.DBConfigProvider.DBConfig._
import ch.datascience.generators.Generators.{durations, nonEmptyStrings, positiveInts, relativePaths}
import eu.timepit.refined.api.RefType
import org.scalacheck.Gen

import scala.concurrent.duration._
import scala.language.postfixOps

private object DbConfigGenerator {

  val dbConfigs: Gen[DBConfig[TestDB]] = for {
    user           <- nonEmptyStrings() map (RefType.applyRef[User](_).getOrError)
    pass           <- nonEmptyStrings()
    name           <- nonEmptyStrings() map (RefType.applyRef[DbName](_).getOrError)
    host           <- nonEmptyStrings() map (RefType.applyRef[Host](_).getOrError)
    connectionPool <- positiveInts() map (_.value) map (RefType.applyRef[ConnectionPool](_).getOrError)
    maxLifetime    <- durations(max = 60 minutes)
  } yield DBConfig(host, name, user, pass, connectionPool, maxLifetime)

  implicit class RefinedOps[V](maybeValue: Either[String, V]) {
    lazy val getOrError: V = maybeValue.fold(s => throw new IllegalArgumentException(s), identity)
  }

  trait TestDB
}
