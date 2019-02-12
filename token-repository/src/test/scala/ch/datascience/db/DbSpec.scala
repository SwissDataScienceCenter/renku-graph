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

package ch.datascience.db

import cats.effect.IO
import doobie.util.transactor.Transactor.Aux
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, TestSuite}
import ch.datascience.generators.Generators._
import ch.datascience.generators.Generators.Implicits._

trait DbSpec extends BeforeAndAfterAll with BeforeAndAfter {
  self: TestSuite =>

  protected def dbName: String = nonEmptyStrings().map(suffix => s"db_$suffix").generateOne

  protected lazy val transactorProvider: TransactorProvider[IO] = new H2TransactorProvider(dbName, "user")
  protected lazy val transactor:         Aux[IO, Unit]          = transactorProvider.transactor

  protected def initDb(transactor: Aux[IO, Unit]): Unit

  protected def prepareDbForTest(transactor: Aux[IO, Unit]): Unit

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    initDb(transactor)
  }

  protected override def before(fun: => Any)(implicit pos: Position): Unit = {
    super.before(fun)
    prepareDbForTest(transactor)
  }
}
