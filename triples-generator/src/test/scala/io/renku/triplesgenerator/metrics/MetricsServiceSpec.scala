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

package io.renku.triplesgenerator.metrics

import cats.Eval
import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{Clock, IO, Ref, Temporal}
import cats.syntax.all._
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators.{ints, positiveInts}
import io.renku.interpreters.TestLogger
import io.renku.interpreters.TestLogger.Level.Error
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.duration._

class MetricsServiceSpec extends AsyncWordSpec with AsyncIOSpec with should.Matchers with BeforeAndAfterEach {

  "collectEvery" should {

    "keep collecting the metrics forever in the defined pace" in {

      val pace   = 500 millis
      val cycles = positiveInts(max = 5).generateOne.value

      val collected            = Ref.unsafe[IO, List[FiniteDuration]](List.empty)
      val notFailingCollection = Clock[IO].monotonic.flatMap(d => collected.update(d :: _))

      for {
        _ <- service(notFailingCollection).collectEvery(500 millis).start.void
        _ <- Temporal[IO].sleep(pace * cycles)
        _ <- collected.get.asserting(_.size shouldBe >=(cycles))
        res <- collected.get
                 .map(list => list.zip(list.tail).map { case (l, r) => l.toMillis - r.toMillis })
                 .asserting(_.forall(_ >= (pace.toMillis - 50)) shouldBe true)
      } yield res
    }

    "don't stop collecting in case of a failure in collect" in {

      val pace   = 500 millis
      val cycles = ints(min = 2, max = 5).generateOne

      val collected        = Ref.unsafe[IO, List[FiniteDuration]](List.empty)
      val currentTimestamp = Clock[IO].monotonic.flatMap(d => collected.update(d :: _))

      val exception         = new Exception("boom")
      val collectionResults = List(exception.raiseError[IO, Unit], currentTimestamp).iterator

      val failingCollection =
        IO.eval(
          Eval.always(
            collectionResults
              .nextOption()
              .getOrElse(currentTimestamp)
          )
        ).flatten

      for {
        _ <- service(failingCollection).collectEvery(500 millis).start.void
        _ <- Temporal[IO].sleep(pace * cycles)
        _ <- collected.get.asserting(_.isEmpty shouldBe false)
        _ <- collected.get
               .map(list => list.zip(list.tail).map { case (l, r) => l.toMillis - r.toMillis })
               .asserting(_.forall(_ >= (pace.toMillis - 50)) shouldBe true)
        res <- logger.loggedOnlyF(Error("An error during Metrics collection", exception))
      } yield res
    }
  }

  private implicit lazy val logger: TestLogger[IO] = TestLogger()
  private def service(collectProcess: IO[Unit]) = new MetricsService[IO] {
    override def collect: IO[Unit] = collectProcess
  }

  protected override def beforeEach() = {
    super.beforeEach()
    logger.reset()
  }
}
