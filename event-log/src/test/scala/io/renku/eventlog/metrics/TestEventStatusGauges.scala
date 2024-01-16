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

package io.renku.eventlog.metrics

import cats.MonadThrow
import cats.effect.Sync
import cats.syntax.all._
import io.renku.graph.model.projects
import io.renku.metrics.{LabeledGauge, TestLabeledGauge}

object TestEventStatusGauges {
  def apply[F[_]: Sync]: EventStatusGauges[F] = new EventStatusGaugesImpl[F](
    new TestLabeledGauge[F, projects.Slug]() with AwaitingGenerationGauge[F],
    new TestLabeledGauge[F, projects.Slug]() with UnderTriplesGenerationGauge[F],
    new TestLabeledGauge[F, projects.Slug]() with AwaitingTransformationGauge[F],
    new TestLabeledGauge[F, projects.Slug]() with UnderTransformationGauge[F],
    new TestLabeledGauge[F, projects.Slug]() with AwaitingDeletionGauge[F],
    new TestLabeledGauge[F, projects.Slug]() with UnderDeletionGauge[F]
  )

  implicit class TestGaugeOps[F[_]: MonadThrow](gauge: LabeledGauge[F, projects.Slug]) {

    def getValue(slug: projects.Slug): F[Double] = gauge match {
      case g: TestLabeledGauge[F, projects.Slug] => g.state.get.map(_.getOrElse(slug, 0d))
      case g => throw new Exception(s"Expected TestLabeledGauge but got ${g.getClass.getName}")
    }

    def getAllValues: F[Map[projects.Slug, Double]] = gauge match {
      case g: TestLabeledGauge[F, projects.Slug] => g.state.get
      case g => throw new Exception(s"Expected TestLabeledGauge but got ${g.getClass.getName}")
    }
  }
}
