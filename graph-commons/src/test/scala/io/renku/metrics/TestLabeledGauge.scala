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

package io.renku.metrics
import cats.effect.{Ref, Sync}
import eu.timepit.refined.api.Refined
import eu.timepit.refined.auto._
import eu.timepit.refined.collection.NonEmpty

class TestLabeledGauge[F[_]: Sync, LabelValue](override val name: String Refined NonEmpty = "name",
                                               override val help: String Refined NonEmpty = "help"
) extends LabeledGauge[F, LabelValue] {

  val state: Ref[F, Map[LabelValue, Double]] = Ref.unsafe(Map.empty[LabelValue, Double])

  override def set(labelValue: (LabelValue, Double)): F[Unit] = state.update(m => m + labelValue)

  override def update(labelValue: (LabelValue, Double)): F[Unit] = state.update { m =>
    val vUpdated = m.get(labelValue._1).map(_ + labelValue._2).getOrElse(labelValue._2)
    m + (labelValue._1 -> vUpdated)
  }

  override def increment(labelValue: LabelValue): F[Unit] = update(labelValue -> 1d)

  override def decrement(labelValue: LabelValue): F[Unit] = update(labelValue -> -1d)

  override def reset(): F[Unit] = state.set(Map.empty)

  override def clear(): F[Unit] = state.set(Map.empty)
}
