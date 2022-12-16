/*
 * Copyright 2022 Swiss Data Science Center (SDSC)
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

import cats.MonadThrow
import cats.effect.{Async, Ref, Spawn, Temporal}
import cats.syntax.all._
import eu.timepit.refined.api.Refined
import eu.timepit.refined.collection.NonEmpty
import io.prometheus.client.{Gauge => LibGauge}

import java.time.{Duration => JDuration, Instant}
import scala.concurrent.duration._

trait LabeledGauge[F[_], LabelValue] extends Gauge[F] {
  def set(labelValue:       (LabelValue, Double)): F[Unit]
  def update(labelValue:    (LabelValue, Double)): F[Unit]
  def increment(labelValue: LabelValue):           F[Unit]
  def decrement(labelValue: LabelValue):           F[Unit]
  def reset(): F[Unit]
  def clear(): F[Unit]
}

class PositiveValuesLabeledGauge[F[_]: Async, LabelValue](val name: String Refined NonEmpty,
                                                          val help:              String Refined NonEmpty,
                                                          labelName:             String Refined NonEmpty,
                                                          resetDataFetch:        () => F[Map[LabelValue, Double]],
                                                          zerosCheckingInterval: Duration = 15 seconds
) extends LabeledGauge[F, LabelValue]
    with PrometheusCollector {

  type Collector = LibGauge

  private[metrics] lazy val wrappedCollector: Collector =
    LibGauge
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)
      .create()

  private val zeroedValues = Ref.unsafe[F, List[(LabelValue, Instant)]](List.empty)

  def startZeroedValuesCleaning(after: Duration = 2 minutes): F[Unit] = Spawn[F].start {
    val gracePeriod = JDuration.ofMillis(after.toMillis)

    val removeOldZeros: List[(LabelValue, Instant)] => List[(LabelValue, Instant)] = l =>
      l.partition(_._2.plus(gracePeriod).isBefore(Instant.now())) match {
        case (toDelete, toLeave) =>
          toDelete.foreach { case (label, _) => wrappedCollector.remove(label.toString) }
          toLeave
      }

    Temporal[F]
      .delayBy(
        ().pure[F] >> zeroedValues.update(removeOldZeros),
        zerosCheckingInterval
      )
      .foreverM
  }.void

  override def set(labelValueAndValue: (LabelValue, Double)): F[Unit] = MonadThrow[F].catchNonFatal {
    val (labelValue, value) = labelValueAndValue
    val validatedValue      = if (value < 0) 0d else value

    wrappedCollector.labels(labelValue.toString).set(validatedValue)

    labelValue -> validatedValue
  } >>= updateZeroedValues

  override def update(labelValueAndValue: (LabelValue, Double)): F[Unit] = MonadThrow[F].catchNonFatal {
    val (labelValue, value) = labelValueAndValue
    val child               = wrappedCollector.labels(labelValue.toString)
    val newValue = child.get() + value match {
      case v if v < 0 => 0d
      case v          => v
    }
    child.set(newValue)
    labelValue -> newValue
  } >>= updateZeroedValues

  override def increment(labelValue: LabelValue): F[Unit] = MonadThrow[F].catchNonFatal {
    wrappedCollector.labels(labelValue.toString).inc()
  }

  override def decrement(labelValue: LabelValue): F[Unit] = MonadThrow[F].catchNonFatal {
    val child = wrappedCollector.labels(labelValue.toString)
    if (child.get() == 0) ()
    else child.dec()
    labelValue -> child.get()
  } >>= updateZeroedValues

  private lazy val updateZeroedValues: ((LabelValue, Double)) => F[Unit] = {
    case (label, 0d) =>
      zeroedValues.update {
        case l if l.exists(_._1 == label) => l
        case l                            => (label -> Instant.now()) :: l
      }
    case (label, _) =>
      zeroedValues.update {
        case l if l.exists(_._1 == label) => l.filterNot(_._1 == label)
        case l                            => l
      }
  }

  override def reset(): F[Unit] = for {
    newValues <- resetDataFetch()
    _         <- clear()
    _         <- MonadThrow[F].catchNonFatal(newValues foreach set)
  } yield ()

  override def clear(): F[Unit] = MonadThrow[F].catchNonFatal(wrappedCollector.clear())
}

class AllValuesLabeledGauge[F[_]: MonadThrow, LabelValue](val name: String Refined NonEmpty,
                                                          val help:       String Refined NonEmpty,
                                                          labelName:      String Refined NonEmpty,
                                                          resetDataFetch: () => F[Map[LabelValue, Double]]
) extends LabeledGauge[F, LabelValue]
    with PrometheusCollector {

  type Collector = LibGauge

  private[metrics] lazy val wrappedCollector: Collector =
    LibGauge
      .build()
      .name(name.value)
      .help(help.value)
      .labelNames(labelName.value)
      .create()

  override def set(labelValueAndValue: (LabelValue, Double)): F[Unit] = MonadThrow[F].catchNonFatal {
    val (labelValue, value) = labelValueAndValue
    wrappedCollector.labels(labelValue.toString).set(value)
  }

  override def update(labelValueAndValue: (LabelValue, Double)): F[Unit] = MonadThrow[F].catchNonFatal {
    val (labelValue, value) = labelValueAndValue
    val child               = wrappedCollector.labels(labelValue.toString)
    child.set {
      child.get() + value
    }
  }

  override def increment(labelValue: LabelValue): F[Unit] = MonadThrow[F].catchNonFatal {
    wrappedCollector.labels(labelValue.toString).inc()
  }

  override def decrement(labelValue: LabelValue): F[Unit] = MonadThrow[F].catchNonFatal {
    wrappedCollector.labels(labelValue.toString).dec()
  }

  override def reset(): F[Unit] = for {
    newValues <- resetDataFetch()
    _         <- clear()
    _         <- MonadThrow[F].catchNonFatal(newValues foreach set)
  } yield ()

  override def clear(): F[Unit] = MonadThrow[F].catchNonFatal(wrappedCollector.clear())
}
