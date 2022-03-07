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

import cats.syntax.all._
import io.prometheus.client.{Histogram => LibHistogram}
import io.renku.generators.Generators.Implicits._
import io.renku.generators.Generators._
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Success, Try}

class HistogramSpec extends AnyWordSpec with MockFactory with should.Matchers {

  "apply" should {

    "register the metrics in the Metrics Registry " +
      "and return an instance of the LabeledHistogram" in new TestCase {

        (metricsRegistry
          .register[LibHistogram, LibHistogram.Builder](_: LibHistogram.Builder))
          .expects(*)
          .onCall((builder: LibHistogram.Builder) => builder.create().pure[Try])

        val labelName = nonBlankStrings().generateOne

        val Success(histogram) = Histogram[Try, String](name, help, labelName, Seq(.1, 1))

        histogram.isInstanceOf[LabeledHistogram[Try, String]] shouldBe true
        histogram.name                                        shouldBe name.value
        histogram.help                                        shouldBe help.value
      }
  }

  private trait TestCase {
    val name = nonBlankStrings().generateOne
    val help = sentences().generateOne

    implicit val metricsRegistry: MetricsRegistry[Try] = mock[MetricsRegistry[Try]]
  }
}

class LabeledHistogramSpec extends AnyWordSpec with MockFactory with should.Matchers {

  import MetricsTools._
  import io.renku.graph.model.GraphModelGenerators._
  import io.renku.graph.model.projects.Path

  "startTimer -> observeDuration" should {

    "associate measured time with the label on the histogram" in new TestCase {

      // iteration 1
      val labelValue1    = projectPaths.generateOne
      val Success(timer) = histogram.startTimer(labelValue1)

      val sleepTime = 500
      Thread sleep sleepTime

      val Success(duration) = timer.observeDuration

      (duration * 1000) should be > sleepTime.toDouble

      underlying.collectAllSamples.map(_._1)             should contain(label)
      underlying.collectAllSamples.map(_._2)             should contain(labelValue1.toString)
      underlying.collectAllSamples.map(_._3).last * 1000 should be > sleepTime.toDouble
    }
  }

  private trait TestCase {
    val label        = nonBlankStrings().generateOne.value
    private val name = nonBlankStrings().generateOne
    private val help = sentences().generateOne
    val underlying   = LibHistogram.build(name.value, help.value).labelNames(label).create()

    val histogram = new LabeledHistogramImpl[Try, Path](underlying)
  }
}
