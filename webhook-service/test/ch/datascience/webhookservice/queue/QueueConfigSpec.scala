/*
 * Copyright 2018 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.queue

import ch.datascience.generators.Generators.Implicits._
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import org.scalatest.prop.PropertyChecks
import play.api.Configuration

class QueueConfigSpec extends WordSpec with PropertyChecks {

  private val positiveInts = Gen.choose( 1, 1000 )

  "apply" should {

    "read 'queue.buffer-size', 'queue.triples-finder-threads' and 'queue.fuseki-upload-threads' to instantiate the QueueConfig" in {
      forAll( positiveInts, positiveInts, positiveInts ) { ( bufferSizeValue, triplesFinderThreadsValue, fusekiUploadThreads ) =>
        val config = Configuration.from(
          Map( "queue" -> Map(
            "buffer-size" -> bufferSizeValue,
            "triples-finder-threads" -> triplesFinderThreadsValue,
            "fuseki-upload-threads" -> fusekiUploadThreads
          ) )
        )

        val queueConfig = new QueueConfig( config )

        queueConfig.bufferSize shouldBe BufferSize( bufferSizeValue )
        queueConfig.triplesFinderThreads shouldBe TriplesFinderThreads( triplesFinderThreadsValue )
        queueConfig.fusekiUploadThreads shouldBe FusekiUploadThreads( fusekiUploadThreads )
      }
    }

    "throw an IllegalArgumentException if buffer-size is <= 0" in {
      val config = Configuration.from(
        Map( "queue" -> Map(
          "buffer-size" -> 0,
          "triples-finder-threads" -> positiveInts.generateOne,
          "fuseki-upload-threads" -> positiveInts.generateOne
        ) )
      )

      an[IllegalArgumentException] should be thrownBy new QueueConfig( config )
    }

    "throw an IllegalArgumentException if triples-finder-threads is <= 0" in {
      val config = Configuration.from(
        Map( "queue" -> Map(
          "buffer-size" -> positiveInts.generateOne,
          "triples-finder-threads" -> 0,
          "fuseki-upload-threads" -> positiveInts.generateOne
        ) )
      )

      an[IllegalArgumentException] should be thrownBy new QueueConfig( config )
    }

    "throw an IllegalArgumentException if fuseki-upload-threads is <= 0" in {
      val config = Configuration.from(
        Map( "queue" -> Map(
          "buffer-size" -> positiveInts.generateOne,
          "triples-finder-threads" -> positiveInts.generateOne,
          "fuseki-upload-threads" -> 0
        ) )
      )

      an[IllegalArgumentException] should be thrownBy new QueueConfig( config )
    }
  }
}
