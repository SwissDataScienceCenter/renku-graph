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

import ch.datascience.tinytypes.TinyType
import com.typesafe.config.Config
import javax.inject.{ Inject, Singleton }
import play.api.{ ConfigLoader, Configuration }

private case class BufferSize( value: Int ) extends TinyType[Int] {
  verify( value > 0, s"'$value' is not > 0" )
}

private object BufferSize {

  implicit object BufferSizeFinder extends ConfigLoader[BufferSize] {
    override def load( config: Config, path: String ): BufferSize = BufferSize( config.getInt( path ) )
  }
}

private case class TriplesFinderThreads( value: Int ) extends TinyType[Int] {
  verify( value > 0, s"'$value' is not > 0" )
}

private object TriplesFinderThreads {

  implicit object TriplesFinderThreadsFinder extends ConfigLoader[TriplesFinderThreads] {
    override def load( config: Config, path: String ): TriplesFinderThreads = TriplesFinderThreads( config.getInt( path ) )
  }
}

private case class FusekiUploadThreads( value: Int ) extends TinyType[Int] {
  verify( value > 0, s"'$value' is not > 0" )
}

private object FusekiUploadThreads {

  implicit object FusekiUploadThreadsFinder extends ConfigLoader[FusekiUploadThreads] {
    override def load( config: Config, path: String ): FusekiUploadThreads = FusekiUploadThreads( config.getInt( path ) )
  }
}

@Singleton
private case class QueueConfig(
    bufferSize:           BufferSize,
    triplesFinderThreads: TriplesFinderThreads,
    fusekiUploadThreads:  FusekiUploadThreads
) {

  @Inject() def this( configuration: Configuration ) = this(
    configuration.get[BufferSize]( "queue.buffer-size" ),
    configuration.get[TriplesFinderThreads]( "queue.triples-finder-threads" ),
    configuration.get[FusekiUploadThreads]( "queue.fuseki-upload-threads" )
  )

}
