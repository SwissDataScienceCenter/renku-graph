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

package ch.datascience.graph.log

import java.nio.file.{ FileSystems, Files, Path }

import akka.Done
import akka.stream.scaladsl.Source
import akka.stream.stage._
import akka.stream.{ Attributes, Outlet, SourceShape }
import akka.util.ByteString
import play.api.libs.json.{ JsValue, Json }

import scala.concurrent.{ Future, Promise }
import scala.util.{ Failure, Success, Try }

object FileSource {
  def apply( path: String ): Source[Try[JsValue], Future[Done]] = {
    val jPath = FileSystems.getDefault.getPath( path )
    FileSource( jPath )
  }

  def apply( path: Path ): Source[Try[JsValue], Future[Done]] = {
    Source
      .fromGraph( new FileSourceStage( path ) )
      .map( Try[ByteString]( _ ) )
      .map( _.map( _.toArray ) )
      .map( _.map( Json.parse ) )
  }

  class FileSourceStage(
      val path: Path
  ) extends GraphStageWithMaterializedValue[SourceShape[ByteString], Future[Done]] {
    val out: Outlet[ByteString] = Outlet( "FileSourceStage.out" )

    def shape: SourceShape[ByteString] = SourceShape( out )

    def createLogicAndMaterializedValue( inheritedAttributes: Attributes ): ( GraphStageLogic, Future[Done] ) = {
      val promise = Promise[Done]

      val logic: GraphStageLogic = new GraphStageLogic( shape ) with StageLogging {
        private val reader = Files.newBufferedReader( path )

        override def postStop(): Unit = {
          promise.complete( Try {
            reader.close()
            Done
          } )
        }

        setHandler( out, new OutHandler {
          def onPull(): Unit = {
            var line: String = null

            while ( line eq null ) {
              line = reader.readLine()
              if ( line eq null ) Thread.sleep( 100L )
            }

            push( out, ByteString( line ) )
          }
        } )

      }

      ( logic, promise.future )
    }
  }

}
