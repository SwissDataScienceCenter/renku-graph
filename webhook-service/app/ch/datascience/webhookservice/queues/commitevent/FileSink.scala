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

package ch.datascience.webhookservice.queues.commitevent

import java.nio.file._

import akka.stream.IOResult
import akka.stream.scaladsl.{ FileIO, Flow, Keep, Sink }
import akka.util.ByteString
import ch.datascience.graph.events.{ CommitEvent, Project, PushUser, User }
import javax.inject.{ Inject, Named, Provider }
import play.api.libs.json.{ Json, Writes }

import scala.concurrent.Future

object FileSink {
  def apply[T : Writes]( path: String ): Sink[T, Future[IOResult]] = {
    val jPath = FileSystems.getDefault.getPath( path )
    val openOptions: Set[OpenOption] = Set(
      StandardOpenOption.WRITE,
      StandardOpenOption.CREATE,
      StandardOpenOption.APPEND,
      StandardOpenOption.SYNC
    )

    FileSink( jPath, openOptions )
  }

  def apply[T : Writes]( path: Path, options: Set[OpenOption] ): Sink[T, Future[IOResult]] = {
    val flow = Flow.fromFunction( ( obj: T ) => toByteString( obj )( implicitly[Writes[T]] ) )
    val sink = FileIO.toPath( path, options )

    flow.toMat( sink )( Keep.right )
  }

  private[this] def toByteString[T : Writes]( obj: T ): ByteString = {
    val writer = implicitly[Writes[T]]
    val byteArray = Json.toBytes( writer.writes( obj ) )
    ByteString( byteArray ) ++ ByteString( "\n" )
  }
}

class FileEventLogSinkProvider @Inject() ( @Named( "event-log-file-path" ) eventLogFilePath:Path )
  extends EventLogSinkProvider {

  import FileEventLogSinkProvider._

  override def get: Sink[CommitEvent, Future[IOResult]] = {
    FileSink( eventLogFilePath.toAbsolutePath.toString )
  }
}

object FileEventLogSinkProvider {

  private implicit val userWrites: Writes[User] = Json.writes[User]
  private implicit val pushUserWrites: Writes[PushUser] = Json.writes[PushUser]
  private implicit val projectWrites: Writes[Project] = Json.writes[Project]
  implicit val commitEventWrites: Writes[CommitEvent] = Json.writes[CommitEvent]

}
