package ch.datascience.graph.log

import java.nio.file.{ FileSystems, OpenOption, Path, StandardOpenOption }

import akka.stream.IOResult
import akka.stream.scaladsl.{ FileIO, Flow, Keep, Sink }
import akka.util.ByteString
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
