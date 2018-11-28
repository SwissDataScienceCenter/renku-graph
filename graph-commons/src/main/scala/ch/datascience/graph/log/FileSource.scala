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
