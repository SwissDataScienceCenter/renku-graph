package ch.datascience.triplesgenerator.events.membersync

import cats.MonadError
import cats.effect.{Concurrent, ContextShift}
import cats.syntax.all._
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.triplesgenerator.events
import ch.datascience.triplesgenerator.events.EventSchedulingResult.{Accepted, BadRequest, UnsupportedEventType}
import io.chrisdavenport.log4cats.Logger

private[events] class EventHandler[Interpretation[_]](
    membersSynchronizer: MemberSynchronizer[Interpretation],
    logger:              Logger[Interpretation]
)(implicit
    ME:           MonadError[Interpretation, Throwable],
    contextShift: ContextShift[Interpretation],
    concurrent:   Concurrent[Interpretation]
) extends events.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import io.circe.Decoder
  import membersSynchronizer._
  import org.http4s._
  import org.http4s.circe._

  override val categoryName: CategoryName = EventHandler.categoryName

  override def handle(request: Request[Interpretation]) = {
    for {
      _           <- request.as[CategoryName].toRightT(recoverTo = UnsupportedEventType)
      projectPath <- request.as[projects.Path].toRightT(recoverTo = BadRequest)
      result <- (contextShift.shift *> concurrent
                  .start(synchronizeMembers(projectPath))).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger.log(projectPath))
                  .leftSemiflatTap(logger.log(projectPath))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: projects.Path => String = { path =>
    s"projectPath = $path"
  }

  private implicit lazy val categoryDecoder: EntityDecoder[Interpretation, CategoryName] = {

    implicit lazy val categoryNameDecoder: Decoder[CategoryName] = { implicit cursor => validateCategoryName }

    jsonOf[Interpretation, CategoryName]
  }

  private implicit lazy val payloadDecoder: EntityDecoder[Interpretation, projects.Path] = {

    implicit lazy val eventDecoder: Decoder[projects.Path] = { implicit cursor =>
      import ch.datascience.tinytypes.json.TinyTypeDecoders._
      cursor.downField("project").downField("path").as[projects.Path](relativePathDecoder(projects.Path))
    }

    jsonOf[Interpretation, projects.Path]
  }

}

private[events] object EventHandler {
  val categoryName: CategoryName = CategoryName("MEMBER_SYNC")
}
