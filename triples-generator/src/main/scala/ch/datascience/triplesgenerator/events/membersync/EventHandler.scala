package ch.datascience.triplesgenerator.events.membersync

import cats.MonadError
import cats.effect.Effect
import ch.datascience.graph.model.events.CategoryName
import ch.datascience.triplesgenerator.events
import ch.datascience.triplesgenerator.events.EventSchedulingResult.{Accepted, UnsupportedEventType}
import io.chrisdavenport.log4cats.Logger

private[events] class EventHandler[Interpretation[_]: Effect](
    membersSynchronizer: MembersSynchronizer[Interpretation],
    logger:              Logger[Interpretation]
)(implicit ME:           MonadError[Interpretation, Throwable])
    extends events.EventHandler[Interpretation] {

  import ch.datascience.graph.model.projects
  import io.circe.Decoder
  import membersSynchronizer._
  import org.http4s._
  import org.http4s.circe._

  override val categoryName: CategoryName = EventHandler.categoryName

  override def handle(request: Request[Interpretation]) = {
    for {
      projectPath <- request.as[projects.Path].toRightT(recoverTo = UnsupportedEventType)
      result <- synchronizeMembers(projectPath).toRightT
                  .map(_ => Accepted)
                  .semiflatTap(logger.log(projectPath))
                  .leftSemiflatTap(logger.log(projectPath))
    } yield result
  }.merge

  private implicit lazy val eventInfoToString: projects.Path => String = { path =>
    s"projectPath = $path"
  }

  private implicit lazy val payloadDecoder: EntityDecoder[Interpretation, projects.Path] =
    jsonOf[Interpretation, projects.Path]

  private implicit lazy val eventDecoder: Decoder[projects.Path] = { implicit cursor =>
    import ch.datascience.tinytypes.json.TinyTypeDecoders._

    for {
      _           <- validateCategoryName
      projectPath <- cursor.downField("project").downField("path").as[projects.Path]
    } yield projectPath
  }
}

private[events] object EventHandler {
  val categoryName: CategoryName = CategoryName("MEMBER_SYNC")
}
