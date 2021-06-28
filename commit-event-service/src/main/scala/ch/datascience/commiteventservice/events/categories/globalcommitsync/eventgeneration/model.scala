package ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration

import ch.datascience.commiteventservice.events.categories.globalcommitsync.eventgeneration.ProjectCommitStats.{CommitCount, commitCountDecoder}
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects
import ch.datascience.tinytypes.constraints.PositiveInt
import ch.datascience.tinytypes.json.TinyTypeDecoders.intDecoder
import ch.datascience.tinytypes.{IntTinyType, TinyTypeFactory}
import io.circe.{Decoder, HCursor}

private[globalcommitsync] final case class CommitWithParents(id:        CommitId,
                                                             projectId: projects.Id,
                                                             parents:   List[CommitId]
)

private[globalcommitsync] final case class ProjectCommitStats(maybeLatestCommit: Option[CommitId],
                                                              commitCount:       CommitCount
)

private[globalcommitsync] object ProjectCommitStats {
  implicit val commitCountDecoder: Decoder[CommitCount] = (cursor: HCursor) =>
    cursor.downField("statistics").downField("commit_count").as[CommitCount]

//  implicit val latestCommitDecoder: Decoder[Option[CommitId]] = ???

  private[globalcommitsync] final class CommitCount private (val value: Int) extends AnyVal with IntTinyType
  private[globalcommitsync] implicit object CommitCount
      extends TinyTypeFactory[CommitCount](new CommitCount(_))
      with PositiveInt
}
