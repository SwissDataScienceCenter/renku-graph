package io.renku.knowledgegraph.datasets

import cats.effect.IO
import io.renku.graph.http.server.security.Authorizer
import io.renku.graph.model.{GitLabApiUrl, RenkuUrl, testentities}
import io.renku.interpreters.TestLogger
import io.renku.knowledgegraph.DatasetProvision
import io.renku.logging.TestSparqlQueryTimeRecorder
import io.renku.testtools.CustomAsyncIOSpec
import io.renku.triplesstore.{InMemoryJenaForSpec, ProjectsDataset, SparqlQueryTimeRecorder}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should
import org.typelevel.log4cats.Logger

abstract class SecurityRecordFinderSupport
    extends AsyncFlatSpec
    with CustomAsyncIOSpec
    with should.Matchers
    with InMemoryJenaForSpec
    // with ExternalJenaForSpec
    with ProjectsDataset
    with DatasetProvision {

  implicit val renkuUrl:  RenkuUrl                    = RenkuUrl("http://u.rl")
  implicit val gitlabUrl: GitLabApiUrl                = GitLabApiUrl("http://gitl.ab")
  implicit val ioLogger:  Logger[IO]                  = TestLogger()
  implicit val sqtr:      SparqlQueryTimeRecorder[IO] = TestSparqlQueryTimeRecorder.createUnsafe

  def toSecRecord(p: testentities.Project) =
    Authorizer.SecurityRecord(p.visibility, p.slug, p.members.flatMap(_.maybeGitLabId))
}
