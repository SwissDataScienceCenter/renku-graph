package ch.datascience.triplesgenerator.events.categories.triplesgenerated.triplescuration.persondetails

import cats.effect.IO
import ch.datascience.graph.model.events.CommitId
import ch.datascience.graph.model.projects.Path
import ch.datascience.graph.model.users.{Email, Name}

private trait CommitCommitterFinder[Interpretation[_]] {

  def findCommitPeople(projectId: Path, commitId: CommitId): Set[CommitPerson]
}

private class CommitCommitterFinderImpl[Interpretation[_]] extends CommitCommitterFinder[Interpretation] {

  def findCommitPeople(projectId: Path, commitId: CommitId): Set[CommitPerson] =
    // if there are blank strings, None
    // if name or email isn't found, None
    ???

  """
    |{
    |  "id": "6104942438c14ec7bd21c6cd5bd995272b3faff6",
    |  "short_id": "6104942438c",
    |  "title": "Sanitize for network graph",
    |  "author_name": "randx",
    |  "author_email": "user@example.com",
    |  "committer_name": "Dmitriy",
    |  "committer_email": "user@example.com",
    |  "created_at": "2012-09-20T09:06:12+03:00",
    |  "message": "Sanitize for network graph",
    |  "committed_date": "2012-09-20T09:06:12+03:00",
    |  "authored_date": "2012-09-20T09:06:12+03:00",
    |  "parent_ids": [
    |    "ae1d9fb46aa2b07ee9836d49862ec4e2c46fbbba"
    |  ],
    |  "last_pipeline" : {
    |    "id": 8,
    |    "ref": "master",
    |    "sha": "2dc6aa325a317eda67812f05600bdf0fcdc70ab0",
    |    "status": "created"
    |  },
    |  "stats": {
    |    "additions": 15,
    |    "deletions": 10,
    |    "total": 25
    |  },
    |  "status": "running",
    |  "web_url": "https://gitlab.example.com/thedude/gitlab-foss/-/commit/6104942438c14ec7bd21c6cd5bd995272b3faff6"
    |}
    |""".stripMargin

}

private object IOCommitCommitterFinder {
  def apply(): CommitCommitterFinder[IO] = new CommitCommitterFinderImpl[IO]
}

case class CommitPerson(name: Name, email: Email)
