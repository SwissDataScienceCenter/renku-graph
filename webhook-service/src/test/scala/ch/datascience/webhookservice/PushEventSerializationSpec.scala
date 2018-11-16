package ch.datascience.webhookservice

import org.scalatest.Matchers._
import org.scalatest.WordSpec

class PushEventSerializationSpec extends WordSpec {

  import WebhookEndpoint.JsonSupport._
  import spray.json._

  "JSON deserializer for PushEvent" should {

    "be able to translate a valid JSON to a PushEvent object" in {
      pushEventJson.parseJson.convertTo[PushEvent] shouldBe PushEvent(
        CheckoutSha("da1560886d4f094c3e6c9ef40349f7d38b5d27d7"),
        GitRepositoryUrl("http://example.com/mike/diaspora.git"),
        ProjectName("Diaspora")
      )
    }
  }

  private val pushEventJson =
    """
      |{
      |  "object_kind": "push",
      |  "before": "95790bf891e76fee5e1747ab589903a6a1f80f22",
      |  "after": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
      |  "ref": "refs/heads/master",
      |  "checkout_sha": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
      |  "user_id": 4,
      |  "user_name": "John Smith",
      |  "user_username": "jsmith",
      |  "user_email": "john@example.com",
      |  "user_avatar": "https://s.gravatar.com/avatar/d4c74594d841139328695756648b6bd6?s=8://s.gravatar.com/avatar/d4c74594d841139328695756648b6bd6?s=80",
      |  "project_id": 15,
      |  "project":{
      |    "id": 15,
      |    "name":"Diaspora",
      |    "description":"",
      |    "web_url":"http://example.com/mike/diaspora",
      |    "avatar_url":null,
      |    "git_ssh_url":"git@example.com:mike/diaspora.git",
      |    "git_http_url":"http://example.com/mike/diaspora.git",
      |    "namespace":"Mike",
      |    "visibility_level":0,
      |    "path_with_namespace":"mike/diaspora",
      |    "default_branch":"master",
      |    "homepage":"http://example.com/mike/diaspora",
      |    "url":"git@example.com:mike/diaspora.git",
      |    "ssh_url":"git@example.com:mike/diaspora.git",
      |    "http_url":"http://example.com/mike/diaspora.git"
      |  },
      |  "repository":{
      |    "name": "Diaspora",
      |    "url": "git@example.com:mike/diaspora.git",
      |    "description": "",
      |    "homepage": "http://example.com/mike/diaspora",
      |    "git_http_url":"http://example.com/mike/diaspora.git",
      |    "git_ssh_url":"git@example.com:mike/diaspora.git",
      |    "visibility_level":0
      |  },
      |  "commits": [
      |    {
      |      "id": "b6568db1bc1dcd7f8b4d5a946b0b91f9dacd7327",
      |      "message": "Update Catalan translation to e38cb41.",
      |      "timestamp": "2011-12-12T14:27:31+02:00",
      |      "url": "http://example.com/mike/diaspora/commit/b6568db1bc1dcd7f8b4d5a946b0b91f9dacd7327",
      |      "author": {
      |        "name": "Jordi Mallach",
      |        "email": "jordi@softcatala.org"
      |      },
      |      "added": ["CHANGELOG"],
      |      "modified": ["app/controller/application.rb"],
      |      "removed": []
      |    },
      |    {
      |      "id": "da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
      |      "message": "fixed readme",
      |      "timestamp": "2012-01-03T23:36:29+02:00",
      |      "url": "http://example.com/mike/diaspora/commit/da1560886d4f094c3e6c9ef40349f7d38b5d27d7",
      |      "author": {
      |        "name": "GitLab dev user",
      |        "email": "gitlabdev@dv6700.(none)"
      |      },
      |      "added": ["CHANGELOG"],
      |      "modified": ["app/controller/application.rb"],
      |      "removed": []
      |    }
      |  ],
      |  "total_commits_count": 4
      |}
    """.stripMargin
}
