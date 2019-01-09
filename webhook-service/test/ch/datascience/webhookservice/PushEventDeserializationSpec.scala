/*
 * Copyright 2019 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice

import ch.datascience.graph.events._
import ch.datascience.webhookservice.queues.pushevent.PushEvent
import org.scalatest.Matchers._
import org.scalatest.WordSpec
import play.api.libs.json.Json

class PushEventDeserializationSpec extends WordSpec {

  import PushEventConsumer.pushEventReads

  "JSON deserializer for PushEvent" should {

    "be able to translate a valid JSON to a PushEvent object" in {
      Json.parse(pushEventJson).as[PushEvent] shouldBe PushEvent(
        before = CommitId("95790bf891e76fee5e1747ab589903a6a1f80f22"),
        after  = CommitId("da1560886d4f094c3e6c9ef40349f7d38b5d27d7"),
        pushUser = PushUser(
          UserId(4),
          Username("jsmith"),
          Email("john@example.com")
        ),
        project = Project(
          ProjectId(15),
          ProjectPath("mike/diaspora")
        )
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
