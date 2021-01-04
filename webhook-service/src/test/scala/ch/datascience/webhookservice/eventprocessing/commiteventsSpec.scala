/*
 * Copyright 2021 Swiss Data Science Center (SDSC)
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

package ch.datascience.webhookservice.eventprocessing

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.graph.model.events.{CompoundEventId, EventId}
import ch.datascience.graph.model.users.Email
import ch.datascience.webhookservice.generators.WebhookServiceGenerators._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class CommitEventSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "compoundEventId" should {

    "return CommitEventId comprised of id and project.id" in {
      forAll { commitEvent: CommitEvent =>
        commitEvent.compoundEventId shouldBe CompoundEventId(EventId(commitEvent.id.value), commitEvent.project.id)
      }
    }
  }
}

class AuthorSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "withEmail" should {

    "instantiate a new Author with username extracted from the email" in {
      forAll { email: Email =>
        Author.withEmail(email) shouldBe Author(email.extractName, email)
      }
    }
  }
}

class CommitterSpec extends AnyWordSpec with ScalaCheckPropertyChecks with should.Matchers {

  "withEmail" should {

    "instantiate a new Committer with username extracted from the email" in {
      forAll { email: Email =>
        Committer.withEmail(email) shouldBe Committer(email.extractName, email)
      }
    }
  }
}
