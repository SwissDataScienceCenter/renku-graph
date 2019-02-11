package ch.datascience.tokenrepository.repository

import ch.datascience.generators.Generators.Implicits._
import ch.datascience.generators.Generators._
import ch.datascience.graph.events.EventsGenerators._
import ch.datascience.graph.events.ProjectId
import doobie.implicits._
import org.scalatest.Matchers._
import org.scalatest.WordSpec

class TokenInRepoFinderSpec extends WordSpec {

  "findToken" should {

    "return token associated with the projectId - Personal Access Token case" in new TestCase {

      val encryptedToken = nonEmptyStrings().generateOne
      val tokenType      = TokenType.Personal

      insert(projectId, encryptedToken, tokenType.value)

      finder.findToken(projectId).value.unsafeRunSync shouldBe Some(encryptedToken -> tokenType)
    }

    "return token associated with the projectId - OAuth Access Token case" in new TestCase {

      val encryptedToken = nonEmptyStrings().generateOne
      val tokenType      = TokenType.OAuth

      insert(projectId, encryptedToken, tokenType.value)

      finder.findToken(projectId).value.unsafeRunSync shouldBe Some(encryptedToken -> tokenType)
    }

    "return None if there's no token associated with the projectId" in new TestCase {
      finder.findToken(projectId).value.unsafeRunSync shouldBe None
    }

    "fail with an error if the token associated with the projectId is of unknown type" in new TestCase {
      val encryptedToken = nonEmptyStrings().generateOne

      insert(projectId, encryptedToken, "UNKNOWN_TYPE")

      val result = finder.findToken(projectId)

      intercept[RuntimeException] {
        result.value.unsafeRunSync
      }.getMessage shouldBe s"Unknown token type: UNKNOWN_TYPE for projectId: $projectId"
    }
  }

  private trait TestCase {
    val projectId = projectIds.generateOne

    private val transactorProvider = IODBTransactorProvider
    import transactorProvider.transactor

    val finder = new TokenInRepoFinder(transactorProvider)

    def insert(projectId: ProjectId, accessToken: String, tokenType: String): Unit =
      sql"""insert into 
          projects_tokens (project_id, token, token_type) 
          values (${projectId.value}, $accessToken, $tokenType)
      """.update.run
        .map(assureInserted)
        .transact(transactor)
        .unsafeRunSync()

    private lazy val assureInserted: Int => Unit = {
      case 1 => ()
      case _ => fail("insertion problem")
    }
  }
}
