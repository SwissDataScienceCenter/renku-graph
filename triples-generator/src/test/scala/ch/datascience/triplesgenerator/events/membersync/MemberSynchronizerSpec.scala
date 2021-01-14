package ch.datascience.triplesgenerator.events.membersync

import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec
import ch.datascience.graph.model.GraphModelGenerators._
import ch.datascience.generators.Generators.Implicits._
import scala.util.Try
import cats.syntax.all._

class MemberSynchronizerSpec extends AnyWordSpec with MockFactory with should.Matchers {
  "synchronizeMembers" should {
    "pulls members from Gitlab" +
      "AND generates triples and sends them to triplestore" in new TestCase {

        synchronizer.synchronizeMembers(projectPath) shouldBe ().pure[Try]
      }

  }

  private trait TestCase {
    val projectPath  = projectPaths.generateOne
    val synchronizer = new MemberSynchronizerImpl[Try]()
  }

}
