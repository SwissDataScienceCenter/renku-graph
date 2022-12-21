package io.renku.knowledgegraph.entities

import cats.syntax.all._
import org.scalatest.matchers.should
import org.scalatest.wordspec.AnyWordSpec

class CriteriaSpec extends AnyWordSpec with should.Matchers {

  "EntityType" should {

    Criteria.Filters.EntityType.all.map {
      case t @ Criteria.Filters.EntityType.Project  => "project"  -> t
      case t @ Criteria.Filters.EntityType.Dataset  => "dataset"  -> t
      case t @ Criteria.Filters.EntityType.Workflow => "workflow" -> t
      case t @ Criteria.Filters.EntityType.Person   => "person"   -> t
    } foreach { case (name, t) =>
      s"be instantiatable from '$name'" in {
        Criteria.Filters.EntityType.from(name) shouldBe t.asRight
      }
    }
  }
}
