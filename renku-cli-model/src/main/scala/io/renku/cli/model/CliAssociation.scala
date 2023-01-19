package io.renku.cli.model

import cats.syntax.all._
import io.circe.DecodingFailure
import io.renku.cli.model.CliAssociation.AssociatedPlan
import io.renku.cli.model.Ontologies.Prov
import io.renku.graph.model.associations._
import io.renku.jsonld.syntax._
import io.renku.jsonld.{EntityTypes, JsonLD, JsonLDDecoder, JsonLDEncoder}

final case class CliAssociation(
    id:    ResourceId,
    agent: Option[CliAgent],
    plan:  AssociatedPlan
)

object CliAssociation {

  sealed trait AssociatedPlan {
    def fold[A](fa: CliPlan => A, fb: CliWorkflowFilePlan => A, fc: CliWorkflowFileCompositePlan => A): A
  }
  object AssociatedPlan {
    final case class Step(plan: CliPlan) extends AssociatedPlan {
      def fold[A](fa: CliPlan => A, fb: CliWorkflowFilePlan => A, fc: CliWorkflowFileCompositePlan => A): A = fa(plan)
    }

    final case class WorkflowFile(plan: CliWorkflowFilePlan) extends AssociatedPlan {
      def fold[A](fa: CliPlan => A, fb: CliWorkflowFilePlan => A, fc: CliWorkflowFileCompositePlan => A): A = fb(plan)
    }

    final case class WorkflowFileComposite(plan: CliWorkflowFileCompositePlan) extends AssociatedPlan {
      def fold[A](fa: CliPlan => A, fb: CliWorkflowFilePlan => A, fc: CliWorkflowFileCompositePlan => A): A = fc(plan)
    }

    def apply(plan: CliPlan):                      AssociatedPlan = Step(plan)
    def apply(plan: CliWorkflowFilePlan):          AssociatedPlan = WorkflowFile(plan)
    def apply(plan: CliWorkflowFileCompositePlan): AssociatedPlan = WorkflowFileComposite(plan)

    implicit val jsonLDDecoder: JsonLDDecoder[AssociatedPlan] = {
      val da = CliPlan.jsonLDDecoder.emap(p => AssociatedPlan(p).asRight)
      val db = CliWorkflowFilePlan.jsonLDDecoder.emap(p => AssociatedPlan(p).asRight)
      val dc = CliWorkflowFileCompositePlan.jsonLDDecoder.emap(p => AssociatedPlan(p).asRight)

      JsonLDDecoder.instance { cursor =>
        val currentTypes = cursor.getEntityTypes
        (currentTypes.map(CliPlan.matchingEntityTypes),
         currentTypes.map(CliWorkflowFilePlan.matchingEntityTypes),
         currentTypes.map(CliWorkflowFileCompositePlan.matchingEntityTypes)
        ).flatMapN {
          case (true, _, _) => da(cursor)
          case (_, true, _) => db(cursor)
          case (_, _, true) => dc(cursor)
          case _ => Left(DecodingFailure(s"Invalid entity types for decoding associated plan: $currentTypes", Nil))
        }
      }
    }

    implicit val jsonLDEncoder: JsonLDEncoder[AssociatedPlan] =
      JsonLDEncoder.instance(_.fold(_.asJsonLD, _.asJsonLD, _.asJsonLD))
  }

  private val entityTypes: EntityTypes = EntityTypes.of(Prov.Association)

  implicit val jsonLDDecoder: JsonLDDecoder[CliAssociation] =
    JsonLDDecoder.entity(entityTypes) { cursor =>
      for {
        resourceId <- cursor.downEntityId.as[ResourceId]
        plan       <- cursor.downField(Prov.hadPlan).as[AssociatedPlan]
        agent      <- cursor.downField(Prov.agent).as[Option[CliAgent]]
      } yield CliAssociation(resourceId, agent, plan)
    }

  implicit val jsonLDEncoder: FlatJsonLDEncoder[CliAssociation] =
    FlatJsonLDEncoder.unsafe { assoc =>
      JsonLD.entity(
        assoc.id.asEntityId,
        entityTypes,
        Prov.hadPlan -> assoc.plan.asJsonLD,
        Prov.agent   -> assoc.agent.asJsonLD
      )
    }
}
