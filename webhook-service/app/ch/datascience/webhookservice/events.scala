package ch.datascience.webhookservice

import ch.datascience.tinytypes.StringValue
import ch.datascience.tinytypes.constraints.NonBlank

case class PushEvent(
    checkoutSha:      CheckoutSha,
    gitRepositoryUrl: GitRepositoryUrl,
    projectName:      ProjectName
)

case class CheckoutSha( value: String ) extends GitSha

case class GitRepositoryUrl( value: String ) extends StringValue with NonBlank

case class ProjectName( value: String ) extends StringValue with NonBlank
