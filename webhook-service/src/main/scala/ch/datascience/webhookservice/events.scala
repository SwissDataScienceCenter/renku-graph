package ch.datascience.webhookservice

import ch.datascience.tinytypes.StringValue
import ch.datascience.tinytypes.constraints.NonBlank

case class PushEvent(checkoutSha: CheckoutSha,
                     repositoryGitUrl: RepositoryGitUrl)

case class CheckoutSha(value: String) extends GitSha

case class RepositoryGitUrl(value: String) extends StringValue with NonBlank