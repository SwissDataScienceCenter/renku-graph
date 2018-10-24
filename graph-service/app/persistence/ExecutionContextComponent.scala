package persistence

import scala.concurrent.ExecutionContext

trait ExecutionContextComponent {
  def ec: ExecutionContext

  protected implicit lazy val _ec: ExecutionContext = ec
}
