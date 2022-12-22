package io.renku.entities.searchgraphs

private trait SearchInfoUploader[F[_]] {
  def upload(infos: List[SearchInfo]): F[Unit]
}
