package graphql

import javax.inject.{ Inject, Singleton }
import persistence.DatabaseAccessLayer

@Singleton
class UserContextProvider @Inject() (
    dal: DatabaseAccessLayer
) {
  def get: UserContext = UserContext( dal )
}
