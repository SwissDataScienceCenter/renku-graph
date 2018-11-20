package graphql

import persistence.DatabaseAccessLayer

/**
 * The context used to resolve graphql queries
 * @param dal DatabaseAccessLayer
 */
//TODO: Add user (client) context (e.g. user id), for access to restricted objects
case class UserContext(
    dal: DatabaseAccessLayer
)
