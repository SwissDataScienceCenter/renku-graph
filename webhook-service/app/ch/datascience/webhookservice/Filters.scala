package ch.datascience.webhookservice

import ch.datascience.service.utils.AccessLoggingFilter
import javax.inject.Inject
import play.api.http.DefaultHttpFilters
import play.filters.cors.CORSFilter
import play.filters.headers.SecurityHeadersFilter
import play.filters.hosts.AllowedHostsFilter

class Filters @Inject() (
    allowedHostsFilter:    AllowedHostsFilter,
    corsFilter:            CORSFilter,
    securityHeadersFilter: SecurityHeadersFilter,
    accessLoggingFilter:   AccessLoggingFilter
) extends DefaultHttpFilters(
  allowedHostsFilter,
  corsFilter,
  securityHeadersFilter,
  accessLoggingFilter
)
