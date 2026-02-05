locals {
    country = "IN"
    prefix = "apps"
    resource_name = "${local.country}-${local.prefix}-${var.environment}-${var.app_name}" 
    app_role_name = "role-${local.resource_name}"
}