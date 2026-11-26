# 1. AWS role trust. Use values from approved Databricks UC setup.
# A complete deployment also needs the matching S3 access policy,
# role self-assumption where required, and credential validation.
data "aws_iam_policy_document" "passrole_for_uc" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.unity_catalog_principal_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.storage_credential_external_id]
    }
  }
}

# 2. Define the IAM role that Databricks will assume
resource "aws_iam_role" "metastore_data_access" {
  name               = "cobalt-uc-access-role"
  assume_role_policy = data.aws_iam_policy_document.passrole_for_uc.json
}

# 3. Register the Storage Credential in Unity Catalog
# This maps the Databricks object to the physical AWS IAM role
resource "databricks_storage_credential" "cobalt_s3_cred" {
  name = "cobalt_s3_credential"
  aws_iam_role {
    role_arn = aws_iam_role.metastore_data_access.arn
  }
  comment = "Managed by Terraform: Credential for accessing factory data"
}

# 4. Define the External Location for the Bronze Volumes
# This defines the specific S3 path that the credential is allowed to access
resource "databricks_external_location" "factory_bronze_loc" {
  name            = "factory_bronze_location"
  url             = "s3://cobalt-enterprise-data/bronze/factory_sensors"
  credential_name = databricks_storage_credential.cobalt_s3_cred.id
  comment         = "Managed by Terraform: Bronze external location"
}

# 5. Create the Catalog and assign access
resource "databricks_catalog" "cobalt_prod" {
  name    = "cobalt_prod"
  comment = "Production catalog for CobaltWorks managed via IaC"
}

# 6. Grant catalog visibility, not table data access.
# Production Git approval must be enforced separately.
resource "databricks_grants" "catalog_grants" {
  catalog = databricks_catalog.cobalt_prod.name
  grant {
    principal  = "Data_Science_Group"
    privileges = ["USE_CATALOG"]
  }
}
