# Chapter 14 reference: Databricks SQL alert Terraform.
# Provider, warehouse, notification destination required. Run terraform validate after configuring providers.

# Excerpt; requires a workspace provider and an existing SQL warehouse.
resource "databricks_query" "ai_errors" {
  warehouse_id = var.sql_warehouse_id
  display_name = "QA assistant errors (last hour)"
  query_text   = <<-SQL
    SELECT count(*) AS error_count
    FROM cobalt_prod.system_logs.qa_assistant_inference_payload
    WHERE event_time >= current_timestamp() - INTERVAL 1 HOUR
      AND status_code >= 400
  SQL
}

resource "databricks_alert" "ai_errors" {
  query_id     = databricks_query.ai_errors.id
  display_name = "QA assistant error threshold"
  condition {
    op = "GREATER_THAN"
    operand {
      column {
        name = "error_count"
      }
    }
    threshold {
      value {
        double_value = 50
      }
    }
  }
}
