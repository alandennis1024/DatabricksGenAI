# Databricks notebook source

# MAGIC %md
# MAGIC # Chapter 13 — Data Ingestion and Refinement Deployment
# MAGIC
# MAGIC This notebook contains the DAB configurations for deploying data pipelines
# MAGIC that feed the data quality agent. These include Lakeflow Jobs, Declarative
# MAGIC Pipelines, and custom embedding generation notebooks.
# MAGIC
# MAGIC All of these assets are defined in DABs alongside models, endpoints, and apps
# MAGIC so the entire solution deploys as a single unit.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lakeflow Jobs — Scheduled Data Preparation
# MAGIC
# MAGIC This job extracts table metadata and statistics on a schedule, preparing
# MAGIC the context that the agent uses when generating rules.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     prepare_agent_context:
# MAGIC       name: "prepare-dq-agent-context"
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 6 * * ?"
# MAGIC         timezone_id: "UTC"
# MAGIC       job_clusters:
# MAGIC         - job_cluster_key: "etl_cluster"
# MAGIC           new_cluster:
# MAGIC             spark_version: "15.4.x-scala2.12"
# MAGIC             num_workers: 2
# MAGIC             node_type_id: "${var.node_type}"
# MAGIC       tasks:
# MAGIC         - task_key: "extract_metadata"
# MAGIC           job_cluster_key: "etl_cluster"
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/extract_table_metadata.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog}"
# MAGIC               schema: "${var.schema}"
# MAGIC ```
# MAGIC
# MAGIC **Note:** Job clusters are preferred over all-purpose clusters for cost
# MAGIC optimization — they are created when the job starts and terminated when
# MAGIC it finishes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lakeflow Declarative Pipelines — Document Processing
# MAGIC
# MAGIC A declarative pipeline processes raw rule documentation through the
# MAGIC medallion architecture: raw documents in bronze, cleaned and parsed rules
# MAGIC in silver, and structured, embedding-ready content in gold.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   pipelines:
# MAGIC     document_processing:
# MAGIC       name: "dq-document-processing"
# MAGIC       target: "${var.catalog}.${var.schema}"
# MAGIC       libraries:
# MAGIC         - notebook:
# MAGIC             path: ./pipelines/process_documents.py
# MAGIC       configuration:
# MAGIC         catalog: "${var.catalog}"
# MAGIC         schema: "${var.schema}"
# MAGIC       continuous: false
# MAGIC ```
# MAGIC
# MAGIC **Note:** The `continuous: false` setting means the pipeline is triggered
# MAGIC on demand rather than running continuously. For most generative AI
# MAGIC solutions, triggered mode is sufficient.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Custom Notebooks — Embedding Generation
# MAGIC
# MAGIC A custom notebook generates embeddings for the knowledge base that feeds
# MAGIC the vector index. It is included in the DAB and referenced by a job.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     generate_embeddings:
# MAGIC       name: "generate-dq-embeddings"
# MAGIC       tasks:
# MAGIC         - task_key: "chunk_and_embed"
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/generate_embeddings.py
# MAGIC             base_parameters:
# MAGIC               catalog: "${var.catalog}"
# MAGIC               schema: "${var.schema}"
# MAGIC               embedding_endpoint: "${var.embedding_endpoint}"
# MAGIC ```
# MAGIC
# MAGIC **Note:** For production workloads, prefer structured pipelines
# MAGIC (Declarative Pipelines or formal jobs) over ad-hoc notebook execution —
# MAGIC they provide better monitoring, retry logic, and dependency management.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Dependency Order
# MAGIC
# MAGIC When deploying the full solution, dependency ordering matters:
# MAGIC
# MAGIC 1. **Data pipelines** (metadata extraction, document processing) run first
# MAGIC 2. **Embedding generation** runs after documents are processed
# MAGIC 3. **Vector index sync** happens after embeddings are written
# MAGIC 4. **Agent deployment** happens after the index is ready
# MAGIC
# MAGIC DAB job dependencies and task ordering handle this sequencing.
