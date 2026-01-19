is a copy of the file at "C:\Users\alana\Writing\GenAIBook\generative-ai-solutions-with-databricks\ch13.asciidoc"

[[GenAIDBRX_chapter_id_13]]

// Alan
// From Contract
// ----
// Chapter 13: Assembling and Deploying 
// o Importance of automated deployment 
// o Environments, data platforms and the art of building AI systems without violating data 
// policies, nor deploying things that work only in non-production environments 
// o Deploying models, prompts, data, indexes, ingestion pipelines, use-case logic and hundred 
// other interwoven and time-dependant concerns 
//    Databricks model hosting 
//    MLflow 
//    Databricks Asset Bundles 
// DB Apps 
// DevOps Pipelines and GitHub Actions 
// ----

== Assembling and Deploying
Creating a solution is only part of the process.
We must package up that solution and deploy it to high-level environments.
This chapter discusses the importance of automated development.
We dig into environments and data platforms,
and the art of building AI systems without violating data policies
or deploying things that only work in non-production environment.
We will discuss models, prompts, data, indexes, and all things related to deploying generative and agentic solutions.
We will close with a discussion of deploying Databricks Apps and automated deployment using GitHub actions and DevOps pipelines.

=== Importance of Automate Deployment
Since the advent and adoption of DevOps, iterative software development and deployment have become the standard.
Developing agentic and generative solutions follows an iterative pattern as well.
Generally, getting a working solution and then improving it over time.
To make those improvements,
we need a means to measure the effectiveness of a given iteration and an easy and reliable way of deployment.

Long ago, before DevOps taught us to use scripts for most things, we used to deploy solutions by hand.
That means someone would use a mouse and keyboard to install a version of code in an environment.
The well-prepared used a list to keep track of the things that needed to be deployed
and would mark them off as they were completed.
The problems with this approach range from incomplete lists,
to people skipping steps, to having environmentally specific settings in source code.
Quickly, we figured out that to have reliable deployments, we needed people doing as few things as possible.
That desire drove the adoption of automation.

Stating the obvious, when dealing with complex systems, automation is essential.
These systems contain many elements,
performing a range of operations including infrastructure provisioning and configuration, data cleansing, and model training. 
This level of complexity requires new tools to manage the deployment process. 
Fortunately, Databricks has been busily building tools to help with this process.

When we talk about automating deployments, we are referring to the process of copying code and other assets from a source code repository to a target environment. 
The code and resources in the source code repository are considered the source of truth. 
The environments they target serve different purposes, which we will discuss in the next section.


=== Getting Things Deployed to Production Without Violating Data Policies
Packaging up and deploying code may not sound exciting, but it is essential and also the most common point of failure in delivering value from AI systems.
To understand this process, we need to understand what an environment is and what purpose it serves.
We can also learn from common pitfalls that lead to solutions that only work in non-production environments.
Lastly, it is important to know what to promote, not just how to promote it.

==== Environments
Data platforms often have multiple environments to support the software development lifecycle.
These environments typically include development, staging/testing, and production.
There are variations on this theme, such as having only a production and non-production environemnt, or having multiple non-production environments for different purposes, such as QA, UAT, and pre-production.
We will explore the three commonly used environments.

When we talk about environments, it is important to understand the purpose of each one.
Development is where we build and test new features, with greater freedom to enable rapid iteration.
Once a feature is built and tested in development, we promote it to a staging/testing environment.
Staging should be as similar to production as possible in configuration and scale.
The purpose of staging is to validate that the feature functions as expected and that we can successfully deploy those changes to production.

We realize the value of our Databricks Data Intelligence Platform solutions in production.
As a result, production environments are more tightly controlled.
A failure in production can have a significant impact on the business, so we apply more stringent requirements for change management, monitoring, and alerting, as shown in <<ch13_environments_and_their_purpose_table>>.


// [cols="1,2,2",options="header"]
[[ch13_environments_and_their_purpose_table]]
.Environment Purpose Table
[cols="1,1,1,1", options="header"] 
|===
| | Development | Staging | Production 
| Purpose | Test and iterate code and models | Validate features before release and validate the promotion process | Serve real-time workloads generating business value
| Level of Control | Moderate (full access for developers) | High (limited access for QA) | Strict (read-only for most users)
| Source for Data | Read-only from Production catalog | Read-only from Production catalog | Production catalog
| Destination for Data | Development catalog | Staging catalog | Production catalog
| Git Integration | Active development branch (feature branches) | Release candidate branch | Main branch (tagged releases)
| Personas | Data Scientists, ML Engineers | QA Engineers, DevOps | Business Analysts, End Users
| Typical Workloads | Unit tests, experimentation | Integration tests, performance tests | Inference, dashboards, APIs 
|===

While this material is not new, it is certainly worth a refresher. 
Often, organizations confuse the environmental requirements for data engineering and for generative and agentic solution engineering.
In data engineering, the purpose is to modify data pipelines to deliver data to end users.
In generative and agentic solution engineering, the purpose is to deliver code and models that generate value from data.
This distinction is important because data policies often restrict the use of production data, based on the requirements for data engineering activities.
When building generative and agentic solutions, we often need to use production data to train and validate models.

The lack of representative data in lower-level environments is one of the reasons that solutions only work in non-production environments. In the next section, we explore other common failure points that lead to this problem.

==== Why Some Solutions Only Work in Non-Production Environments
Iterative development is essential when building generative and agentic solutions.
To effectively iterate, we need to be able to evaluate a solution and be able to quickly deploy changes.
When these two capabilities are missing or limited, we often find that solutions only work in non-production environments.
There are several broad areas that can lead to this problem as shown in <<ch13_failure_points_in_ml_ai_deployment_on_databricks>>.


[[ch13_failure_points_in_ml_ai_deployment_on_databricks]]
.Failure Points in ML/AI Deployment on Databricks
[cols="1,3", options="header"] 
|=== 
| Failure Point | Description
| Lack of Automation  | Manual deployment processes lead to human error, inconsistencies, scalability issues, and delays in getting models to production.
| Data Control and Drift | Inadequate data governance and lack of drift detection mechanisms result in degraded model performance and compliance risks. 
| Evaluation and Monitoring | Manual and inconsistent evaluation methods prevent scalable, production-grade confidence. Lack of observability tools leads to undetected model failures.
| Quality and Reliability | Generative models often produce unpredictable or inaccurate outputs, making them unsuitable for production without rigorous validation.
| MLOps Infrastructure | Manual or Missing CI/CD pipelines, automated testing, reproducible environments, and dependency management hinder operationalization.
| Team and Governance Misalignment | Disconnected roles and unclear ownership across AI and data science development, deployment engineering, and operations teams lead to deployment friction.
| Cost and Scalability | High compute costs and latency issues during inference make production deployment financially and technically challenging.
|===

One way we can address data-related issues is to ensure there is clear differentiation between catalogs and environments.
We sometimes commingle the two concepts, referring to the development environment, when we really mean a development workspace with read-only access to production data catalogs and write access to development catalogs.
The concept of a lower-level environment reading production data will cause many organizations pause.
Mainly because they are used to creating applications which read data or performing data engineering tasks.
When we are building generative and agentic solutions, we need production data in lower-level environments for training and testing.
This is one of the primary reasons that we see that causes solutions to only work in non-production environments.

=== Promote Code or Models
Related to the idea of reading production data in lower-level environments, is the question of what we will promote between environments.
The two approaches are: promoting trained models or promoting the code to perform the training of those models in each environment.
Databricks, and the authors of this book, take an opinionated stance on the topic. 
We recommend using production data in non-production environments and promoting code, not trained models. 
By taking this approach, we are developing the code to transform data and to train models in the development environment.
We use the staging/testing environment to validate that the code works, and more importantly to demonstrate we can successfully promote a model across environments.

=== Use-Case Logic: Where the Product Lives
As we talk about promoting code, we should talk about what that code does.
The code in a generative and agentic solution can be thought of in serveral ways.
One way is that the code is describing how to transforming and move data, making it usable by the models.
Another way to think about the code is that the code may be used to train models.
Lastly, some of the code is related to the use-case specific logic, required by the solution.

When we build solutions, we are trying to address a specific problem.
We define the justification for the solution often as a use case.
The use case specific logic we include is to address that problem.
While all elements of a solution are important, the use-case specific exists only to address a specific problem.

This logic is often implemented in noteboook or scripts, and scheduled as jobs.
We may expose this logic through an API, hosted in a Databricks App, or some other user interface.
Regardless of how we expose the use-case specific logic, it is important to understand that this is where the product lives.
While models, prompts, and data are all important ingredients to the solution, the heart of the solution is the use-case specific logic. 
Now that we understand what we are promoting, and where the product lives, let's look at the tools we can use to package and deploy our solutions.

=== Packaging and Deployment Tools
To create an AI solution, we often need to combine models, prompts, data, vector indexes, ingestion and refinement pipelines, use-case logic, and often a user interface.
Because of this complexity, we need tools to help us package up and deploy these solutions.
That is where Databricks' https://docs.databricks.com/aws/en/reference/api[REST API], https://docs.databricks.com/aws/en/dev-tools/cli[CLI], and https://docs.databricks.com/aws/en/dev-tools/bundles[Asset Bundles (DABs)] with https://github.com/databricks/mlops-stacks[MLOps Stacks] come in, see <<ch13_databricks_deployment_stack>>.

[[ch13_databricks_deployment_stack]]
.Databricks Deployment
image::images/ch13/Databricks Deployment Stack-larger.png["Databricks Deployment"]

As you can see, there are several elements to the Databricks deployment stack. Since a Git repository is the source of truth for code and assets, we start there. 

=== Automated CI/CD
The way we interact with the repository depends on organizational preferences, and the of Git provider.
If using GitHub, then we rely on GitHub Actions to automate deployments.
If using Azure DevOps, then we rely on DevOps Pipelines to automate deployments.
Lastly, you may be using a different Git provider, and would rely on their CI/CD capabilities.

The main purpose of the CI/CD automation layer is to provide a place for the Databricks CLI to execute. 
The Databricks CLI provides a simple way to interact with the Databricks REST API.
It is also how we deploy Databricks Asset Bundles (DABs) and use MLOps Stacks.

There are several ways we can interact with Git repositories to get code and assets into Databricks. 
Git repositories are the source of truth for code and assets.
They are essential for a successful project.
We will discuss three of the most common methods.

==== Git Folders
Databricks has supported Git integration for some time.
Originally, they required users to clone repositories into their workspace under a specific folder.
While this method is still supported, you can also create folders in any location that are linked to a Git repository, as shown in <<ch13_create_git_folder>>.

[[ch13_create_git_folder]]
.Creating a Git Folder
image::images/ch13/create git folder.png["Creating a Git Folder"]

This functionality brings a great deal of flexibility in how we organize our workspaces.
Once you click the "Create" button, you will be prompted to provide details about the Git repository.
You can the specify the repository URL, branch, and folder name.
This allows you to create multiple Git folders in a single workspace, each pointing to different repositories or branches.

The challenge with using Git folders is they are tied to a specific workspace.
If you need to deploy the same code to multiple workspaces, you will need to create Git folders in each workspace.
A better approach is to use the Databricks CLI, the REST API, or Databricks Asset Bundles (DABs).
The way to automate this process is to use DevOps Pipelines or GitHub Actions.

==== DevOps Pipelines and GitHub Actions
DevOps pipelines automate the entire https://docs.databricks.com/aws/en/dev-tools/ci-cd/azure-devops[Databricks Data Intelligence Platform deployment workflow], from code commit to production release. 
When you commit code to your main branch, the pipeline will trigger automatically, 
executing a series of steps to build, validate, and deploy your solution. 
The pipeline uses can use Databricks Asset Bundles, REST API, and CLI to interact with your Databricks workspaces, 
making it the essential link between your Git repository and Databricks.

A typical GitHub Actions or DevOps Pipeline workflow for Databricks-based generative and agentic solutions follows this pattern: 
first, the pipeline first checks out your code from a Git repository; 
next, it packages your assets using Databricks Asset Bundles (DABs), which define notebooks, jobs, configuration, and other resources as a deployable unit; 
then it then validates the bundle to catch configuration errors early; 
and finally, it deploys the bundle into the target Databricks workspace using the Databricks CLI. 
This automation removes manual deployment steps and ensures consistent, repeatable promotion across environments.

For example, consider a typical deployment scenario. A data engineer commits changes to a notebook and a job configuration to the main branch. The pipeline detects the commit and immediately starts. It builds a DAB containing the updated notebook and job definition, validates the syntax and structure, and then uses the Databricks CLI to deploy the bundle to the production workspace. The job configuration updates automatically, and the next scheduled run uses the new notebook code. This entire process happens without manual intervention, reducing errors and enabling rapid iteration from development through production.

Often the choice between using DevOps Pipelines or GitHub Actions comes down to organizational preference and existing infrastructure. 
Usually, an organization already has a preferred or even mandated CI/CD tool.
As organizations strive for standardization, it is common to see a single tool used across multiple teams and projects.
That said, both tools are capable of automating Databricks deployments effectively.

We will provide a hands on example of using GitHub Actions to deploy a Databricks Asset Bundle in the section <<ch13_walkthrough_example>> at the end of this chapter.


Before we discuss the process for deciding what deployment tool to use, let's look at the main technologies Databricks supplies to simplify packaging and deploying complex solutions.

==== Databricks Asset Bundles 
Packing and promoting complex solutions with interdependent elements is challenging.
Databricks created Asset Bundles, commonly referred to as DABs, to address this complexity. 
DABs are generally accessed through the Databricks CLI.
While you cannot currently use DABs through the REST API,
you could perform the same actions that a DAB performs by calling the appropriate REST API endpoints in the correct order.

A DAB is a folder with a specific structure that includes one or more YAML Ain't Markup Language (YAML) files to define the assets to be included in the bundle.
https://yaml.org/[YAML] is a human-readable data serialization standard that is commonly used for configuration files.
They are typically created using the Databricks CLI, which provides commands to create, validate, and deploy DABs.
DABs are often based on templates that provide a starting point for common use cases.
For example, there are https://docs.databricks.com/aws/en/dev-tools/bundles/templates[templates] for deploying machine learning models, data pipelines, and dashboards. We will provide a complete walkthrough example of creating and deploying a DAB in the section <<ch13_walkthrough_example>> at the end of this chapter.

===== MLOps Stacks
https://github.com/databricks/mlops-stacks[MLOps Stacks] brings standardization and best practices to ML projects.
It removes the need to manually configure and connect Databricks services.
It is a DAB template that is focused on using ML models, pipeliens, and evaluation workflows.

MLOps Stacks enable the enforcement of repository layout standardization,
MLFlow experiment tracking patterns, training and inference job definitions, and evaluation and validation workflows.
By levering the CI/CD templates, we get a uniform and consistent ways of creating, testing, and deploying solutions.

===== Choosing a Deployment Option
Choosing between MLOps Stacks and Databricks Asset Bundles may seem like a challenging decision.
Fortunately, Databricks has taken an opinionated stance on the topic.
WHen deploying classic ML projects, MLOps Stacks is the best tool.
However, if you are deploying a generative and agentic AI project, DAB is a better choice.

Understand that MLOps Stacks is a Databricks Asset Bundle template,
which means you are really choosing which template to start from, not if you will use DABs or not.
MLOps Stacks is not the best choice for all projects, as shown in <<ch13_mlops_dabs_decision_flow>>.

[[ch13_mlops_dabs_decision_flow]]
.MLOps Stacks and DABs Decision Tree
image::images/ch13/DABS vs MLOps Stacks.png["DABS vs MLOps Stacks", width=600, height=400]

In summary, DABs are an excellent way of packaging and deploying components.
It is an ideal technology for use with generative and agentic solutions.
Use MLOps Stacks to deploy classical ML projects.
Do not use MLOps stacks for agents.
Next, we look into how to deploy the services typically used in a generative AI solution.

=== Deploying Databricks Services for Generative and Agentic Solutions
So far, we have discussed the importance of automated deployment,
the concept of environments,
and the tools Databricks provides to help with packaging and deploying complex solutions.
Now, we will look at the specific services we typically use when building generative and agentic solutions on Databricks.

==== MLflow
As discussed previously, MLflow has evolved from experient tracking to helping in the creation and deployment of complex LLM-based solutions.
Today, MLflow has capabilities relating to LLM development, prompt versioning, and much more.
In this section, we will evalaute the capabilities of MLflow from a packaging and deployment perspective.
This is not intended to be a through discussion of all of the capabilities of MLflow, rather, what you need to know to package and deploy a generative and agentic solution.

===== Models - Standardized Packaging Format
There are a multitude of frameworks, eacdh of which have their own way of packaging models.
https://mlflow.org/docs/latest/ml/model/[MLflow Models] brings a standard consistent way of interacting with models, regardless of the framework involved.
MLflow Models provide a standardized way to package machine learning and generative models, regardless of the underlying framework. 
Whether you are working with Hugging Face Transformers, PyTorch, TensorFlow, or scikit-learn, MLflow abstracts away the differences and presents a consistent interface for saving, loading, and deploying models. 
This uniformity is essential when building complex solutions that may combine traditional ML, LLMs, and agentic pipelines. 
By adopting the MLflow model format, teams can focus on developing and iterating their solutions, confident that deployment and management will remain consistent across environments.

For example, imagine deploying a model-hosted agent designed to assist with data engineering tasks.
One of its core activities is to generate data quality rules for a given dataframe. The agent first examines the schema, column names, and descriptive statistics to suggest basic validation rules.
It then invokes a large language model (LLM) to reason about more complex patterns, such as detecting outliers, inferring business logic, or proposing cross-column constraints.
With MLflow, you can package this entire workflow—including the code for schema analysis, the LLM integration, and supporting artifacts—into a single deployable unit.
A typical MLflow model directory for this agent might look like:

[source]
----
/data_quality_agent_model/
├── MLmodel
├── requirements.txt
├── code/
│   └── data_quality_agent/
│       ├── __init__.py
│       ├── agent.py
│       └── utils.py
├── artifacts/
│   ├── system_prompt.txt
│   └── example_rules.json
└── python_model.py
----

Now that we have a brief sketch of what we may be packaging, let's look at some of the key features of MLflow Models that facilitate deploying complex generative and agentic solutions. 
This ties into a discussion of the model lifecycle, which we will cover in the next section.

**TODO: Finish this section**

https://docs.databricks.com/en/mlflow/models.html[Log, load, and register MLflow models]

- Packages entire RAG pipelines or agentic systems as a single deployable unit
- Supports custom models that orchestrate retrieval, tool calls, and generation
- Simplifies deployment to Model Serving without framework-specific configuration

===== MLflow Model Registry - Lifecycle Management

Once a model-hosted agent has been packaged, the next challenge is managing its lifecycle as it moves from development to production. The MLflow Model Registry provides a central hub for tracking, versioning, and promoting models across environments. Each time the data quality agent is updated—whether to improve its schema analysis, enhance LLM reasoning, or refine its prompt templates—a new version can be registered, ensuring that every change is tracked and auditable.

The registry supports stage transitions such as “Staging,” “Production,” and “Archived,” making it easy to promote a model after validation or roll back to a previous version if issues arise. This process enforces discipline and governance, reducing the risk of accidental overwrites or untested code reaching production. By using the Model Registry as the single source of truth, teams can confidently manage the evolution of their agentic solutions, knowing that every version is documented, reviewable, and ready for automated deployment.
### TODO: write this section

Version control for fine-tuned LLMs, RAG applications, and agent systems
Aliases for staging promotion (staging, production, archived)
Centralized governance with model lineage tracking
Single source of truth for validated models across environments

Model Registry (versioning, staging, aliases, governance): https://docs.databricks.com/en/mlflow/model-registry.html


===== MLflow Tracking - Observability & Lineage

###  TODO: write this section

Logs parameters (hyperparameters, prompt templates, retrieval configs)
Records metrics (perplexity, BLEU scores, custom evaluation metrics)
Stores artifacts (model checkpoints, prompt versions, evaluation datasets)
LLM Tracing: Traces entire flow from prompt → tool calls → response for debugging

Tracking (runs, params/metrics/artifacts, lineage; includes LLM tracing): https://docs.databricks.com/en/mlflow/tracking.html


=====  MLflow Projects - Reproducibility

### TODO: write this section

Packages complex fine-tuning pipelines and RAG systems as reproducible units
Encapsulates all dependencies and execution logic
Enables team collaboration with consistent environments

Projects (reproducible packaging; note deprecated but still informative): https://docs.databricks.com/en/mlflow/projects.html

===== Integration with Deployment Pipeline

### TODO: write this section

Works with Databricks Asset Bundles (DABs) for environment promotion
Supports CI/CD workflows with git-based versioning
Enables automated testing and validation before production deployment





===== Prompts

### TODO: write this section

Tracks prompt template versions alongside model versions
Critical for maintaining consistency across deployments
Ensures reproducibility of agentic behavior

Prompt/version management (MLflow prompt engineering): https://docs.databricks.com/en/generative-ai/mlflow-prompt-engineering.html


===== Databricks Model Serving

Databricks Model Serving for MLflow models (deployment targets): https://docs.databricks.com/en/machine-learning/model-serving/index.html

Hosting models on Databricks

### TODO: write this section

==== AI Gateway

AI Gateway (routing, throttling, auth, observability for served models): https://docs.databricks.com/en/generative-ai/ai-gateway/index.html

### TODO: write this section

deployment role (routing, throttling, standardizing model access)
how AI Gateway participates in deployment pipelines and environment separation
See chapter 11 (TODO: need section link here) for more details on policy and governace of AI Gateway.


https://docs.databricks.com/en/generative-ai/ai-gateway/index.html

==== Vector Search Indexes

==== DB Apps

==== Data Ingestion and Refinement

Keep this as a thin deployment-focused section:
How Lakeflow assets are referenced in a DAB and promoted alongside models and DB Apps.
One or two sentences per subheading to anchor what type of ingestion they represent.
Avoid re-teaching Lakeflow; cross-reference the earlier platform chapter for fundamentals.

===== Lakeflow Jobs

===== Lakeflow Connect

===== Lakeflow Apache Declarative Pipelines

===== Custom Notebooks

[#ch13_walkthrough_example]
=== Walk-Through Example 

1-2 solid paragraphs each explaining how the asset type is deployed (preferably via DAB + CLI + CI/CD), and
In at least one case (MLflow/Model Serving or DB Apps), a small concrete example (CLI snippet, YAML fragment, or pseudo-config).

NOTE: We may need to combine streamlit app ( https://github.com/databricks/bundle-examples/tree/main/contrib/templates/streamlit-app) and other templates (https://github.com/databricks/app-templates)

=== Conclusion
