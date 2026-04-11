# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Create a notebook
notebookutils.notebook.create('CreateANotebook','How to create a notebook')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

./builtin
./builtin/01 - Create Delta Tables.ipynb

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read notebook template from a file
with open("./builtin/01 - Create Delta Tables.ipynb", "r") as f:
    notebook_content = f.read()

# Create the notebook
notebook = notebookutils.notebook.create(
    name="ProcessingNotebook",
    description="Data processing notebook from template",
    content=notebook_content
)

print(f"Created notebook: {notebook.displayName} (ID: {notebook.id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

MSLearnDEnb

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Minimum valid notebook content - content cannot be empty
minimal_content = '''{
    "cells": [],
    "metadata": {},
    "nbformat": 4,
    "nbformat_minor": 5
}'''

# Create notebook with default lakehouse configuration
notebook = notebookutils.notebook.create(
    name="DataAnalysis",
    description="Analysis notebook with lakehouse access",
    content=minimal_content,
    defaultLakehouse="MSLearnDEnb",
    defaultLakehouseWorkspace=""  # Current workspace
)

print(f"Created notebook with lakehouse: {notebook.displayName}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Create a notebook in another workspace
with open("./builtin/01 - Create Delta Tables.ipynb", "r") as f:
    content = f.read()

notebook = notebookutils.notebook.create(
    name="SharedNotebook",
    description="Notebook for the shared workspace",
    content=content,
    workspaceId="95d5a6a0-efbc-4ab3-80e9-75516c3369df"
)

print(f"Created in remote workspace: {notebook.displayName}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebook = notebookutils.notebook.get("NotebookSimple")

print(f"Notebook Name: {notebook.displayName}")
print(f"Notebook ID: {notebook.id}")
print(f"Description: {notebook.description}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.credentials.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

storage_token = notebookutils.credentials.getToken('storage')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

artifact = notebookutils.lakehouse.create("lakehouse_name", "Description of the Lakehouse")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

artifact = notebookutils.lakehouse.create(
    "SalesAnalyticsWithSchema1",
    "Lakehouse with schema support for multi-tenant data",
    {"enableSchemas": True}
)

print(f"Created lakehouse with schema support: {artifact.displayName}")
print(f"Lakehouse ID: {artifact.id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = "95d5a6a0-efbc-4ab3-80e9-75516c3369df"

artifact = notebookutils.lakehouse.create(
    name="SharedAnalytics1",
    description="Shared analytics lakehouse",
    workspaceId=workspace_id
)

print(f"Created lakehouse in workspace: {workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

departments = ["Sales1", "Marketing1", "Finance1", "Operations1"]

created_lakehouses = []
for dept in departments:
    lakehouse = notebookutils.lakehouse.create(
        name=f"{dept}Analytics",
        description=f"Analytics lakehouse for {dept} department"
    )
    created_lakehouses.append(lakehouse)
    print(f"Created: {lakehouse.displayName}")

print(f"Created {len(created_lakehouses)} lakehouses")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

artifact = notebookutils.lakehouse.get("lakehouse_name" )

print(f"Lakehouse Name: {artifact.displayName}")
print(f"Lakehouse ID: {artifact.id}")
print(f"Workspace ID: {artifact.workspaceId}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

artifact = notebookutils.lakehouse.getWithProperties("lakehouse_name")

print(f"Lakehouse: {artifact.displayName}")
print(f"Properties: {artifact.properties}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace_id = "95d5a6a0-efbc-4ab3-80e9-75516c3369df"
artifact = notebookutils.lakehouse.get("SharedAnalytics", workspaceId=workspace_id)

print(f"Retrieved: {artifact.displayName} from workspace {workspace_id}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

artifacts_list = notebookutils.lakehouse.list()

print(f"Found {len(artifacts_list)} lakehouses:")
for lh in artifacts_list:
    print(f"  - {lh.displayName} (ID: {lh.id})")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.runtime.context

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context = notebookutils.runtime.context

if context['isForPipeline']:
    print("Pipeline mode: Strict error handling")
    error_handling = "strict"
    max_retries = 3
elif context['isReferenceRun']:
    print("Running as a referenced notebook")
    error_handling = "strict"
    max_retries = 2
else:
    print("Interactive mode: Lenient error handling")
    error_handling = "lenient"
    max_retries = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context = notebookutils.runtime.context

if context['isReferenceRun']:
    print("Reference Run Context:")
    print(f"  Current Run ID: {context['currentRunId']}")
    print(f"  Parent Run ID: {context['parentRunId']}")
    print(f"  Root Run ID: {context['rootRunId']}")
    print(f"  Root Notebook: {context['rootNotebookName']}")
else:
    print("Not a reference run")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

context = notebookutils.runtime.context

run_id = context.get("currentRunId") or "interactive"
log_prefix = (
    f"[{context['currentWorkspaceName']}/{context['currentNotebookName']}]"
    f" run={run_id}"
)

print(f"{log_prefix} Processing started")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.credentials.getSecret('https://fabricnotebooksecrete.vault.azure.net/', 'Test-Key-Vault-Secret')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.credentials.help()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.credentials.getToken('keyvault')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
