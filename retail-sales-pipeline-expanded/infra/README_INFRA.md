# Infrastructure Hints (NOT a full ARM/Bicep template)
- Create ADLS Gen2 storage account with HNS enabled.
- Create Databricks workspace and configure a service principal + mount to ADLS using OAuth.
- Configure Databricks secret scope for credentials.
- Use ADF to orchestrate Databricks notebooks or use Databricks Jobs API.
