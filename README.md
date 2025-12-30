<a id="readme-top"></a>

[![LinkedIn][linkedin-shield]][linkedin-url]


<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/floydmoratti/fx-data-pipeline">
    <img src="images/logo.png" alt="Logo" width="100" height="100">
  </a>

<h3 align="center">FX Data Pipeline</h3>

  <p align="center">
    AWS Event-driven ingestion, transform, and anomaly detection with EventBridge, Step Functions, Lambda, S3, Athena, CloudWatch, and SNS.
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#key-features">Key Features</a></li>
    <li><a href="#architecture">Architecture</a></li>
    <li><a href="#services-used">Services Used</a></li>
    <li><a href="#data-design">Data Design</a>
      <ul>
        <li><a href="#ingested-data">Ingested Data</a></li>
        <li><a href="#s3-layout">S3 Layout</a></li>
      </ul>
    </li>
    <li><a href="#monitoring-and-alerts">Monitoring and Alerts</a></li>
    <li><a href="#infrastructure-as-code">Infrastructure as Code</a></li>
    <li><a href="#deployment">Deployment</a></li>
    <li><a href="#design-decisions">Design Decisions</a></li>
    <li><a href="#why-this-project">Why This Project</a></li>
    <li><a href="#contact">Contact</a></li>
  </ol>
</details>


<!-- ABOUT THE PROJECT -->
## About The Project

[![Product Name Screen Shot][product-screenshot]](images/diagram.png)

This project implements a production-style, event-driven FX data pipeline on AWS using fully managed, serverless services.

The pipeline ingests foreign exchange rates, processes and stores them in S3, and performs analytical checks using Athena, with observability and alerting built in.

The infrastructure is defined entirely using AWS CloudFormation, demonstrating Infrastructure as Code (IaC), least-privilege IAM, and operational best practices.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- KEY FEATURES -->
## Key Features

- Scheduled ingestion using Amazon EventBridge
- Orchestration with AWS Step Functions
- Serverless compute via AWS Lambda
- Partitioned data lake in Amazon S3
- Query & analysis with Amazon Athena
- Metrics & alerts using Amazon CloudWatch and SNS
- Secure secrets handling via AWS Systems Manager Parameter Store
- Fully reproducible infrastructure using CloudFormation

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- ARCHITECTURE -->
## Architecture

1. EventBridge triggers the pipeline on a schedule
2. Step Functions orchestrates the workflow
3. Lambda (Ingest) fetches FX rates from an external API
4. Lambda (Transform) normalizes and partitions the data
5. Lambda (Analysis) queries Athena and publishes custom metrics
6. CloudWatch Alarms monitor failures and anomalies
7. SNS sends notifications on alert conditions

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- SERVICES USED -->
## Services Used

| Service | Purpose |
|----------|----------|
| AWS Lambda  | Ingest, transform, and analyze FX data  |
| AWS Step Functions  | Workflow orchestration  |
| Amazon EventBridge  | Scheduled execution  |
| Amazon S3  | Raw and processed data storage  |
| Amazon Athena  | SQL-based analytics  |
| Amazon CloudWatch  | Logs, metrics, and alarms  |
| Amazon SNS  | Alert notifications  |
| AWS IAM  | Security and access control  |
| AWS Systems Manager  | Secure storage and retrieval of API secrets  |
| AWS CloudFormation  | Infrastructure as Code  |
| GitHub Actions  | CI/CD automation for infrastructure deployment  |

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- DATA DESIGN -->
## Data Design
### Ingested Data
- Source: External FX rates API
- Base currency: USD (API limitation)
- Supports multiple FX pairs in a single run (e.g. USDJPY, EURUSD)
- Inverse rates (e.g. EURUSD) are calculated in-code when required

### S3 Layout
Partitioned by currency pair, year, month, day.
  ```r

    s3://<fx-data-bucket>/
        └── processed/
          └── pair=USDJPY/
              └── year=2025/
                  └── month=01/
                      └── day=30/
                          └── data.json

  ```

Partition projection is used in Athena to avoid expensive scans and repetitive MSCK REPAIR TABLE operations.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- MONITORING AND ALERTS -->
## Monitoring and Alerts

The pipeline includes CloudWatch alarms for:

- Lambda execution errors
- Missing Lambda invocations (pipeline not running)
- Custom FX deviation metrics exceeding thresholds

Alerts are routed through SNS for notification delivery.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- IAC -->
## Infrastructure as Code

All AWS resources are defined in CloudFormation YAML, including:

- IAM roles and policies
- Lambda functions and aliases
- Step Functions state machine
- EventBridge schedule
- S3 buckets
- Athena database and table
- CloudWatch alarms and SNS topics

This allows the entire stack to be recreated or updated consistently.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- DEPLOYMENT -->
## Deployment

1. Upload Lambda deployment packages to S3
2. Deploy the CloudFormation template
3. Provide required parameters (bucket names, code locations, etc.)
4. The pipeline runs automatically on schedule

The project is designed to support Git-based workflows and automated deployments.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- DESIGN -->
## Design Decisions

- Single ingestion Lambda handles multiple FX pairs to minimize resource usage
- Serverless-first approach keeps operational overhead low
- Strong observability ensures failures are detected quickly
- Least-privilege IAM improves security posture
- Partition projection improves Athena performance and cost efficiency
- Multi-environment cloudformation logic and tags for seperation of production and development environments

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- WHY -->
## Why This Project
This project was built to demonstrate:

- Real-world cloud architecture patterns
- Production-minded design decisions
- Infrastructure as Code proficiency
- Observability and reliability principles
- Practical use of AWS serverless services

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- CONTACT -->
## Contact

Floyd Moratti - floyd.moratti@gmail.com.com

Project Link: [https://github.com/floydmoratti/fx-data-pipeline](https://github.com/floydmoratti/fx-data-pipeline)

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- MARKDOWN LINKS & IMAGES -->
[linkedin-shield]: https://custom-icon-badges.demolab.com/badge/LinkedIn-0A66C2?logo=linkedin-white&logoColor=fff
[linkedin-url]: https://linkedin.com/in/floydmoratti/
[product-screenshot]: images/diagram.png
