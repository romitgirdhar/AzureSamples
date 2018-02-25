# Introduction 
This project helps you move your data from Oracle to Azure Data Lake Storage using Azure Data Factory v2 and self-hosted Integrated Runtime. This sample iterates through all the tables in a given oracle database and copies it over to Azure Data Lake Storage.

# Getting Started

1. This sample is in .NET and will require an IDE such as Visual Studio 2017 or VS code to run this solution.
1. You will first need to create a self-hosted IR. This can be done by following [these steps](https://docs.microsoft.com/en-us/azure/data-factory/tutorial-hybrid-copy-powershell#create-a-self-hosted-integration-runtime).
1. You will also need to install the self-hosted IR on a gateway machine in your datacenter. You can follow the steps here to [deploy a self-hosted IR](https://docs.microsoft.com/en-us/azure/data-factory/create-self-hosted-integration-runtime).
1. You will also need an AAD service principal account, which can be setup using [these steps](https://docs.microsoft.com/en-us/azure/azure-resource-manager/resource-group-create-service-principal-portal#create-an-azure-active-directory-application). This is necessary for your code to communicate with Microsoft Azure.
1. Next, clone the application and fill in the required details in the code and execute the job.

# Contribute
Feel free to contribute to this sample by submitting PRs.