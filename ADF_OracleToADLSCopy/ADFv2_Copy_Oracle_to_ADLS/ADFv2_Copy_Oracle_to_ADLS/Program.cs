using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Rest;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.Azure.Management.ResourceManager;

//Before executing this program, please ensure that the OracleVM and ADLS is created and that the ServicePrincipal has owner access to the ADLS.
//Also, please create and install the DMG on the gateway box.

//Install the following NuGet Packages:
//1. Microsoft.Azure.Management.DataFactory
//2. Microsoft.IdentityModel.Clients.ActiveDirectory
//3. Microsoft.Azure.Management.ResourceManager

namespace ADFv2_Copy_Oracle_to_ADLS
{
    class Program
    {
        const string Region = "<ADFv2 Region>";
        const string rg = "<Resource Group>"; //Your Resource group
        const string dataFactory = "<ADFv2 name>";
        const string SubscriptionID = "<Azure subscription ID>";
        const string TenantID = "<AAD Tenant ID>";
        const string ClientID = "<AAD Service Principal ID>";
        const string ClientSecret = "<AAD Service Principal Key>";
        const string oracleDB = "<oracle DB Name>";
        const string oracleUsername = "<Oracle Username>";
        const string oraclePassword = "<Oracle Password>";

        const string lookuppipeline = "oraclelookup";
        const string copypipeline = "oracletoadlscopy";
        const string oracleDataset = oracleDB;
        const string adlsDatset = oracleDB + "adls";
        const string oracleLinkedService = "<oracle linked service name>";
        const string adlsLinkedService = "<adls linked service name>";
        const string oracleLookupActivity = "OracleTableLookupList";

        class TestClass
        {
            public string OWNER { get; set; }
            public string TABLE_NAME { get; set; }
        }


        static void Main(string[] args)
        {
            try
            {
                var context = new AuthenticationContext("https://login.windows.net/" + TenantID + "/oauth2/authorize");
                ClientCredential cc = new ClientCredential(ClientID, ClientSecret);
                AuthenticationResult result = context.AcquireTokenAsync("https://management.azure.com/", cc).Result;
                TokenCredentials cred = new TokenCredentials(result.AccessToken);
                var client = new DataFactoryManagementClient(cred) { SubscriptionId = SubscriptionID };

                Factory df = GetDataFactory(client);

                if (df == null)
                {
                    df = CreateOrUpdateDataFactory(client);
                }

                while (df.ProvisioningState != "Succeeded")
                {
                    df = GetDataFactory(client);
                    System.Threading.Thread.Sleep(1000);
                }

                Console.Out.WriteLine("ADFv2 found or created!");

                var response = client.Pipelines.DeleteWithHttpMessagesAsync(rg, dataFactory, lookuppipeline).Result;

                var lsresponse = client.LinkedServices.CreateOrUpdate(rg, dataFactory, oracleLinkedService, OracleLinkedServiceDefinition());
                lsresponse = client.LinkedServices.CreateOrUpdate(rg, dataFactory, adlsLinkedService, ADLSLinkedServiceDefinition());

                var dsresponse = client.Datasets.CreateOrUpdate(rg, dataFactory, oracleDB, OracleDatasetDefinition());
                dsresponse = client.Datasets.CreateOrUpdate(rg, dataFactory, adlsDatset, ADLSDatasetDefinition());

                var ppresponse = client.Pipelines.CreateOrUpdate(rg, dataFactory, lookuppipeline, TableListLookupPipeline());
                ppresponse = client.Pipelines.CreateOrUpdate(rg, dataFactory, copypipeline, OracleToADLSCopyPipelineDef());


                var parameters = new Dictionary<string, object>
                {
                    { "dbowner", oracleUsername}
                };

                CreateRunResponse rr = null;
                rr = CreateRun(client, parameters);
                Console.WriteLine("Pipeline Run submitted. RunId: " + rr.RunId.ToString());

                //Monitoring the run                
                while (true)
                {
                    System.Threading.Thread.Sleep(10000);
                    var run = client.PipelineRuns.Get(rg, dataFactory, rr.RunId);

                    if (run.Status != "InProgress")
                    {
                        Console.Out.WriteLine("Run {0} finished with Status: {1}", run.RunId, run.Status);
                        if (run.Status == "Failed")
                        {
                            Console.Error.WriteLine("Failure Details: " + run.Message);
                        }

                        Console.Out.WriteLine("Result: {0}", run);
                        break;
                    }

                    Console.Out.WriteLine("Run {0} currently has status: {1}", run.RunId, run.Status);
                }

                Console.Out.WriteLine("Press Enter to exit");
                Console.ReadLine();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
                System.Environment.Exit(1);
            }
        }

        static Factory CreateOrUpdateDataFactory(DataFactoryManagementClient client)
        {
            Factory resource = new Factory
            {
                Location = Region
            };

            Factory response;
            {
                response = client.Factories.CreateOrUpdate(rg, dataFactory, resource);
            }

            return response;
        }

        static Factory GetDataFactory(DataFactoryManagementClient client)
        {
            try
            {
                var factory = client.Factories.Get(rg, dataFactory);
                return factory;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Error getting factory. Error Details: " + e.ToString());
                return null;
            }

        }

        /*** LinkedService Definitions ***/
        static LinkedServiceResource OracleLinkedServiceDefinition()
        {
            OracleLinkedService properties = new OracleLinkedService
            {
                ConnectionString = new SecureString("Host=<LAN IP Address>;Port=1521;Sid=<Database Name>;User Id="+oracleUsername+";Password="+ oraclePassword + ";"),
                ConnectVia = new IntegrationRuntimeReference
                {
                    ReferenceName = "oraclevmir"
                }
            };

            return new LinkedServiceResource(properties);
        }

        static LinkedServiceResource ADLSLinkedServiceDefinition()
        {
            AzureDataLakeStoreLinkedService properties = new AzureDataLakeStoreLinkedService
            {
                DataLakeStoreUri = "https://<adlsname>.azuredatalakestore.net/webhdfs/v1",
                ServicePrincipalId = ClientID,
                ServicePrincipalKey = new SecureString(ClientSecret),
                Tenant = TenantID,
                SubscriptionId = SubscriptionID,
                ResourceGroupName = rg
            };

            return new LinkedServiceResource(properties);
        }


        static DatasetResource ADLSDatasetDefinition()
        {
            AzureDataLakeStoreDataset properties = new AzureDataLakeStoreDataset
            {
                LinkedServiceName = new LinkedServiceReference
                {
                    ReferenceName = adlsLinkedService
                },
                Parameters = new Dictionary<string, ParameterSpecification>
                {
                    { "folderPath", new ParameterSpecification { Type = ParameterType.String } }
                },
                Format = new TextFormat(),
                FolderPath = new Expression
                {
                    Value = "@dataset().folderPath"
                }
            };

            return new DatasetResource(properties);
        }

        static DatasetResource OracleDatasetDefinition()
        {
            OracleTableDataset properties = new OracleTableDataset
            {

                LinkedServiceName = new LinkedServiceReference
                {
                    ReferenceName = oracleLinkedService
                },
                TableName = new Expression
                {
                    Value = "DummyTable"
                }
            };

            return new DatasetResource(properties);

        }


        static PipelineResource TableListLookupPipeline()
        {
            PipelineResource resource = new PipelineResource
            {
                Parameters = new Dictionary<string, ParameterSpecification>
                {
                    { "dbowner", new ParameterSpecification { Type = ParameterType.String, DefaultValue = "\"" + oracleUsername + "\""} }
                },
                Activities = new List<Activity>
                {
                    new LookupActivity
                    {
                        Name = oracleLookupActivity,
                        Source = new OracleSource
                        {
                            OracleReaderQuery = "SELECT OWNER, TABLE_NAME FROM all_tables WHERE OWNER = '" + oracleUsername + "'" //@pipeline.parameters.dbowner
                        },
                        Dataset = new DatasetReference
                        {
                            ReferenceName = oracleDataset,
                        },
                        FirstRowOnly = false,   //Default behavior is true currently
                    },
                    new ExecutePipelineActivity
                    {
                        Name = "ExecuteOracleToADLSCopyPipeline",
                        Parameters = new Dictionary<string, object>
                        {
                            { "oracleTableList", new Expression{ Value = "@activity('OracleTableLookupList').output.value"} }
                        },
                        Pipeline = new PipelineReference
                        {
                            ReferenceName = copypipeline
                        },
                        WaitOnCompletion = true,
                        DependsOn = new List<ActivityDependency>
                        {
                            new ActivityDependency
                            {
                                Activity = oracleLookupActivity,
                                DependencyConditions = new List<string>
                                {
                                    "Succeeded"
                                }
                            }
                        }
                    }
                }
            };

            return resource;
        }

        static PipelineResource OracleToADLSCopyPipelineDef()
        {
            PipelineResource resource = new PipelineResource
            {
                Parameters = new Dictionary<string, ParameterSpecification>
                {
                    { "oracleTableList", new ParameterSpecification{ Type = ParameterType.Array } },
                    { "oracledb", new ParameterSpecification { Type = ParameterType.String, DefaultValue = oracleDB } },
                },
                Activities = new List<Activity>
                {

                    new ForEachActivity
                    {
                        Name = "OracleToADLSForEach",
                        IsSequential = false,
                        Items = new Expression
                        {
                            Value = "@pipeline().parameters.oracleTableList"
                        },
                        Activities = new List<Activity>
                        {
                            new CopyActivity
                            {
                                Name = "OracleToADLSCopy",
                                Source = new OracleSource
                                {
                                    SourceRetryCount = 3,
                                    OracleReaderQuery = "SELECT * FROM \"@{item().TABLE_NAME}\""
                                },
                                Sink = new AzureDataLakeStoreSink
                                {
                                    SinkRetryCount = 3
                                },
                                Inputs = new List<DatasetReference>
                                {
                                    new DatasetReference
                                    {
                                        ReferenceName = oracleDataset
                                    }
                                },
                                Outputs = new List<DatasetReference>
                                {
                                    new DatasetReference
                                    {
                                        ReferenceName = adlsDatset,
                                        Parameters = new Dictionary<string, object>
                                        {
                                            {"folderPath", new Expression{ Value = "@concat(pipeline().parameters.oracledb, '/', item().OWNER, '/', item().TABLE_NAME)"} }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };
            return resource;
        }


        static CreateRunResponse CreateRun(DataFactoryManagementClient client, Dictionary<string, object> arguments)
        {
            try
            {
                //var response = client.Pipelines.CreateRunAsync(rg, dataFactory, pipeline, arguments).Result;
                var response = client.Pipelines.CreateRunWithHttpMessagesAsync(rg, dataFactory, lookuppipeline, arguments).Result;
                return response.Body;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine(e);
                return null;
            }

        }
    }
}
