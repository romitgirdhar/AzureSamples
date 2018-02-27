using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Hadoop.Avro;
using Microsoft.Hadoop.Avro.Container;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.Azure;
using System.Runtime.Serialization;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace AvroFileMerger
{

    class Program
    {
        static void Main(string[] args)
        {
            //Read data from files
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("storageAccountName"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(CloudConfigurationManager.GetSetting("eventHubName"));
            var blobInputInitialPath = CloudConfigurationManager.GetSetting("eventHubName") + "/" + CloudConfigurationManager.GetSetting("eventHubName") + "/";
            var blobOutputInitialPath = "outputdata/";
            var dt = DateTime.UtcNow.AddHours(-1);
            var dtStr = dt.ToString("yyyy/MM/dd/HH/");

            var ehPartitions = 8;
            MemoryStream stream = new MemoryStream();
            MemoryStream outStream = new MemoryStream();

            var outputSchemaJson = "{\"type\":\"record\",\"name\":\"EventDataOut\",\"namespace\":\"Microsoft.ServiceBus.Messaging\",\"fields\":[{\"name\":\"Offset\",\"type\":\"string\"},{\"name\":\"EnqueuedTimeUtc\",\"type\":\"string\"}, {\"name\":\"Body\",\"type\":[\"null\",\"bytes\"]}]}";
            var outputSchema = Microsoft.Hadoop.Avro.Schema.TypeSchema.Create(outputSchemaJson);

            var writer = AvroContainer.CreateGenericWriter(outputSchemaJson, outStream, Codec.Null);


            for (int i = 0 ; i < ehPartitions; i++)
            {
                var blobPath = blobInputInitialPath + i.ToString() + "/" + dtStr;
                foreach (IListBlobItem item in container.ListBlobs(blobPath, true))
                {
                    CloudBlockBlob cbb = (CloudBlockBlob)item;
                    if(cbb.Properties.Length > 200)
                    {
                        Console.Out.WriteLine("Downloading File: " + cbb.Name);
                        stream = new MemoryStream();
                        cbb.DownloadToStream(stream);

                        //Setting stream position to zero so we can read from the start
                        stream.Position = 0;

                        var reader = AvroContainer.CreateGenericReader(stream); //Reading the stream
                        AvroRecord avroRecord = new AvroRecord(outputSchema);   //Creating a new Avro Record to be written
                        

                        while (reader.MoveNext())
                        {
                            var streamWriter = new SequentialWriter<object>(writer, reader.Current.Objects.Count());

                            var testCount = reader.Current.Objects.Count();
                            Console.Out.WriteLine("Reading downloaded Objects. Count: " + testCount);

                            foreach (dynamic record in reader.Current.Objects)
                            {
                                var eventData = new EventData(record);
                                var eventDataOut = new EventDataOut(eventData);
                                dynamic outRecord = new AvroRecord(outputSchema);
                                outRecord.Offset = eventData.Offset;
                                outRecord.EnqueuedTimeUtc = eventData.EnqueuedTimeUtc.ToString();
                                outRecord.Body = eventData.Body;

                                streamWriter.Write(outRecord);   //Writing the Avro Record
                            
                            }
                        }
                    }
                }
            }

            //Test to Read the Avro File we just wrote

            outStream.Position = 0;

            var testreader = AvroContainer.CreateGenericReader(outStream);

            while (testreader.MoveNext())
            {
                var testCount = testreader.Current.Objects.Count();
                Console.Out.WriteLine("Reading Written Objects. Count: " + testCount);

                foreach (dynamic record in testreader.Current.Objects)
                {
                    var eventData = new EventDataOut(record);

                    Console.Out.WriteLine("Test - EventData Body: " + Encoding.UTF8.GetString(eventData.Body));
                }
            }

            outStream.Position = 0;
            var outputBlob = container.GetBlockBlobReference(blobOutputInitialPath + dtStr + "output.avro");
            outputBlob.UploadFromStream(outStream);

            Console.Out.WriteLine("Test Done");

        }
    }
}
