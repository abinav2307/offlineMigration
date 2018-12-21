
namespace Microsoft.Azure.Documents.Tools.POC.OfflineMigration
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.IO;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Threading;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.CosmosDB.BulkExecutor;

    using Newtonsoft.Json;

    internal sealed class ReadDocumentFeedByPKRange
    {
        /// <summary>
        /// Reads documents from the source collection and writes them into the destination collection
        /// </summary>
        /// <param name="client">DocumentClient instance used to read from source and write to destination</param>
        /// <param name="dbName">Database name for the source collection</param>
        /// <param name="collectionName">Collection name for the source collection</param>
        /// <param name="pkRangeToDocCountMapping">Map of count of documents processed per partition for the source collection</param>
        /// <param name="pkRangeToContTokenMapping">Map of continuation token by partition for the source collection</param>
        /// <param name="pkRanges">Partition key ranges for the source collection</param>
        /// <param name="MaxItemCountInput"></param>
        /// <param name="MaxBufferedItemCountInput"></param>
        /// <param name="MaxDegreeOfParallelismInput">Number of physical partitions to process in parallel</param>
        private static void GetDocumentCount(
            DocumentClient client,
            string dbName,
            string collectionName,
            Dictionary<string, int> pkRangeToDocCountMapping,
            Dictionary<string, string> pkRangeToContTokenMapping,
            List<string> pkRanges,
            int MaxItemCountInput,
            int MaxBufferedItemCountInput,
            int MaxDegreeOfParallelismInput)
        {
            // 1. Documents link for Source Account/Collection
            Uri documentsLink = UriFactory.CreateDocumentCollectionUri(dbName, collectionName);
            string documentsLinkAsString = string.Concat(documentsLink.ToString(), "/docs/");

            // 2. Documents link for Destination Account/Collection
            string destinationDBName = ConfigurationManager.AppSettings["DestinationDatabaseName"];
            string destinationCollectionName = ConfigurationManager.AppSettings["DestinationCollectionName"];
            Uri newDocumentsLink = UriFactory.CreateDocumentCollectionUri(destinationDBName, destinationCollectionName);
            string FileNamePrefix = "Partition";

            Parallel.ForEach(pkRanges, new ParallelOptions { MaxDegreeOfParallelism = MaxDegreeOfParallelismInput }, pkRange =>
            {
                string fileName = string.Concat(FileNamePrefix, "_", pkRange);

                // Local file that keeps track of any failed write for each partition that is being migrated to the new account
                using (StreamWriter file = new StreamWriter(Path.Combine(Directory.GetCurrentDirectory(), fileName)))
                {
                    pkRangeToContTokenMapping[pkRange] = null;

                    int continuation = 1;

                    do
                    {
                        FeedResponse<dynamic> response = client.ReadDocumentFeedAsync(
                            documentsLinkAsString,
                            new FeedOptions
                            {
                                MaxItemCount = MaxItemCountInput,
                                MaxBufferedItemCount = MaxBufferedItemCountInput,
                                PartitionKeyRangeId = pkRange,
                                RequestContinuation = pkRangeToContTokenMapping[pkRange]
                            }).Result;

                        Dictionary<string, object> objectRetrieved = new Dictionary<string, object>();
                        string partitionKeyFieldName = ConfigurationManager.AppSettings["PartitionKeyFieldName"];
                        string pkField = "";
                        string idField = "";

                        foreach (dynamic document in response)
                        {
                            try
                            {
                                client.CreateDocumentAsync(newDocumentsLink, document, null, true);
                            }
                            catch (DocumentClientException ex)
                            {
                                objectRetrieved = JsonConvert.DeserializeObject<Dictionary<string, object>>(document.ToString());
                                pkField = objectRetrieved[partitionKeyFieldName].ToString();
                                idField = objectRetrieved["id"].ToString();

                                if (string.IsNullOrEmpty(pkField))
                                {
                                    Console.WriteLine("Document does not contain partition key field: {0}", partitionKeyFieldName);
                                }

                                if (string.IsNullOrEmpty(idField))
                                {
                                    Console.WriteLine("Document does not contain /id field");
                                }

                                if ((int)ex.StatusCode == 409)
                                {
                                    Console.WriteLine(
                                        "Received status code 409 while attempting to write document with partition key: {0} and id: {1} as the resource already exists",
                                        pkField,
                                        idField);
                                }

                                else if ((int)ex.StatusCode == 429)
                                {
                                    Console.WriteLine(
                                        "Throttled when writing document with pk: {0} and id: {1}. Original message was: {2}",
                                        pkField,
                                        idField,
                                        ex.Message);

                                    while (true)
                                    {
                                        // If a 429 was thrown during the Write Operation, sleep for twice as long as recommended and re-try
                                        Thread.Sleep(ex.RetryAfter);
                                        Thread.Sleep(ex.RetryAfter);

                                        try
                                        {
                                            // If the write operation succeeds after a retry on throttle, break the while loop
                                            client.CreateDocumentAsync(newDocumentsLink, document, null, true);
                                            break;
                                        }
                                        catch (DocumentClientException e)
                                        {
                                            Console.WriteLine(
                                                "Yet another Exception seen while issuing a write operation for document with pk: {0} and id: {1}. Message is: {2}",
                                                pkField,
                                                idField,
                                                e.Message);
                                        }
                                    }
                                }

                                else
                                {
                                    Console.WriteLine(
                                        "Exception thrown when writing document with pk: {0} and id: {1}. Original message was: {2}. Not a 429 or 409. Logging to local file.",
                                        pkField,
                                        idField,
                                        ex.Message);

                                    // Log the pkField and idField to a local file as this would need to be re-tried at a later time.
                                    file.WriteLine(
                                        "Exception thrown when writing document with pk: {0} and id: {1}. Original message was: {2}. Not a 429 or 409. Logging to local file.",
                                        pkField,
                                        idField,
                                        ex.Message);
                                }
                            }
                        }

                        pkRangeToDocCountMapping[pkRange] += response.Count;
                        pkRangeToContTokenMapping[pkRange] = response.ResponseContinuation;

                        Console.WriteLine(
                            "PKRange: {0}, Continuation: {1}, Document Count: {2}",
                            pkRange,
                            continuation,
                            pkRangeToDocCountMapping[pkRange]);

                        continuation++;
                    }
                    while (!string.IsNullOrEmpty(pkRangeToContTokenMapping[pkRange]));
                }
            });
        }

        /// <summary>
        /// Processes documents from the source to be written to the destination
        /// </summary>
        /// <param name="client">DocumentClient instance used to read from source and write to destination</param>
        /// <param name="dbName">Database name for the source collection</param>
        /// <param name="collectionName">Collection name for the source collection</param>
        /// <param name="pkRanges">Partition key ranges for the source collection</param>
        /// <param name="pkRangeToDocCountMapping">Map of count of documents processed per partition for the source collection</param>
        /// <param name="pkRangeToContTokenMapping">Map of continuation token by partition for the source collection</param>
        /// <param name="MaxItemCountInput"></param>
        /// <param name="MaxBufferedItemCountInput"></param>
        /// <param name="MaxDegreeOfParallelism">Number of physical partitions to process in parallel</param>
        public static void ReadDocumentFeed(
            DocumentClient client,
            string dbName,
            string collectionName,
            List<string> pkRanges,
            Dictionary<string, int> pkRangeToDocCountMapping,
            Dictionary<string, string> pkRangeToContTokenMapping,
            int MaxItemCountInput,
            int MaxBufferedItemCountInput,
            int MaxDegreeOfParallelism)
        {
            Uri collectionUri = UriFactory.CreateDocumentCollectionUri(dbName, collectionName);

            try
            {
                GetDocumentCount(
                    client,
                    dbName,
                    collectionName,
                    pkRangeToDocCountMapping,
                    pkRangeToContTokenMapping,
                    pkRanges,
                    MaxItemCountInput,
                    MaxBufferedItemCountInput,
                    MaxDegreeOfParallelism);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Type of Exception thrown is: {0}", ex.GetType().ToString());
                Console.WriteLine("Exception message is: {0}", ex.Message);
                Console.WriteLine("Inner exception message is: {0}", ex.InnerException.Message);
            }
        }
    }
}
