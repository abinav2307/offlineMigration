
namespace Microsoft.Azure.Documents.Tools.POC.OfflineMigration
{
    using System;
    using System.Collections.Generic;
    using System.Configuration;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;

    class Program
    {
        // 1. Source information - Cosmos DB
        private static readonly string SourceEndpoint = ConfigurationManager.AppSettings["SourceEndPointUrl"];
        private static readonly string SourceAuthKey = ConfigurationManager.AppSettings["SourceAuthorizationKey"];
        private static readonly string SourceDatabaseName = ConfigurationManager.AppSettings["SourceDatabaseName"];
        private static readonly string SourceDataCollectionName = ConfigurationManager.AppSettings["SourceCollectionName"];
        private static readonly string PKRanges = ConfigurationManager.AppSettings["PKRanges"];

        // 2. Destination information - Cosmos DB
        private static readonly string DestinationEndpoint = ConfigurationManager.AppSettings["DestinationEndPointUrl"];
        private static readonly string DestinationAuthKey = ConfigurationManager.AppSettings["DestinationAuthorizationKey"];
        private static readonly string DestinationDatabaseName = ConfigurationManager.AppSettings["DestinationDatabaseName"];
        private static readonly string DestinationDataCollectionName = ConfigurationManager.AppSettings["DestinationCollectionName"];

        private static readonly int MaxItemCount = int.Parse(ConfigurationManager.AppSettings["MaxItemCount"]);
        private static readonly int MaxBufferedItemCount = int.Parse(ConfigurationManager.AppSettings["MaxBufferedItemCount"]);

        private static readonly int MaxDegreeOfParallelism = int.Parse(ConfigurationManager.AppSettings["DegreeOfParallelism"]);

        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };

        public static void Main(string[] args)
        {
            Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(String.Format("Source Endpoint: {0}", SourceEndpoint));
            Console.WriteLine(String.Format("Source Database, Collection : {0}, {1}", SourceDatabaseName, SourceDataCollectionName));
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(String.Format("Destination Endpoint: {0}", DestinationEndpoint));
            Console.WriteLine(String.Format("Destination Database, Collection : {0}, {1}", DestinationDatabaseName, DestinationDataCollectionName));
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");

            try
            {
                using (var sourceClient = new DocumentClient(new Uri(SourceEndpoint), SourceAuthKey, ConnectionPolicy))
                using (var destinationClient = new DocumentClient(new Uri(DestinationEndpoint), DestinationAuthKey, ConnectionPolicy))
                {
                    List<string> partitionKeyRanges = new List<string>();
                    ReadPartitionKeyRanges(sourceClient, partitionKeyRanges).Wait();

                    Dictionary<string, int> pkRangeToDocCountMapping = new Dictionary<string, int>();
                    Dictionary<string, string> pkRangeToContTokenMapping = new Dictionary<string, string>();

                    foreach (string eachPkrange in partitionKeyRanges)
                    {
                        pkRangeToDocCountMapping.Add(eachPkrange, 0);
                    }

                    ReadDocumentFeedByPKRange.ReadDocumentFeed(
                        sourceClient,
                        SourceDatabaseName,
                        SourceDataCollectionName,
                        partitionKeyRanges,
                        pkRangeToDocCountMapping,
                        pkRangeToContTokenMapping,
                        MaxItemCount,
                        MaxBufferedItemCount,
                        MaxDegreeOfParallelism);

                    Console.ReadLine();
                }
            }
            catch (AggregateException e)
            {
                Console.WriteLine("Caught AggregateException in Main, Inner Exception Message: " + e.InnerException.Message);
                Console.ReadLine();
            }
        }

        /// <summary>
        /// Fetches the list of physical partitions for this collection
        /// </summary>
        /// <param name="client">DocumentClient instance to fetch metadata for the source collection</param>
        /// <param name="partitionKeyRanges">List of partition key ranges for this collection</param>
        /// <returns></returns>
        private static async Task ReadPartitionKeyRanges(DocumentClient client, List<string> partitionKeyRanges)
        {
            Uri sourceCollectionUri = UriFactory.CreateDocumentCollectionUri(SourceDatabaseName, SourceDataCollectionName);

            string pkRangesResponseContinuation = null;
            do
            {
                FeedResponse<PartitionKeyRange> pkRanges = await client.ReadPartitionKeyRangeFeedAsync(
                    sourceCollectionUri,
                    new FeedOptions { RequestContinuation = pkRangesResponseContinuation });

                foreach (PartitionKeyRange pkRange in pkRanges)
                {
                    partitionKeyRanges.Add(pkRange.Id);
                }
            }
            while (pkRangesResponseContinuation != null);
        }
    }
}
