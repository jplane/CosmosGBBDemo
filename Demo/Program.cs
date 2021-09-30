using Common;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Polly;
using Polly.Contrib.WaitAndRetry;
using Polly.Retry;
using System;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

namespace Demo
{
    public class Program
    {
        record QueryResult(string State, int Count);

        record ConfigInfo(string endpoint, string key, string database, string container);

        static Lazy<CosmosClient> _client = new Lazy<CosmosClient>(CreateClient);

        static Lazy<ConfigInfo> _configInfo = new Lazy<ConfigInfo>(GetConfig);

        static async Task Main(string[] args)
        {
            try
            { 
                await DatabaseFactory.BulkInsert(_configInfo.Value.endpoint,
                                                 _configInfo.Value.key,
                                                 _configInfo.Value.database,
                                                 _configInfo.Value.container);

                using CosmosClient cosmosClient = _client.Value;

                Database database = cosmosClient.GetDatabase(_configInfo.Value.database);

                Container container = database.GetContainer(_configInfo.Value.container);

                await Query(container, TimeSpan.FromMilliseconds(4000));

                await PointRead(container, TimeSpan.FromMilliseconds(800));

                await Upsert(container, TimeSpan.FromMilliseconds(1000));
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }

        private static void LogDiagnostics(CosmosDiagnostics diagnostics, Func<CosmosDiagnostics, bool> shouldLog)
        {
            if (shouldLog(diagnostics))
            {
                Console.WriteLine(diagnostics.ToString());  // log to App Insights, etc.
            }
        }

        private static CosmosClient CreateClient()
        {
            CosmosClientOptions options = new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Direct,
                EnableTcpConnectionEndpointRediscovery = true,

                ApplicationRegion = Regions.AustraliaEast

                //ApplicationPreferredRegions = new []
                //{
                //    Regions.WestUS3,
                //    Regions.CentralUS,
                //    Regions.WestEurope
                //}
            };

            return new CosmosClient(_configInfo.Value.endpoint, _configInfo.Value.key, options);
        }

        private static ConfigInfo GetConfig()
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .Build();

            string endpoint = configuration["EndPointUrl"];

            if (string.IsNullOrEmpty(endpoint))
            {
                throw new ArgumentNullException("Please specify a valid endpoint in the appSettings.json");
            }

            string authKey = configuration["AuthorizationKey"];

            if (string.IsNullOrEmpty(authKey) || string.Equals(authKey, "Super secret key"))
            {
                throw new ArgumentException("Please specify a valid AuthorizationKey in the appSettings.json");
            }

            string databaseName = configuration["DatabaseName"];

            if (string.IsNullOrEmpty(authKey))
            {
                throw new ArgumentException("Please specify a valid DatabaseName in the appSettings.json");
            }

            string containerName = configuration["ContainerName"];

            if (string.IsNullOrEmpty(authKey))
            {
                throw new ArgumentException("Please specify a valid ContainerName in the appSettings.json");
            }

            return new ConfigInfo(endpoint, authKey, databaseName, containerName);
        }

        private static AsyncRetryPolicy GetRetryPolicy(TimeSpan medianFirstRetryDelay, int retryCount)
        {
            var delay = Backoff.DecorrelatedJitterBackoffV2(medianFirstRetryDelay: medianFirstRetryDelay, retryCount: retryCount);

            // see: https://docs.microsoft.com/en-us/azure/cosmos-db/sql/troubleshoot-dot-net-sdk?tabs=diagnostics-v3

            return Policy
                .Handle<CosmosException>(ex => ex.StatusCode switch
                {
                    HttpStatusCode.RequestTimeout => true,
                    HttpStatusCode.Gone => true,
                    HttpStatusCode.ServiceUnavailable => true,
                    _ => ((int) ex.StatusCode) == 449   // applies for writes only
                })
                .WaitAndRetryAsync(delay, (Exception ex, TimeSpan _) => LogDiagnostics(((CosmosException) ex).Diagnostics, _ => true));
        }

        private static async Task PointRead(Container container, TimeSpan requestThreshold)
        {
            var retryPolicy = GetRetryPolicy(TimeSpan.FromSeconds(1), 3);

            ItemResponse<ItemCounts> response = await retryPolicy.ExecuteAsync(
                () => container.ReadItemAsync<ItemCounts>(ItemCounts.DocumentId, new PartitionKey(ItemCounts.PartitionKey)));

            LogDiagnostics(response.Diagnostics, diag => diag.GetClientElapsedTime() > requestThreshold);

            Console.WriteLine(JsonConvert.SerializeObject(response.Resource));
            Console.WriteLine();

            Console.WriteLine("Point Read elapsed client time: " + response.Diagnostics.GetClientElapsedTime());
            Console.WriteLine("Point Read RUs consumed: " + response.RequestCharge);

            var regions = response.Diagnostics
                                  .GetContactedRegions()
                                  .Aggregate(string.Empty, (curr, pair) => $"{curr} [{pair.regionName}]");

            Console.WriteLine("Point Read contacted regions: " + regions);
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
        }

        private static async Task Query(Container container, TimeSpan requestThreshold)
        {
            var retryPolicy = GetRetryPolicy(TimeSpan.FromSeconds(1), 3);

            FeedResponse<QueryResult> response = await retryPolicy.ExecuteAsync(() =>
            {
                var sql = "SELECT c.State, COUNT(1) as Count from Items c GROUP BY c.State";

                using FeedIterator<QueryResult> iterator = container.GetItemQueryIterator<QueryResult>(sql);

                return iterator.ReadNextAsync();
            });

            LogDiagnostics(response.Diagnostics, diag => diag.GetClientElapsedTime() > requestThreshold);

            Console.WriteLine(JsonConvert.SerializeObject(response.Resource));
            Console.WriteLine();

            Console.WriteLine("Query elapsed client time: " + response.Diagnostics.GetClientElapsedTime());
            Console.WriteLine("Query RUs consumed: " + response.RequestCharge);

            var regions = response.Diagnostics
                                  .GetContactedRegions()
                                  .Aggregate(string.Empty, (curr, pair) => $"{curr} [{pair.regionName}]");

            Console.WriteLine("Query contacted regions: " + regions);
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
        }

        private static async Task Upsert(Container container, TimeSpan requestThreshold)
        {
            Item item = new Item
            {
                Id = "josh@email.com",
                FirstName = "Josh",
                LastName = "Lane",
                Address = "123 Easy Street Anytown AR 55222",
                State = "AR",
                Email = "josh@email.com"
            };

            var retryPolicy = GetRetryPolicy(TimeSpan.FromSeconds(1), 3);

            ItemResponse<Item> response = await retryPolicy.ExecuteAsync(() => container.UpsertItemAsync(item, new PartitionKey(item.State)));

            LogDiagnostics(response.Diagnostics, diag => diag.GetClientElapsedTime() > requestThreshold);

            Console.WriteLine(JsonConvert.SerializeObject(response.Resource));
            Console.WriteLine();

            Console.WriteLine("Insert elapsed client time: " + response.Diagnostics.GetClientElapsedTime());
            Console.WriteLine("Insert RUs consumed: " + response.RequestCharge);

            var regions = response.Diagnostics
                                  .GetContactedRegions()
                                  .Aggregate(string.Empty, (curr, pair) => $"{curr} [{pair.regionName}]");

            Console.WriteLine("Insert contacted regions: " + regions);
            Console.WriteLine();
            Console.WriteLine();
            Console.WriteLine();
        }
    }
}
