using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public static class DatabaseFactory
    {
        public static async Task BulkInsert(string endpoint,
                                            string authKey,
                                            string databaseName,
                                            string containerName,
                                            int itemsToInsert = 100000,
                                            int containerThroughput = 10000)
        {
            // using bulk-optimized client here for one-off insert operations

            CosmosClientOptions options = new CosmosClientOptions
            {
                AllowBulkExecution = true
            };

            using CosmosClient cosmosClient = new CosmosClient(endpoint, authKey, options);

            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);

            try
            {
                Container container = await database.CreateContainerAsync(containerName, "/State", 10000);

                IReadOnlyCollection<Item> items = GetItemsToInsert(itemsToInsert);

                Task[] tasks = items.Select(item => container.CreateItemAsync(item, new PartitionKey(item.State))
                        .ContinueWith(itemResponse =>
                        {
                            if (!itemResponse.IsCompletedSuccessfully)
                            {
                                Debug.Assert(itemResponse.Exception != null);

                                AggregateException innerException = itemResponse.Exception.Flatten();

                                if (innerException.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException) is CosmosException cosmosException)
                                {
                                    Debug.WriteLine($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                                }
                                else
                                {
                                    Debug.WriteLine($"Exception {innerException.InnerExceptions.FirstOrDefault()}.");
                                }
                            }
                        })).ToArray();

                await Task.WhenAll(tasks);

                var counts = new ItemCounts
                {
                    Counts = items.GroupBy(i => i.State)
                                  .Select(grp => new StateCount(grp.Key, grp.Count()))
                                  .ToArray()
                };

                await container.CreateItemAsync(counts, new PartitionKey(counts.State));

                Debug.WriteLine("Bulk insert complete.");
            }
            catch (CosmosException ex)
            {
                if (ex.StatusCode == System.Net.HttpStatusCode.Conflict)
                {
                    return; // container already exists
                }
            }
        }

        private static IReadOnlyCollection<Item> GetItemsToInsert(int itemsToInsert)
        {
            return new Bogus.Faker<Item>()
                .StrictMode(false)
                .RuleFor(o => o.FirstName, f => f.Name.FirstName())
                .RuleFor(o => o.LastName, f => f.Name.LastName())
                .RuleFor(o => o.State, (f, o) => f.Address.StateAbbr())
                .RuleFor(o => o.Address, (f, o) => $"{f.Address.StreetAddress()} {f.Address.City()}, {o.State} {f.Address.ZipCode()}")
                .RuleFor(o => o.Email, (f, o) => f.Internet.Email(firstName: o.FirstName, lastName: o.LastName))
                .RuleFor(o => o.Id, (f, o) => o.Email)
                .Generate(itemsToInsert);
        }
    }
}
