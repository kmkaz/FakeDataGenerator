using MongoDB.Bson;
using MongoDB.Driver;
using Bogus;
using System.Diagnostics;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

class Program
{
    private static string mongoConnectionString;
    private static string cosmosConnectionString;
    private static string mongoDbId = "MyDataMongo";
    private static string collectionName = "FakeEvents";
    private static string cosmosDbId = "MyData";
    private static string containerId = "FakeEvents";

    static Faker faker = new Faker();
    static DateTime now = DateTime.Now;
    static DateTime weekAgo = DateTime.Now.AddDays(-7);
    static int maxBatchSizeMongo = 500;
    static int maxBatchSizeCosmos = 500;

    static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true).Build();

        mongoConnectionString = config.GetSection("mongoConnection")?.Value ?? "";
        cosmosConnectionString = config.GetSection("cosmosConnection")?.Value ?? "";
 
        var sw = new Stopwatch();
        Console.Write("Total number of records: ");
        _ = int.TryParse(Console.ReadLine(), out var total);
        Console.Write("Threads: ");
        _ = int.TryParse(Console.ReadLine(), out var threads);
        Console.Write("Cosmos or Mongo? [C/M]: ");
        var mongo = Console.ReadLine()?.ToUpper() == "M";

        var totalRecordsPerThread = (int)Math.Ceiling((double)total / threads);
        var tasks = new List<Task>(threads);
        sw.Start();
        for (var i = 0; i < threads; i++)
        {
            tasks.Add(mongo ? InsertRecordsMongo($"{i}", totalRecordsPerThread) : InsertRecordsCosmos($"{i}", totalRecordsPerThread));
        }
        await Task.WhenAll(tasks);
        Console.WriteLine($"Time taken: {sw.Elapsed.TotalSeconds}");
        Console.ReadLine();
    }

    static async Task InsertRecordsCosmos(string threadName, int numberOfRecords)
    {
        var cosmosClient = new CosmosClient(cosmosConnectionString, new CosmosClientOptions() { AllowBulkExecution = true });
        var database = cosmosClient.GetDatabase(cosmosDbId);
        var container = database.GetContainer(containerId);
        var docs = GenerateDocumentsCosmos(numberOfRecords);
        var tasks = new List<Task>(numberOfRecords);
        foreach (var doc in docs)
        {
            tasks.Add(container.CreateItemAsync(doc, new PartitionKey(doc.reference)).ContinueWith(resp =>
            {
                var exc = resp?.Exception;
                if (exc != null)
                {
                    Console.WriteLine($"Error: {exc.Flatten().Message}");
                }
            }));
        }

        await Task.WhenAll(tasks);
        Console.WriteLine($"{threadName}.{numberOfRecords}");
    }

    static async Task InsertRecordsCosmosX(string threadName, int numberOfRecords)
    {
        var cosmosClient = new CosmosClient(cosmosConnectionString, new CosmosClientOptions() { AllowBulkExecution = true });
        var database = cosmosClient.GetDatabase(cosmosDbId);
        var container = database.GetContainer(containerId);
        var batchSize = numberOfRecords > maxBatchSizeMongo ? maxBatchSizeMongo : numberOfRecords;
        var recordsLeft = numberOfRecords;
        var totalInserted = batchSize;
        do
        {
            var docs = GenerateDocumentsCosmos(batchSize);
            var tasks = new List<Task>(batchSize);
            foreach (var doc in docs)
            {
                tasks.Add(container.CreateItemAsync(doc, new PartitionKey(doc.reference)).ContinueWith(resp =>
                {
                    var exc = resp?.Exception;
                    if (exc != null)
                    {
                        Console.WriteLine($"Error: {exc.Flatten().Message}");
                    }
                }));
            }

            await Task.WhenAll(tasks);
            Console.WriteLine($"{threadName}.{totalInserted}/{numberOfRecords}");

            recordsLeft -= batchSize;
            if (batchSize > recordsLeft)
                batchSize = recordsLeft;
            totalInserted += batchSize;
        } while (recordsLeft > 0);
    }


    static Event[] GenerateDocumentsCosmos(int total)
    {
        var count = 0;
        var docs = new Event[total];
        do
        {
            docs[count] = GenerateEventObj();
            count++;
        } while (count < total);
        return docs;
    }

    static Event GenerateEventObj()
    {
        return new Event
        {
            id = Guid.NewGuid().ToString(),
            reference = faker.Random.AlphaNumeric(16),
            batchId = $"{DateTime.Now:yyyyMMdd}",
            carrierParcelStatusDescription = faker.Commerce.Color(),
            carrierParcelStatusId = faker.Random.Number(1, 50),
            cmsDescription = faker.Company.CatchPhrase(),
            cmsId = faker.Random.Number(1, 50),
            cmsParcelStatusDescription = faker.Company.CatchPhrase(),
            cmsParcelStatusId = faker.Random.Number(1, 50),
            sourceCountry = faker.Address.CountryCode(),
            parcelId = faker.Random.Number(1, 50),
            consignmentId = faker.Random.AlphaNumeric(16),
            statusAchievedDateTime = $"{faker.Date.Between(now, weekAgo)}",
            cmsNotifiedDateTime = $"{faker.Date.Between(now, weekAgo)}",
            systemNotifiedDateTime = $"{faker.Date.Between(now, weekAgo)}",
            createdOn = $"{faker.Date.Between(now, weekAgo)}",
            consignmentDirection = faker.Commerce.Color(),
            carrierId = faker.Random.Number(1, 150),
            carrierName = faker.Company.CompanyName(),
            carrierCode = faker.Random.AlphaNumeric(3),
            carrierServiceId = faker.Random.Number(1, 1000),
            carrierServiceCode = faker.Random.Number(1, 500),
            carrierServiceName = faker.Company.CatchPhrase(),
            parcelStatusId = faker.Random.Number(1, 50),
            parcelStatusDescription = faker.Lorem.Sentence()
        };
    }

    static async Task InsertRecordsMongo(string threadName, int numberOfRecords)
    {
        var client = new MongoClient(mongoConnectionString);
        var database = client.GetDatabase(mongoDbId);
        var collection = database.GetCollection<BsonDocument>(collectionName);
        var batchSize = numberOfRecords > maxBatchSizeMongo ? maxBatchSizeMongo : numberOfRecords;
        var recordsLeft = numberOfRecords;
        var totalInserted = batchSize;
        do
        {
            var docs = GenerateDocumentsMongo(batchSize);
            try
            {
                await collection.InsertManyAsync(docs);
                Console.WriteLine($"{threadName}.{totalInserted}/{numberOfRecords}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting document: {ex.Message}");
            }
            recordsLeft -= batchSize;
            if (batchSize > recordsLeft)
                batchSize = recordsLeft;
            totalInserted += batchSize;
        } while (recordsLeft > 0);
    }

    static BsonDocument[] GenerateDocumentsMongo(int total)
    {
        var count = 0;
        var docs = new BsonDocument[total];
        do
        {
            docs[count] = GenerateBsonDoc();
            count++;
        } while (count < total);
        return docs;
    }

    static BsonDocument GenerateBsonDoc()
    {
        return new BsonDocument
        {
            {  "reference", faker.Random.AlphaNumeric(16) },
            {  "batchId", $"{DateTime.Now:yyyyMMdd}" },
            {  "carrierParcelStatusDescription", faker.Commerce.Color()  },
            {  "carrierParcelStatusId", faker.Random.Number(1, 50) },
            {  "cmsDescription", faker.Company.CatchPhrase() },
            {  "cmsId", faker.Random.Number(1, 50) },
            {  "cmsParcelStatusDescription", faker.Company.CatchPhrase() },
            {  "cmsParcelStatusId", faker.Random.Number(1, 50) },
            {  "sourceCountry", faker.Address.CountryCode() },
            {  "parcelId", faker.Random.Number(1, 50) },
            {  "consignmentId", faker.Random.AlphaNumeric(16) },
            {  "statusAchievedDateTime", $"{ faker.Date.Between(now, weekAgo)}" },
            {  "cmsNotifiedDateTime", $"{ faker.Date.Between(now, weekAgo)}" },
            {  "systemNotifiedDateTime", $"{ faker.Date.Between(now, weekAgo)}" },
            {  "createdOn", $"{ faker.Date.Between(now, weekAgo)}" },
            {  "consignmentDirection", faker.Commerce.Color() },
            {  "carrierId", faker.Random.Number(1, 150) },
            {  "carrierName", faker.Company.CompanyName() },
            {  "carrierCode", faker.Random.AlphaNumeric(3) },
            {  "carrierServiceId", faker.Random.Number(1, 1000) },
            {  "carrierServiceCode", faker.Random.Number(1, 500) },
            {  "carrierServiceName", faker.Company.CatchPhrase() },
            {  "parcelStatusId", faker.Random.Number(1, 50) },
            {  "parcelStatusDescription", faker.Lorem.Sentence() }
        };
    }
}