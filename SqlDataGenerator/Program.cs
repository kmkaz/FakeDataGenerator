using Bogus;
using Microsoft.Extensions.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;

class Program
{
    private static string sqlConnectionString = "";
    private static string insertQuery = 
        @"INSERT INTO EventData (Id, Reference, BatchId, CarrierParcelStatusDescription, CarrierParcelStatusId, 
        CmsDescription, CmsId, CmsParcelStatusDescription, CmsParcelStatusId, SourceCountry, ParcelId, 
        ConsignmentId, StatusAchievedDateTime, CmsNotifiedDateTime, SystemNotifiedDateTime, CreatedOn, 
        ConsignmentDirection, CarrierId, CarrierName, CarrierCode, CarrierServiceId, CarrierServiceCode, 
        CarrierServiceName, ParcelStatusId, ParcelStatusDescription) VALUES (@Id, @Reference, @BatchId, @CarrierParcelStatusDescription, @CarrierParcelStatusId, 
        @CmsDescription, @CmsId, @CmsParcelStatusDescription, @CmsParcelStatusId, @SourceCountry, @ParcelId, 
        @ConsignmentId, @StatusAchievedDateTime, @CmsNotifiedDateTime, @SystemNotifiedDateTime, @CreatedOn, 
        @ConsignmentDirection, @CarrierId, @CarrierName, @CarrierCode, @CarrierServiceId, @CarrierServiceCode, 
        @CarrierServiceName, @ParcelStatusId, @ParcelStatusDescription)";

    static Faker faker = new Faker();
    static DateTime now = DateTime.Now;
    static DateTime weekAgo = DateTime.Now.AddDays(-7);

    static async Task Main(string[] args)
    {
        var config = new ConfigurationBuilder()
        .SetBasePath(Directory.GetCurrentDirectory())
        .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true).Build();

        sqlConnectionString = config.GetSection("sqlConnection")?.Value ?? "";

        var sw = new Stopwatch();
        Console.Write("Total number of records: ");
        _ = int.TryParse(Console.ReadLine(), out var total);
        Console.Write("Threads: ");
        _ = int.TryParse(Console.ReadLine(), out var threads);
        var totalRecordsPerThread = (int)Math.Ceiling((double)total / threads);
        var tasks = new List<Task>(threads);
        sw.Start();
        for (var i = 0; i < threads; i++)
        {
            tasks.Add(InsertRecords($"{i}", totalRecordsPerThread));
        }
        await Task.WhenAll(tasks);
        Console.WriteLine($"Time taken: {sw.Elapsed.TotalSeconds}");
        Console.ReadLine();
    }

    static async Task InsertRecords(string threadName, int numberOfRecords)
    {
        var docs = GenerateEvents(numberOfRecords);
        var tasks = new List<Task>(numberOfRecords);
        
        using var connection = new SqlConnection(sqlConnectionString);
        connection.Open();
        foreach (var doc in docs)
        {
            InsertToSql(connection, doc);
        }
        connection.Close();
        await Task.WhenAll(tasks);
        Console.WriteLine($"{threadName}.{numberOfRecords}");
    }

    static void InsertToSql(SqlConnection connection, Event eventData)
    {
        using (SqlCommand command = new SqlCommand())
        {
            command.Connection = connection;
            command.CommandType = CommandType.Text;
            command.CommandText = insertQuery;

            command.Parameters.AddWithValue("Id", eventData.Id);
            command.Parameters.AddWithValue("Reference", eventData.Reference);
            command.Parameters.AddWithValue("BatchId", eventData.BatchId);
            command.Parameters.AddWithValue("CarrierParcelStatusDescription", eventData.CarrierParcelStatusDescription);
            command.Parameters.AddWithValue("CarrierParcelStatusId", eventData.CarrierParcelStatusId);
            command.Parameters.AddWithValue("CmsDescription", eventData.CmsDescription);
            command.Parameters.AddWithValue("CmsId", eventData.CmsId);
            command.Parameters.AddWithValue("CmsParcelStatusDescription", eventData.CmsParcelStatusDescription);
            command.Parameters.AddWithValue("CmsParcelStatusId", eventData.CmsParcelStatusId);
            command.Parameters.AddWithValue("SourceCountry", eventData.SourceCountry);
            command.Parameters.AddWithValue("ParcelId", eventData.ParcelId);
            command.Parameters.AddWithValue("ConsignmentId", eventData.ConsignmentId);
            command.Parameters.AddWithValue("StatusAchievedDateTime", eventData.StatusAchievedDateTime);
            command.Parameters.AddWithValue("CmsNotifiedDateTime", eventData.CmsNotifiedDateTime);
            command.Parameters.AddWithValue("SystemNotifiedDateTime", eventData.SystemNotifiedDateTime);
            command.Parameters.AddWithValue("CreatedOn", eventData.CreatedOn);
            command.Parameters.AddWithValue("ConsignmentDirection", eventData.ConsignmentDirection);
            command.Parameters.AddWithValue("CarrierId", eventData.CarrierId);
            command.Parameters.AddWithValue("CarrierName", eventData.CarrierName);
            command.Parameters.AddWithValue("CarrierCode", eventData.CarrierCode);
            command.Parameters.AddWithValue("CarrierServiceId", eventData.CarrierServiceId);
            command.Parameters.AddWithValue("CarrierServiceCode", eventData.CarrierServiceCode);
            command.Parameters.AddWithValue("CarrierServiceName", eventData.CarrierServiceName);
            command.Parameters.AddWithValue("ParcelStatusId", eventData.ParcelStatusId);
            command.Parameters.AddWithValue("ParcelStatusDescription", eventData.ParcelStatusDescription);
            command.ExecuteNonQuery();
        }
    }

    static Event[] GenerateEvents(int total)
    {
        var count = 0;
        var docs = new Event[total];
        do
        {
            docs[count] = GenerateEvent();
            count++;
        } while (count < total);
        return docs;
    }

    static Event GenerateEvent()
    {
        return new Event
        {
            Id = Guid.NewGuid().ToString(),
            Reference = faker.Random.AlphaNumeric(16),
            BatchId = $"{DateTime.Now:yyyyMMdd}",
            CarrierParcelStatusDescription = faker.Commerce.Color(),
            CarrierParcelStatusId = faker.Random.Number(1, 50),
            CmsDescription = faker.Lorem.Sentence(5),
            CmsId = faker.Random.Number(1, 50),
            CmsParcelStatusDescription = faker.Lorem.Sentence(5),
            CmsParcelStatusId = faker.Random.Number(1, 50),
            SourceCountry = faker.Address.CountryCode(),
            ParcelId = faker.Random.Number(1, 50),
            ConsignmentId = faker.Random.AlphaNumeric(16),
            StatusAchievedDateTime = $"{faker.Date.Between(now, weekAgo)}",
            CmsNotifiedDateTime = $"{faker.Date.Between(now, weekAgo)}",
            SystemNotifiedDateTime = $"{faker.Date.Between(now, weekAgo)}",
            CreatedOn = $"{faker.Date.Between(now, weekAgo)}",
            ConsignmentDirection = faker.Commerce.Color(),
            CarrierId = faker.Random.Number(1, 150),
            CarrierName = faker.Company.CompanyName(),
            CarrierCode = faker.Random.AlphaNumeric(3),
            CarrierServiceId = faker.Random.Number(1, 1000),
            CarrierServiceCode = faker.Random.Number(1, 500),
            CarrierServiceName = faker.Lorem.Sentence(5),
            ParcelStatusId = faker.Random.Number(1, 50),
            ParcelStatusDescription = faker.Lorem.Sentence(5),
        };
    }

}