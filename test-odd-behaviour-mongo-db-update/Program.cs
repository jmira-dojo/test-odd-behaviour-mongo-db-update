using System.Text.Json;
using AutoFixture;
using AutoFixture.Kernel;
using EphemeralMongo;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Bson.Serialization.Conventions;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;

var fixture = new Fixture();
fixture.Customizations.Add(new ObjectIdStringGenerator());

BsonSerializer.RegisterSerializer(typeof(Guid), new GuidSerializer(BsonType.String));
BsonSerializer.RegisterSerializer(typeof(DateTimeOffset), new DateTimeOffsetSerializer(BsonType.String));

var conventionPack = new ConventionPack
{
    new EnumRepresentationConvention(BsonType.String),
    new IgnoreExtraElementsConvention(true)
};
ConventionRegistry.Register("Conventions", conventionPack, type => true);

var options = new MongoRunnerOptions
{
    AdditionalArguments = "--quiet"
};

using var runner = MongoRunner.Run(options);

var database = new MongoClient(runner.ConnectionString).GetDatabase("default");

var bsonDocumentCollection = GetCollection<BsonDocument>();
var documentCollection = GetCollection<Document>();

var documentForFindOneAndUpdateAsync = fixture.Build<Document>().Without(d => d.Url).Create();

await FindOneAndUpdateAsync(documentForFindOneAndUpdateAsync);

var findOneAndUpdateAsyncDocumentAsBsonDocument = (await bsonDocumentCollection.FindAsync(Builders<BsonDocument>.Filter.Eq("_id", ObjectId.Parse(documentForFindOneAndUpdateAsync.Id)))).ToList().FirstOrDefault();
var findOneAndUpdateAsyncDocumentAsDocument = (await documentCollection.FindAsync(Builders<Document>.Filter.Eq("_id", ObjectId.Parse(documentForFindOneAndUpdateAsync.Id)))).ToList().FirstOrDefault();

var findOneAndUpdateAsyncUrlBsonValue = findOneAndUpdateAsyncDocumentAsBsonDocument.GetValue("Url");
var findOneAndUpdateAsyncUrl = findOneAndUpdateAsyncDocumentAsDocument.Url;

var originalUrlBsonValue = documentForFindOneAndUpdateAsync.ToBsonDocument().GetValue("Url");

Console.WriteLine("ORIGINAL DOCUMENT");
Console.WriteLine(JsonSerializer.Serialize(documentForFindOneAndUpdateAsync));
Console.WriteLine();
Console.WriteLine("UPSERTED DOCUMENT");
Console.WriteLine(findOneAndUpdateAsyncDocumentAsBsonDocument.ToJson());
Console.WriteLine();
Console.WriteLine("ORIGINAL Url (value + type)");
Console.WriteLine($"{documentForFindOneAndUpdateAsync.Url ?? "null"} + {originalUrlBsonValue.GetType()}");
Console.WriteLine();
Console.WriteLine("UPSERTED Url (value + type)");
Console.WriteLine($"{findOneAndUpdateAsyncUrl ?? "null"} + {findOneAndUpdateAsyncUrlBsonValue.GetType()}");

Console.ReadLine();

IMongoCollection<T> GetCollection<T>()
{
    return database.GetCollection<T>("documents");
}

async Task FindOneAndUpdateAsync<T>(T document, FilterDefinition<T> filter = null)
{
    var bsonDocument = document.ToBsonDocument();

    if (filter == null)
    {
        var bsonValue = bsonDocument["_id"];
        filter = Builders<T>.Filter.Eq("_id", bsonValue);
    }

    var propertiesToIgnore = new List<string> { "_t" };

    if (bsonDocument.GetValue("_id", null) == null)
    {
        propertiesToIgnore.Add("_id");
    }

    var updates = bsonDocument
        .Where(a => !propertiesToIgnore.Contains(a.Name))
        .Select(a => Builders<T>.Update.Set(a.Name, a.Value));

    await GetCollection<T>().FindOneAndUpdateAsync(
        filter,
        Builders<T>.Update.Combine(updates),
        new FindOneAndUpdateOptions<T> { IsUpsert = true });
}

[BsonDiscriminator(RootClass = true)]
public class Document
{
    [BsonId, BsonRepresentation(BsonType.ObjectId)]
    [BsonIgnoreIfDefault]
    public string Id { get; set; }
    public DateTimeOffset CreatedAt { get; set; }
    public DateTimeOffset UpdatedAt { get; set; }
    public string Url { get; set; }
}

public class ObjectIdStringGenerator : ISpecimenBuilder
{
    public object Create(object request, ISpecimenContext context)
    {
        var propertyInfo = request as System.Reflection.PropertyInfo;
        if (propertyInfo?.Name == "Id" && propertyInfo.PropertyType == typeof(string))
        {
            return ObjectId.GenerateNewId().ToString();
        }

        return new NoSpecimen();
    }
}