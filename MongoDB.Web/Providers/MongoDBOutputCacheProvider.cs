using System;
using System.Collections.Specialized;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace MongoDB.Web.Providers
{
    public class MongoDBOutputCacheProvider : System.Web.Caching.OutputCacheProvider
    {
        private MongoCollection _mongoCollection;

        public override object Add(string key, object entry, DateTime utcExpiry)
        {
            Set(key, entry, utcExpiry);
            return entry;
        }

        public override object Get(string key)
        {
            var bsonDocument = _mongoCollection.FindOneAs<BsonDocument>(Query.EQ("Key", key));

            if (bsonDocument == null)
            {
                return null;
            }

            if (bsonDocument["Expiration"].ToUniversalTime() <= DateTime.UtcNow)
            {
                Remove(key);
                return null;
            }

            using (var memoryStream = new MemoryStream(bsonDocument["Value"].AsByteArray))
            {
                return new BinaryFormatter().Deserialize(memoryStream);
            }
        }

        public override void Initialize(string name, NameValueCollection config)
        {
            _mongoCollection = new MongoClient(config["connectionString"] ?? "mongodb://localhost").GetServer().GetDatabase(config["database"] ?? "ASPNETDB").GetCollection(config["collection"] ?? "OutputCache");
            _mongoCollection.CreateIndex("Key");
            base.Initialize(name, config);
        }

        public override void Remove(string key)
        {
            _mongoCollection.Remove(Query.EQ("Key", key));
        }

        public override void Set(string key, object entry, DateTime utcExpiry)
        {
            using (var memoryStream = new MemoryStream())
            {
                new BinaryFormatter().Serialize(memoryStream, entry);

                var bsonDocument = new BsonDocument
                {
                    { "Expiration", utcExpiry },
                    { "Key", key },
                    { "Value", memoryStream.ToArray() }
                };

                _mongoCollection.Insert(bsonDocument);
            }
        }
    }
}