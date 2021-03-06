﻿using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Configuration;
using System.Linq;
using System.Web.Hosting;
using System.Web.Profile;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace MongoDB.Web.Providers
{
  public class MongoDBProfileProvider : ProfileProvider
  {
    private MongoCollection mongoCollection;

    public override string ApplicationName { get; set; }

    public override int DeleteInactiveProfiles(ProfileAuthenticationOption authenticationOption, DateTime userInactiveSinceDate)
    {
      var query = Query.And(Query.EQ("ApplicationName", ApplicationName), Query.LTE("LastActivityDate", userInactiveSinceDate));

      if (authenticationOption != ProfileAuthenticationOption.All)
      {
        query = Query.And(query, Query.EQ("IsAnonymous", authenticationOption == ProfileAuthenticationOption.Anonymous));
      }

      return (int)mongoCollection.Remove(query).DocumentsAffected;
    }

    public override int DeleteProfiles(string[] usernames)
    {
      var query = Query.And(Query.EQ("ApplicationName", ApplicationName), Query.In("Username", new BsonArray(usernames)));
      return (int)mongoCollection.Remove(query).DocumentsAffected;
    }

    public override int DeleteProfiles(ProfileInfoCollection profiles)
    {
      return DeleteProfiles(profiles.Cast<ProfileInfo>().Select(profile => profile.UserName).ToArray());
    }

    public override ProfileInfoCollection FindInactiveProfilesByUserName(ProfileAuthenticationOption authenticationOption, string usernameToMatch, DateTime userInactiveSinceDate, int pageIndex, int pageSize, out int totalRecords)
    {
      return GetProfiles(authenticationOption, usernameToMatch, userInactiveSinceDate, pageIndex, pageSize, out totalRecords);
    }

    public override ProfileInfoCollection FindProfilesByUserName(ProfileAuthenticationOption authenticationOption, string usernameToMatch, int pageIndex, int pageSize, out int totalRecords)
    {
      return GetProfiles(authenticationOption, usernameToMatch, null, pageIndex, pageSize, out totalRecords);
    }

    public override ProfileInfoCollection GetAllInactiveProfiles(ProfileAuthenticationOption authenticationOption, DateTime userInactiveSinceDate, int pageIndex, int pageSize, out int totalRecords)
    {
      return GetProfiles(authenticationOption, null, userInactiveSinceDate, pageIndex, pageSize, out totalRecords);
    }

    public override ProfileInfoCollection GetAllProfiles(ProfileAuthenticationOption authenticationOption, int pageIndex, int pageSize, out int totalRecords)
    {
      return GetProfiles(authenticationOption, null, null, pageIndex, pageSize, out totalRecords);
    }

    public override int GetNumberOfInactiveProfiles(ProfileAuthenticationOption authenticationOption, DateTime userInactiveSinceDate)
    {
      var query = GetQuery(authenticationOption, null, userInactiveSinceDate);
      return (int)mongoCollection.Count(query);
    }

    public override SettingsPropertyValueCollection GetPropertyValues(SettingsContext context, SettingsPropertyCollection collection)
    {
      var settingsPropertyValueCollection = new SettingsPropertyValueCollection();

      if (collection.Count < 1)
      {
        return settingsPropertyValueCollection;
      }

      var username = (string)context["UserName"];

      if (string.IsNullOrWhiteSpace(username))
      {
        return settingsPropertyValueCollection;
      }

      var query = Query.And(Query.EQ("ApplicationName", ApplicationName), Query.EQ("Username", username));
      var bsonDocument = mongoCollection.FindOneAs<BsonDocument>(query);

      if (bsonDocument == null)
      {
        return settingsPropertyValueCollection;
      }

      foreach (SettingsProperty settingsProperty in collection)
      {
        var settingsPropertyValue = new SettingsPropertyValue(settingsProperty);
        settingsPropertyValueCollection.Add(settingsPropertyValue);

        if (!bsonDocument.Contains(settingsPropertyValue.Name))
        {
          continue;
        }

        var value = BsonTypeMapper.MapToDotNetValue(bsonDocument[settingsPropertyValue.Name]);
        if (value == null)
        {
          continue;
        }
        settingsPropertyValue.PropertyValue = value;
        settingsPropertyValue.IsDirty = false;
        settingsPropertyValue.Deserialized = true;
      }

      var update = Update.Set("LastActivityDate", DateTime.Now);
      mongoCollection.Update(query, update);

      return settingsPropertyValueCollection;
    }

    public override void Initialize(string name, NameValueCollection config)
    {
      ApplicationName = config["applicationName"] ?? HostingEnvironment.ApplicationVirtualPath;

      var connString = config["connectionString"];
      var connStringName = config["connectionStringName"];
      if ((connString != null) && (connStringName != null))
      {
        throw new ConfigurationErrorsException("Both parmeters connectionString and connectionStringName can not be specified");
      }
      if (connString == null)
      {
        if (connStringName == null)
        {
          throw new ConfigurationErrorsException("Either connectionString or connectionStringName parameter must be specified");
        }
        var settings = ConfigurationManager.ConnectionStrings[connStringName];
        if (settings == null)
        {
          throw new ConfigurationErrorsException(string.Format("Connection string {0} not found", connStringName));
        }
        connString = settings.ConnectionString;
      }
      mongoCollection = new MongoClient(connString).GetServer().GetDatabase(config["database"] ?? "ASPNETDB").GetCollection(config["collection"] ?? "Profiles");
      mongoCollection.CreateIndex("ApplicationName");
      mongoCollection.CreateIndex("ApplicationName", "IsAnonymous");
      mongoCollection.CreateIndex("ApplicationName", "IsAnonymous", "LastActivityDate");
      mongoCollection.CreateIndex("ApplicationName", "IsAnonymous", "LastActivityDate", "Username");
      mongoCollection.CreateIndex("ApplicationName", "IsAnonymous", "Username");
      mongoCollection.CreateIndex("ApplicationName", "LastActivityDate");
      mongoCollection.CreateIndex("ApplicationName", "Username");
      mongoCollection.CreateIndex("ApplicationName", "Username", "IsAnonymous");

      base.Initialize(name, config);
    }

    public override void SetPropertyValues(SettingsContext context, SettingsPropertyValueCollection collection)
    {
      var username = (string)context["UserName"];
      var isAuthenticated = (bool)context["IsAuthenticated"];

      if (string.IsNullOrWhiteSpace(username) || collection.Count < 1)
      {
        return;
      }

      var values = new Dictionary<string, object>();

      foreach (SettingsPropertyValue settingsPropertyValue in collection)
      {
        if (!settingsPropertyValue.IsDirty)
        {
          continue;
        }

        if (!isAuthenticated && !(bool)settingsPropertyValue.Property.Attributes["AllowAnonymous"])
        {
          continue;
        }

        values.Add(settingsPropertyValue.Name, settingsPropertyValue.PropertyValue);
      }

      var query = Query.And(Query.EQ("ApplicationName", ApplicationName), Query.EQ("Username", username));
      var bsonDocument = mongoCollection.FindOneAs<BsonDocument>(query) ?? new BsonDocument
            {
              { "ApplicationName", ApplicationName },
              { "Username", username }
            };

      var mergeDocument = new BsonDocument
            {
                { "LastActivityDate", DateTime.Now },
                { "LastUpdatedDate", DateTime.Now }
            };

      mergeDocument.AddRange(values as IDictionary<string, object>);
      bsonDocument.Merge(mergeDocument);

      mongoCollection.Save(bsonDocument);
    }

    #region Private Methods

    private static ProfileInfo ToProfileInfo(BsonDocument bsonDocument)
    {
      return new ProfileInfo(bsonDocument["Username"].AsString, bsonDocument["IsAnonymous"].AsBoolean, bsonDocument["LastActivityDate"].ToUniversalTime(), bsonDocument["LastUpdatedDate"].ToUniversalTime(), 0);
    }

    private ProfileInfoCollection GetProfiles(ProfileAuthenticationOption authenticationOption, string usernameToMatch, DateTime? userInactiveSinceDate, int pageIndex, int pageSize, out int totalRecords)
    {
      var query = GetQuery(authenticationOption, usernameToMatch, userInactiveSinceDate);

      totalRecords = (int)mongoCollection.Count(query);

      var profileInfoCollection = new ProfileInfoCollection();

      foreach (var bsonDocument in mongoCollection.FindAs<BsonDocument>(query).SetSkip(pageIndex * pageSize).SetLimit(pageSize))
      {
        profileInfoCollection.Add(ToProfileInfo(bsonDocument));
      }

      return profileInfoCollection;
    }

    private IMongoQuery GetQuery(ProfileAuthenticationOption authenticationOption, string usernameToMatch, DateTime? userInactiveSinceDate)
    {
      var query = Query.EQ("ApplicationName", ApplicationName);

      if (authenticationOption != ProfileAuthenticationOption.All)
      {
        query = Query.And(query, Query.EQ("IsAnonymous", authenticationOption == ProfileAuthenticationOption.Anonymous));
      }

      if (!string.IsNullOrWhiteSpace(usernameToMatch))
      {
        query = Query.And(query, Query.Matches("Username", usernameToMatch));
      }

      if (userInactiveSinceDate.HasValue)
      {
        query = Query.And(query, Query.LTE("LastActivityDate", userInactiveSinceDate));
      }

      return query;
    }

    #endregion
  }
}