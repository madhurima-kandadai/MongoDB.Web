using System;
using System.Collections.Specialized;
using System.Configuration;
using System.IO;
using System.Web;
using System.Web.Configuration;
using System.Web.Hosting;
using System.Web.SessionState;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;

namespace MongoDB.Web.Providers
{
  /// <summary>
  /// Class MongoDBSessionStateProvider.
  /// </summary>
  public class MongoDBSessionStateProvider : SessionStateStoreProviderBase
  {
    private MongoCollection _mongoCollection;
    private SessionStateSection _sessionStateSection;

    /// <summary>
    /// Creates a new <see cref="T:System.Web.SessionState.SessionStateStoreData" /> object to be used for the current request.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="timeout">The session-state <see cref="P:System.Web.SessionState.HttpSessionState.Timeout" /> value for the new <see cref="T:System.Web.SessionState.SessionStateStoreData" />.</param>
    /// <returns>A new <see cref="T:System.Web.SessionState.SessionStateStoreData" /> for the current request.</returns>
    public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
    {
      return new SessionStateStoreData(new SessionStateItemCollection(), SessionStateUtility.GetSessionStaticObjects(context), timeout);
    }

    /// <summary>
    /// Adds a new session-state item to the data store.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="id">The <see cref="P:System.Web.SessionState.HttpSessionState.SessionID" /> for the current request.</param>
    /// <param name="timeout">The session <see cref="P:System.Web.SessionState.HttpSessionState.Timeout" /> for the current request.</param>
    public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
    {
      var memoryStream = new MemoryStream();

      var query = Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id));
      _mongoCollection.Remove(query);

      var bsonDocument = new BsonDocument
        {
          { "applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath },
          { "created", DateTime.Now },
          { "expires", DateTime.Now.AddMinutes(1400) },
          { "id", id },
          { "lockDate", DateTime.Now },
          { "locked", false },
          { "lockId", 0 },
          { "sessionStateActions", SessionStateActions.None },
          { "sessionStateItems", memoryStream.ToArray() },
          { "sessionStateItemsCount", 0 },
          { "timeout", 20 }
        };

      _mongoCollection.Insert(bsonDocument);
    }

    /// <summary>
    /// Releases all resources used by the <see cref="T:System.Web.SessionState.SessionStateStoreProviderBase" /> implementation.
    /// </summary>
    public override void Dispose()
    {
    }

    /// <summary>
    /// Called by the <see cref="T:System.Web.SessionState.SessionStateModule" /> object at the end of a request.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    public override void EndRequest(HttpContext context)
    {
    }

    /// <summary>
    /// Returns read-only session-state data from the session data store.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="id">The <see cref="P:System.Web.SessionState.HttpSessionState.SessionID" /> for the current request.</param>
    /// <param name="locked">When this method returns, contains a Boolean value that is set to true if the requested session item is locked at the session data store; otherwise, false.</param>
    /// <param name="lockAge">When this method returns, contains a <see cref="T:System.TimeSpan" /> object that is set to the amount of time that an item in the session data store has been locked.</param>
    /// <param name="lockId">When this method returns, contains an object that is set to the lock identifier for the current request. For details on the lock identifier, see "Locking Session-Store Data" in the <see cref="T:System.Web.SessionState.SessionStateStoreProviderBase" /> class summary.</param>
    /// <param name="actions">When this method returns, contains one of the <see cref="T:System.Web.SessionState.SessionStateActions" /> values, indicating whether the current session is an uninitialized, cookieless session.</param>
    /// <returns>A <see cref="T:System.Web.SessionState.SessionStateStoreData" /> populated with session values and information from the session data store.</returns>
    public override SessionStateStoreData GetItem(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
    {
      return GetSessionStateStoreData(false, context, id, out locked, out lockAge, out lockId, out actions);
    }

    /// <summary>
    /// Returns read-only session-state data from the session data store.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="id">The <see cref="P:System.Web.SessionState.HttpSessionState.SessionID" /> for the current request.</param>
    /// <param name="locked">When this method returns, contains a Boolean value that is set to true if a lock is successfully obtained; otherwise, false.</param>
    /// <param name="lockAge">When this method returns, contains a <see cref="T:System.TimeSpan" /> object that is set to the amount of time that an item in the session data store has been locked.</param>
    /// <param name="lockId">When this method returns, contains an object that is set to the lock identifier for the current request. For details on the lock identifier, see "Locking Session-Store Data" in the <see cref="T:System.Web.SessionState.SessionStateStoreProviderBase" /> class summary.</param>
    /// <param name="actions">When this method returns, contains one of the <see cref="T:System.Web.SessionState.SessionStateActions" /> values, indicating whether the current session is an uninitialized, cookieless session.</param>
    /// <returns>A <see cref="T:System.Web.SessionState.SessionStateStoreData" /> populated with session values and information from the session data store.</returns>
    public override SessionStateStoreData GetItemExclusive(HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
    {
      return GetSessionStateStoreData(true, context, id, out locked, out lockAge, out lockId, out actions);
    }

    /// <summary>
    /// Initializes the provider.
    /// </summary>
    /// <param name="name">The friendly name of the provider.</param>
    /// <param name="config">A collection of the name/value pairs representing the provider-specific attributes specified in the configuration for this provider.</param>
    /// <exception cref="System.Configuration.ConfigurationErrorsException">
    /// Both parmeters connectionString and connectionStringName can not be specified
    /// or
    /// Either connectionString or connectionStringName parameter must be specified
    /// or
    /// </exception>
    public override void Initialize(string name, NameValueCollection config)
    {
      var configuration = WebConfigurationManager.OpenWebConfiguration(HostingEnvironment.ApplicationVirtualPath);
      _sessionStateSection = configuration.GetSection("system.web/sessionState") as SessionStateSection;

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

      _mongoCollection = new MongoClient(connString).GetServer().GetDatabase(config["database"] ?? "ASPNETDB").GetCollection(config["collection"] ?? "SessionState");
      _mongoCollection.CreateIndex("applicationVirtualPath", "id");
      _mongoCollection.CreateIndex("applicationVirtualPath", "id", "lockId");

      base.Initialize(name, config);
    }

    /// <summary>
    /// Called by the <see cref="T:System.Web.SessionState.SessionStateModule" /> object for per-request initialization.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    public override void InitializeRequest(HttpContext context)
    {
    }

    /// <summary>
    /// Releases a lock on an item in the session data store.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="id">The session identifier for the current request.</param>
    /// <param name="lockId">The lock identifier for the current request.</param>
    public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
    {
      var query = Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id), Query.EQ("lockId", lockId.ToString()));
      var update = Update.Set("expires", DateTime.Now.Add(_sessionStateSection.Timeout)).Set("locked", false);
      _mongoCollection.Update(query, update);
    }

    /// <summary>
    /// Deletes item data from the session data store.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="id">The session identifier for the current request.</param>
    /// <param name="lockId">The lock identifier for the current request.</param>
    /// <param name="item">The <see cref="T:System.Web.SessionState.SessionStateStoreData" /> that represents the item to delete from the data store.</param>
    public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
    {
      var query = Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id), Query.EQ("lockId", lockId.ToString()));
      _mongoCollection.Remove(query);
    }

    /// <summary>
    /// Updates the expiration date and time of an item in the session data store.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="id">The session identifier for the current request.</param>
    public override void ResetItemTimeout(HttpContext context, string id)
    {
      var query = Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id));
      var update = Update.Set("expires", DateTime.Now.Add(_sessionStateSection.Timeout));
      _mongoCollection.Update(query, update);
    }

    /// <summary>
    /// Updates the session-item information in the session-state data store with values from the current request, and clears the lock on the data.
    /// </summary>
    /// <param name="context">The <see cref="T:System.Web.HttpContext" /> for the current request.</param>
    /// <param name="id">The session identifier for the current request.</param>
    /// <param name="item">The <see cref="T:System.Web.SessionState.SessionStateStoreData" /> object that contains the current session values to be stored.</param>
    /// <param name="lockId">The lock identifier for the current request.</param>
    /// <param name="newItem">true to identify the session item as a new item; false to identify the session item as an existing item.</param>
    public override void SetAndReleaseItemExclusive(HttpContext context, string id, SessionStateStoreData item, object lockId, bool newItem)
    {
      var memoryStream = new MemoryStream();

      using (var binaryWriter = new BinaryWriter(memoryStream))
      {
        ((SessionStateItemCollection)item.Items).Serialize(binaryWriter);

        if (newItem)
        {
          var query = Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id));
          _mongoCollection.Remove(query);

          var bsonDocument = new BsonDocument
                    {
                        { "applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath },
                        { "created", DateTime.Now },
                        { "expires", DateTime.Now.AddMinutes(item.Timeout) },
                        { "id", id },
                        { "lockDate", DateTime.Now },
                        { "locked", false },
                        { "lockId", 0 },
                        { "sessionStateActions", SessionStateActions.None },
                        { "sessionStateItems", memoryStream.ToArray() },
                        { "sessionStateItemsCount", item.Items.Count },
                        { "timeout", item.Timeout }
                    };

          _mongoCollection.Insert(bsonDocument);
        }
        else
        {
          var query = Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id), Query.EQ("lockId", lockId.ToString()));
          var upate = Update.Set("expires", DateTime.Now.Add(_sessionStateSection.Timeout)).Set("items", memoryStream.ToArray()).Set("locked", false).Set("sessionStateItemsCount", item.Items.Count);
          _mongoCollection.Update(query, upate);
        }
      }
    }

    /// <summary>
    /// Sets a reference to the <see cref="T:System.Web.SessionState.SessionStateItemExpireCallback" /> delegate for the Session_OnEnd event defined in the Global.asax file.
    /// </summary>
    /// <param name="expireCallback">The <see cref="T:System.Web.SessionState.SessionStateItemExpireCallback" />  delegate for the Session_OnEnd event defined in the Global.asax file.</param>
    /// <returns>true if the session-state store provider supports calling the Session_OnEnd event; otherwise, false.</returns>
    public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
    {
      return false;
    }

    #region Private Methods

    /// <summary>
    /// Gets the session state store data.
    /// </summary>
    /// <param name="exclusive">if set to <c>true</c> [exclusive].</param>
    /// <param name="context">The context.</param>
    /// <param name="id">The identifier.</param>
    /// <param name="locked">if set to <c>true</c> [locked].</param>
    /// <param name="lockAge">The lock age.</param>
    /// <param name="lockId">The lock identifier.</param>
    /// <param name="actions">The actions.</param>
    /// <returns>SessionStateStoreData.</returns>
    private SessionStateStoreData GetSessionStateStoreData(bool exclusive, HttpContext context, string id, out bool locked, out TimeSpan lockAge, out object lockId, out SessionStateActions actions)
    {
      actions = SessionStateActions.None;
      lockAge = TimeSpan.Zero;
      lockId = 0;

      var query = Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id));
      var bsonDocument = _mongoCollection.FindOneAs<BsonDocument>(query);

      if (bsonDocument == null)
      {
        locked = false;
      }
      else if (bsonDocument["expires"].ToUniversalTime() <= DateTime.Now)
      {
        locked = false;
        _mongoCollection.Remove(Query.And(Query.EQ("applicationVirtualPath", HostingEnvironment.ApplicationVirtualPath), Query.EQ("id", id)));
      }
      else if (bsonDocument["locked"].AsBoolean)
      {
        lockAge = DateTime.Now.Subtract(bsonDocument["lockDate"].ToUniversalTime());
        locked = true;
        lockId = bsonDocument["lockId"].AsInt32;
      }
      else
      {
        locked = false;
        lockId = bsonDocument["lockId"].AsInt32;
        actions = (SessionStateActions)bsonDocument["sessionStateActions"].AsInt32;
      }

      if (exclusive && bsonDocument != null)
      {
        lockId = (int)lockId + 1;
        actions = SessionStateActions.None;

        var update = Update.Set("lockDate", DateTime.Now).Set("lockId", (int)lockId).Set("locked", true).Set("sessionStateActions", SessionStateActions.None);
        _mongoCollection.Update(query, update);
      }

      if (actions == SessionStateActions.InitializeItem)
      {
        return CreateNewStoreData(context, _sessionStateSection.Timeout.Minutes);
      }

      if (bsonDocument != null)
      {
        using (var memoryStream = new MemoryStream(bsonDocument["sessionStateItems"].AsByteArray))
        {
          var sessionStateItems = new SessionStateItemCollection();

          if (memoryStream.Length > 0)
          {
            var binaryReader = new BinaryReader(memoryStream);
            sessionStateItems = SessionStateItemCollection.Deserialize(binaryReader);
          }

          return new SessionStateStoreData(sessionStateItems, 
            SessionStateUtility.GetSessionStaticObjects(context), bsonDocument["timeout"].AsInt32);
        }
      }

      return null;
    }

    #endregion
  }
}
