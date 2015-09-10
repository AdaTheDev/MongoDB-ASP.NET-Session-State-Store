using System;
using System.Collections.Specialized;
using System.Configuration;
using System.Configuration.Provider;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using System.Web;
using System.Web.Configuration;
using System.Web.Hosting;
using System.Web.SessionState;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoSessionStateStore
{
    /// <summary>
    ///     Custom ASP.NET Session State Provider using MongoDB as the state store.
    ///     For reference on this implementation see MSDN ref:
    ///     - http://msdn.microsoft.com/en-us/library/ms178587.aspx
    ///     - http://msdn.microsoft.com/en-us/library/ms178588.aspx - this sample provider was used as the basis for this
    ///     provider, with MongoDB-specific implementation swapped in, plus cosmetic changes like naming conventions.
    ///     Session state is stored in a "Sessions" collection within a "SessionState" database. Example session document:
    ///     {
    ///     "_id" : "bh54lskss4ycwpreet21dr1h",
    ///     "ApplicationName" : "/",
    ///     "Created" : ISODate("2011-04-29T21:41:41.953Z"),
    ///     "Expires" : ISODate("2011-04-29T22:01:41.953Z"),
    ///     "LockDate" : ISODate("2011-04-29T21:42:02.016Z"),
    ///     "LockId" : 1,
    ///     "Timeout" : 20,
    ///     "Locked" : true,
    ///     "SessionItems" : "AQAAAP////8EVGVzdAgAAAABBkFkcmlhbg==",
    ///     "Flags" : 0
    ///     }
    ///     Inline with the above MSDN reference:
    ///     If the provider encounters an exception when working with the data source, it writes the details of the exception
    ///     to the Application Event Log instead of returning the exception to the ASP.NET application. This is done as a
    ///     security
    ///     measure to avoid private information about the data source from being exposed in the ASP.NET application.
    ///     The sample provider specifies an event Source property value of "MongoSessionStateStore." Before your ASP.NET
    ///     application will be able to write to the Application Event Log successfully, you will need to create the following
    ///     registry key:
    ///     HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\Eventlog\Application\MongoSessionStateStore
    ///     If you do not want the sample provider to write exceptions to the event log, then you can set the custom
    ///     writeExceptionsToEventLog
    ///     attribute to false in the Web.config file.
    ///     The session-state store provider does not provide support for the Session_OnEnd event, it does not automatically
    ///     clean up expired session-item data.
    ///     You should have a job to periodically delete expired session information from the data store where Expires date is
    ///     in the past, i.e.:
    ///     db.Sessions.remove({"Expires" : {$lt : new Date() }})
    ///     Example web.config settings:
    ///     <connectionStrings>
    ///         <add name="MongoSessionServices"
    ///             connectionString="mongodb://localhost" />
    ///     </connectionStrings>
    ///     <system.web>
    ///         <sessionState
    ///             mode="Custom"
    ///             customProvider="MongoSessionStateProvider">
    ///             <providers>
    ///                 <add name="MongoSessionStateProvider"
    ///                     type="MongoSessionStateStore.MongoSessionStateStore"
    ///                     connectionStringName="MongoSessionServices"
    ///                     writeExceptionsToEventLog="false"
    ///                     fsync="false"
    ///                     replicasToWrite="0" />
    ///             </providers>
    ///         </sessionState>
    ///         ...
    ///     </system.web>
    ///     replicasToWrite setting is interpreted as the number of replicas to write to, in addition to the primary (in a
    ///     replicaset environment).
    ///     i.e. replicasToWrite = 0, will wait for the response from writing to the primary node. > 0 will wait for the
    ///     response having written to
    ///     ({replicasToWrite} + 1) nodes
    /// </summary>
    public sealed class MongoSessionStateStore : SessionStateStoreProviderBase
    {
        private const string ExceptionMessage = "An exception occurred. Please contact your administrator.";
        private const string EventSource = "MongoSessionStateStore";
        private const string EventLog = "Application";
        private SessionStateSection _config;
        private ConnectionStringSettings _connectionStringSettings;
        private IMongoDatabase _mongoDb;
        private WriteConcern _writeConcern;
        private string _connectionString;

        /// <summary>
        ///     The ApplicationName property is used to differentiate sessions
        ///     in the data source by application.
        /// </summary>
        public string ApplicationName { get; private set; }

        /// <summary>
        ///     If false, exceptions are thrown to the caller. If true,
        ///     exceptions are written to the event log.
        /// </summary>
        public bool WriteExceptionsToEventLog { get; set; }

        /// <summary>
        ///     Returns a reference to the collection in MongoDB that holds the Session state
        ///     data.
        /// </summary>
        /// <returns>MongoCollection</returns>
        private IMongoCollection<BsonDocument> GetSessionCollection()
        {
            return GetDatabase().GetCollection<BsonDocument>("Sessions").WithWriteConcern(_writeConcern);
        }

        /// <summary>
        ///     Returns a connection to the MongoDB server holding the session state data.
        /// </summary>
        /// <returns>MongoServer</returns>
        private IMongoDatabase GetDatabase()
        {
            if (_mongoDb == null)
            {
                var mc = new MongoClient(_connectionString);
                _mongoDb = mc.GetDatabase("SessionState");
            }
            return _mongoDb;
        }

        /// <summary>
        ///     Initialise the session state store.
        /// </summary>
        /// <param name="name">session state store name. Defaults to "MongoSessionStateStore" if not supplied</param>
        /// <param name="config">configuration settings</param>
        public override void Initialize(string name, NameValueCollection config)
        {
            // Initialize values from web.config.
            if (config == null)
            {
                throw new ArgumentNullException("config");
            }

            if (name.Length == 0)
            {
                name = "MongoSessionStateStore";
            }

            if (string.IsNullOrEmpty(config["description"]))
            {
                config.Remove("description");
                config.Add("description", "MongoDB Session State Store provider");
            }

            // Initialize the abstract base class.
            base.Initialize(name, config);

            // Initialize the ApplicationName property.
            ApplicationName = HostingEnvironment.ApplicationVirtualPath;

            // Get <sessionState> configuration element.
            var cfg = WebConfigurationManager.OpenWebConfiguration(ApplicationName);
            _config = (SessionStateSection) cfg.GetSection("system.web/sessionState");

           // Initialize connection string.
            _connectionStringSettings = ConfigurationManager.ConnectionStrings[config["connectionStringName"]];

            if (_connectionStringSettings == null || _connectionStringSettings.ConnectionString.Trim() == "")
            {
                throw new ProviderException("Connection string cannot be blank.");
            }

            _connectionString = _connectionStringSettings.ConnectionString;

            // Initialize WriteExceptionsToEventLog
            WriteExceptionsToEventLog = false;

            if (config["writeExceptionsToEventLog"] != null)
            {
                if (config["writeExceptionsToEventLog"].ToUpper() == "TRUE")
                {
                    WriteExceptionsToEventLog = true;
                }
            }

            // Initialise WriteConcern options. 
            // Defaults to fsynch=false, w=1 (Provides acknowledgment of write operations on a standalone mongod or the primary in a replica set.)
            // replicasToWrite config item comes into use when > 0. This translates to the WriteConcern wValue, by adding 1 to it.
            // e.g. replicasToWrite = 1 is taken as meaning "I want to wait for write operations to be acknowledged at the primary + {replicasToWrite} replicas"
            // MongoDB C# Driver references on WriteConcern : http://docs.mongodb.org/manual/core/write-operations/#write-concern
            var fsync = false;
            if (config["fsync"] != null)
            {
                if (config["fsync"].ToUpper() == "TRUE")
                {
                    fsync = true;
                }
            }

            var replicasToWrite = 1;
            if (config["replicasToWrite"] != null)
            {
                if (!int.TryParse(config["replicasToWrite"], out replicasToWrite))
                {
                    throw new ProviderException("Replicas To Write must be a valid integer");
                }
            }

            var wValue = "1";
            if (replicasToWrite > 0)
            {
                wValue = (1 + replicasToWrite).ToString(CultureInfo.InvariantCulture);
            }

            _writeConcern = new WriteConcern(WriteConcern.WValue.Parse(wValue), null, fsync, null);
        }

        public override SessionStateStoreData CreateNewStoreData(HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(),
                SessionStateUtility.GetSessionStaticObjects(context),
                timeout);
        }

        /// <summary>
        ///     SessionStateProviderBase.SetItemExpireCallback
        /// </summary>
        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

        /// <summary>
        ///     Serialize is called by the SetAndReleaseItemExclusive method to
        ///     convert the SessionStateItemCollection into a Base64 string to
        ///     be stored in MongoDB.
        /// </summary>
        private string Serialize(SessionStateItemCollection items)
        {
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    if (items != null)
                    {
                        items.Serialize(writer);
                    }

                    writer.Close();

                    return Convert.ToBase64String(ms.ToArray());
                }
            }
        }

        /// <summary>
        ///     SessionStateProviderBase.SetAndReleaseItemExclusive
        /// </summary>
        public override void SetAndReleaseItemExclusive(HttpContext context,
            string id,
            SessionStateStoreData item,
            object lockId,
            bool newItem)
        {
            // Serialize the SessionStateItemCollection as a string.
            var items = Serialize((SessionStateItemCollection) item.Items);

            var sessionCollection = GetSessionCollection();

            try
            {
                if (newItem)
                {
                    var insertDoc = new BsonDocument
                    {
                        {"_id", id},
                        {"ApplicationName", ApplicationName},
                        {"Created", DateTime.Now.ToUniversalTime()},
                        {"Expires", DateTime.Now.AddMinutes(item.Timeout).ToUniversalTime()},
                        {"LockDate", DateTime.Now.ToUniversalTime()},
                        {"LockId", 0},
                        {"Timeout", item.Timeout},
                        {"Locked", false},
                        {"SessionItems", items},
                        {"Flags", 0}
                    };


                    var query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                        Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName),
                        Builders<BsonDocument>.Filter.Lt("Expires", DateTime.Now.ToUniversalTime()));
                    sessionCollection.DeleteOneAsync(query);
                    sessionCollection.InsertOneAsync(insertDoc);
                }
                else
                {
                    var query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                        Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName),
                        Builders<BsonDocument>.Filter.Eq("LockId", (int) lockId));

                    var update = Builders<BsonDocument>.Update
                        .Set("Expires", DateTime.Now.AddMinutes(item.Timeout).ToUniversalTime())
                        .Set("SessionItems", items)
                        .Set("Locked", false);

                    sessionCollection.UpdateOneAsync(query, update);
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "SetAndReleaseItemExclusive");
                    throw new ProviderException(ExceptionMessage);
                }
                throw;
            }
        }

        /// <summary>
        ///     SessionStateProviderBase.GetItem
        /// </summary>
        public override SessionStateStoreData GetItem(HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(false, context, id, out locked,
                out lockAge, out lockId, out actionFlags);
        }

        /// <summary>
        ///     SessionStateProviderBase.GetItemExclusive
        /// </summary>
        public override SessionStateStoreData GetItemExclusive(HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actionFlags)
        {
            return GetSessionStoreItem(true, context, id, out locked,
                out lockAge, out lockId, out actionFlags);
        }

        /// <summary>
        ///     GetSessionStoreItem is called by both the GetItem and
        ///     GetItemExclusive methods. GetSessionStoreItem retrieves the
        ///     session data from the data source. If the lockRecord parameter
        ///     is true (in the case of GetItemExclusive), then GetSessionStoreItem
        ///     locks the record and sets a new LockId and LockDate.
        /// </summary>
        private SessionStateStoreData GetSessionStoreItem(bool lockRecord,
            HttpContext context,
            string id,
            out bool locked,
            out TimeSpan lockAge,
            out object lockId,
            out SessionStateActions actionFlags)
        {
            // Initial values for return value and out parameters.
            SessionStateStoreData item = null;
            lockAge = TimeSpan.Zero;
            lockId = null;
            locked = false;
            actionFlags = 0;
            var sessionCollection = GetSessionCollection();

            // DateTime to check if current session item is expired.
            // String to hold serialized SessionStateItemCollection.
            var serializedItems = "";
            // True if a record is found in the database.
            var foundRecord = false;
            // True if the returned session item is expired and needs to be deleted.
            var deleteData = false;
            // Timeout value from the data store.
            var timeout = 0;

            try
            {
                // lockRecord is true when called from GetItemExclusive and
                // false when called from GetItem.
                // Obtain a lock if possible. Ignore the record if it is expired.
                FilterDefinition<BsonDocument> query;
                if (lockRecord)
                {
                    query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                        Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName),
                        Builders<BsonDocument>.Filter.Eq("Locked", false),
                        Builders<BsonDocument>.Filter.Gt("Expires", DateTime.Now.ToUniversalTime()));


                    var update = Builders<BsonDocument>.Update
                        .Set("LockDate", DateTime.Now.ToUniversalTime())
                        .Set("Locked", true);


                    var result = sessionCollection.UpdateOneAsync(query, update).Result;

                    locked = result.ModifiedCount == 0;
                    // DocumentsAffected == 0 == No record was updated because the record was locked or not found.
                }

                // Retrieve the current session item information.
                query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                    Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName));
                var results = sessionCollection.Find(query).FirstOrDefaultAsync().Result;

                if (results != null)
                {
                    var expires = (DateTime) results["Expires"];

                    if (expires < DateTime.Now.ToUniversalTime())
                    {
                        // The record was expired. Mark it as not locked.
                        locked = false;
                        // The session was expired. Mark the data for deletion.
                        deleteData = true;
                    }
                    else
                    {
                        foundRecord = true;
                    }

                    serializedItems = results["SessionItems"].ToString();
                    lockId = results["LockId"].AsInt32;
                    lockAge = DateTime.Now.ToUniversalTime().Subtract(results["LockDate"].ToUniversalTime());
                    actionFlags = (SessionStateActions) results["Flags"].AsInt32;
                    timeout = results["Timeout"].AsInt32;
                }

                // If the returned session item is expired, 
                // delete the record from the data source.
                if (deleteData)
                {
                    query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                        Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName));
                    sessionCollection.DeleteOneAsync(query);
                }

                // The record was not found. Ensure that locked is false.
                if (!foundRecord)
                {
                    locked = false;
                }

                // If the record was found and you obtained a lock, then set 
                // the lockId, clear the actionFlags,
                // and create the SessionStateStoreItem to return.
                if (foundRecord && !locked)
                {
                    lockId = (int) lockId + 1;

                    query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                        Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName));

                    var update = Builders<BsonDocument>.Update
                        .Set("LockId", (int) lockId)
                        .Set("Flags", 0);

                    sessionCollection.UpdateOneAsync(query, update);

                    // If the actionFlags parameter is not InitializeItem, 
                    // deserialize the stored SessionStateItemCollection.
                    if (actionFlags == SessionStateActions.InitializeItem)
                    {
                        item = CreateNewStoreData(context, (int) _config.Timeout.TotalMinutes);
                    }
                    else
                    {
                        item = Deserialize(context, serializedItems, timeout);
                    }
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "GetSessionStoreItem");
                    throw new ProviderException(ExceptionMessage);
                }

                throw;
            }

            return item;
        }

        private SessionStateStoreData Deserialize(HttpContext context,
            string serializedItems, int timeout)
        {
            using (var ms =
                new MemoryStream(Convert.FromBase64String(serializedItems)))
            {
                var sessionItems =
                    new SessionStateItemCollection();

                if (ms.Length > 0)
                {
                    using (var reader = new BinaryReader(ms))
                    {
                        sessionItems = SessionStateItemCollection.Deserialize(reader);
                    }
                }

                return new SessionStateStoreData(sessionItems,
                    SessionStateUtility.GetSessionStaticObjects(context),
                    timeout);
            }
        }

        public override void CreateUninitializedItem(HttpContext context, string id, int timeout)
        {
            var sessionCollection = GetSessionCollection();
            var doc = new BsonDocument
            {
                {"_id", id},
                {"ApplicationName", ApplicationName},
                {"Created", DateTime.Now.ToUniversalTime()},
                {"Expires", DateTime.Now.AddMinutes(timeout).ToUniversalTime()},
                {"LockDate", DateTime.Now.ToUniversalTime()},
                {"LockId", 0},
                {"Timeout", timeout},
                {"Locked", false},
                {"SessionItems", ""},
                {"Flags", 1}
            };

            try
            {
                var result = sessionCollection.InsertOneAsync(doc);
                if (result.Status != TaskStatus.RanToCompletion)
                {
                    if (result.Exception != null)
                    {
                        throw new Exception(result.Exception.Message);
                    }
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "CreateUninitializedItem");
                    throw new ProviderException(ExceptionMessage);
                }

                throw;
            }
        }

        /// <summary>
        ///     This is a helper function that writes exception detail to the
        ///     event log. Exceptions are written to the event log as a security
        ///     measure to ensure private database details are not returned to
        ///     browser. If a method does not return a status or Boolean
        ///     indicating the action succeeded or failed, the caller also
        ///     throws a generic exception.
        /// </summary>
        private void WriteToEventLog(Exception e, string action)
        {
            using (var log = new EventLog())
            {
                log.Source = EventSource;
                log.Log = EventLog;

                var message =
                    string.Format(
                        "An exception occurred communicating with the data source.\n\nAction: {0}\n\nException: {1}",
                        action, e);

                log.WriteEntry(message);
            }
        }

        public override void Dispose()
        {
        }

        public override void EndRequest(HttpContext context)
        {
        }

        public override void InitializeRequest(HttpContext context)
        {
        }

        public override void ReleaseItemExclusive(HttpContext context, string id, object lockId)
        {
            var sessionCollection = GetSessionCollection();

            var query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName),
                Builders<BsonDocument>.Filter.Eq("LockId", (int) lockId));

            var update = Builders<BsonDocument>.Update
                .Set("Expires", DateTime.Now.AddMinutes(_config.Timeout.TotalMinutes).ToUniversalTime())
                .Set("Locked", false);

            try
            {
                sessionCollection.UpdateOneAsync(query, update);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ReleaseItemExclusive");
                    throw new ProviderException(ExceptionMessage);
                }
                throw;
            }
        }

        public override void RemoveItem(HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            var sessionCollection = GetSessionCollection();

            var query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName),
                Builders<BsonDocument>.Filter.Eq("LockId", (int) lockId));

            try
            {
                sessionCollection.DeleteOneAsync(query);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "RemoveItem");
                    throw new ProviderException(ExceptionMessage);
                }

                throw;
            }
        }

        public override void ResetItemTimeout(HttpContext context, string id)
        {
            var sessionCollection = GetSessionCollection();
            var query = Builders<BsonDocument>.Filter.And(Builders<BsonDocument>.Filter.Eq("_id", id),
                Builders<BsonDocument>.Filter.Eq("ApplicationName", ApplicationName));
            var update = Builders<BsonDocument>.Update.Set("Expires",
                DateTime.Now.AddMinutes(_config.Timeout.TotalMinutes).ToUniversalTime());

            try
            {
                sessionCollection.UpdateOneAsync(query, update);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ResetItemTimeout");
                    throw new ProviderException(ExceptionMessage);
                }
                throw;
            }
        }
    }
}