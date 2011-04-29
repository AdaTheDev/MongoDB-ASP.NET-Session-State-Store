using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web.SessionState;
using System.Configuration;
using System.Configuration.Provider;
using System.Web.Configuration;
using MongoDB.Driver;
using MongoDB.Bson;
using System.Diagnostics;
using System.IO;
using MongoDB.Driver.Builders;
using System.Web;

namespace MongoSessionStateStore
{
    /// <summary>
    /// Custom ASP.NET Session State Provider using MongoDB as the state store.
    /// For reference on this implementation see MSDN ref:
    ///     - http://msdn.microsoft.com/en-us/library/ms178587.aspx
    ///     - http://msdn.microsoft.com/en-us/library/ms178588.aspx - this sample provider was used as the basis for this
    ///       provider, with MongoDB-specific implementation swapped in, plus cosmetic changes like naming conventions.
    /// 
    /// Session state is stored in a "Sessions" collection within a "SessionState" database. Example session document:
    /// {
    ///    "_id" : "bh54lskss4ycwpreet21dr1h",
    ///    "ApplicationName" : "/",
    ///    "Created" : ISODate("2011-04-29T21:41:41.953Z"),
    ///    "Expires" : ISODate("2011-04-29T22:01:41.953Z"),
    ///    "LockDate" : ISODate("2011-04-29T21:42:02.016Z"),
    ///    "LockId" : 1,
    ///    "Timeout" : 20,
    ///    "Locked" : true,
    ///    "SessionItems" : "AQAAAP////8EVGVzdAgAAAABBkFkcmlhbg==",
    ///    "Flags" : 0
    /// }
    /// 
    /// Inline with the above MSDN reference:
    /// If the provider encounters an exception when working with the data source, it writes the details of the exception 
    /// to the Application Event Log instead of returning the exception to the ASP.NET application. This is done as a security 
    /// measure to avoid private information about the data source from being exposed in the ASP.NET application.
    /// The sample provider specifies an event Source property value of "MongoSessionStateStore." Before your ASP.NET 
    /// application will be able to write to the Application Event Log successfully, you will need to create the following registry key:
    ///     HKEY_LOCAL_MACHINE\SYSTEM\CurrentControlSet\Services\Eventlog\Application\MongoSessionStateStore
    /// If you do not want the sample provider to write exceptions to the event log, then you can set the custom writeExceptionsToEventLog 
    /// attribute to false in the Web.config file.
    ///
    /// The session-state store provider does not provide support for the Session_OnEnd event, it does not automatically clean up expired session-item data. 
    /// You should have a job to periodically delete expired session information from the data store where Expires date is in the past, i.e.:
    ///     db.Sessions.remove({"Expires" : {$lt : new Date() }})
    /// 
    /// Example web.config settings:
    ///  
    ///  <connectionStrings>
    ///     <add name="MongoSessionServices"
    ///        connectionString="mongodb://localhost" />
    ///  </connectionStrings>
    ///  <system.web>
    ///     <sessionState
    ///         mode="Custom"
    ///         customProvider="MongoSessionStateProvider">
    ///             <providers>
    ///                 <add name="MongoSessionStateProvider"
    ///                     type="MongoSessionStateStore.MongoSessionStateStore"
    ///                     connectionStringName="MongoSessionServices"
    ///                     writeExceptionsToEventLog="false"
    ///                     fsync="false"
    ///                     replicasToWrite="0" />
    ///             </providers>
    ///     </sessionState>
    ///     ...
    /// </system.web>
    /// </summary>
    public sealed class MongoSessionStateStore : SessionStateStoreProviderBase
    {
        private SessionStateSection _config = null;
        private ConnectionStringSettings _connectionStringSettings;
        private string _applicationName;
        private string _connectionString;
        private bool _writeExceptionsToEventLog;
        private const string _exceptionMessage = "An exception occurred. Please contact your administrator.";
        private const string _eventSource = "MongoSessionStateStore";
        private const string _eventLog = "Application";
        private SafeMode _safeMode = null;

        /// <summary>
        /// The ApplicationName property is used to differentiate sessions
        /// in the data source by application.
        ///</summary>
        public string ApplicationName
        {
            get { return _applicationName; }
        }
       
        /// <summary>
        /// If false, exceptions are thrown to the caller. If true,
        /// exceptions are written to the event log. 
        /// </summary>
        public bool WriteExceptionsToEventLog
        {
            get { return _writeExceptionsToEventLog; }
            set { _writeExceptionsToEventLog = value; }
        }

        /// <summary>
        /// Returns a reference to the collection in MongoDB that holds the Session state
        /// data.
        /// </summary>
        /// <param name="conn">MongoDB server connection</param>
        /// <returns>MongoCollection</returns>
        private MongoCollection<BsonDocument> GetSessionCollection(MongoServer conn)
        {
            return conn.GetDatabase("SessionState").GetCollection("Sessions");
        }

        /// <summary>
        /// Returns a connection to the MongoDB server holding the session state data.
        /// </summary>
        /// <returns>MongoServer</returns>
        private MongoServer GetConnection()
        {
            return MongoServer.Create(_connectionString);
        }

        /// <summary>
        /// Initialise the session state store.
        /// </summary>
        /// <param name="name">session state store name. Defaults to "MongoSessionStateStore" if not supplied</param>
        /// <param name="config">configuration settings</param>
        public override void Initialize(string name, System.Collections.Specialized.NameValueCollection config)
        {   
            // Initialize values from web.config.
            if (config == null)
                throw new ArgumentNullException("config");

            if (name == null || name.Length == 0)
                name = "MongoSessionStateStore";

            if (String.IsNullOrEmpty(config["description"]))
            {
                config.Remove("description");
                config.Add("description", "MongoDB Session State Store provider");
            }

            // Initialize the abstract base class.
            base.Initialize(name, config);

            // Initialize the ApplicationName property.
            _applicationName = System.Web.Hosting.HostingEnvironment.ApplicationVirtualPath;

            // Get <sessionState> configuration element.
            Configuration cfg = WebConfigurationManager.OpenWebConfiguration(ApplicationName);
            _config = (SessionStateSection)cfg.GetSection("system.web/sessionState");

            // Initialize connection string.
            _connectionStringSettings = ConfigurationManager.ConnectionStrings[config["connectionStringName"]];

            if (_connectionStringSettings == null || _connectionStringSettings.ConnectionString.Trim() == "")
            {
                throw new ProviderException("Connection string cannot be blank.");
            }

            _connectionString = _connectionStringSettings.ConnectionString;            

            // Initialize WriteExceptionsToEventLog
            _writeExceptionsToEventLog = false;

            if (config["writeExceptionsToEventLog"] != null)
            {
                if (config["writeExceptionsToEventLog"].ToUpper() == "TRUE")
                    _writeExceptionsToEventLog = true;
            }

            // Initialise safe mode options. Defaults to Safe Mode=true, fsynch=false, w=0 (replicas to write to before returning)
            bool safeModeEnabled = true;            
            
            bool fsync = false;
            if (config["fsync"] != null)
            {
                if (config["fsync"].ToUpper() == "TRUE")
                    fsync = true;
            }

            int replicasToWrite = 0;
            if (config["replicasToWrite"] != null)
            {
                if (!int.TryParse(config["replicasToWrite"], out replicasToWrite))
                    throw new ProviderException("Replicas To Write must be a valid integer");
            }
            
            _safeMode = SafeMode.Create(safeModeEnabled, fsync, replicasToWrite);
        }
        
        public override SessionStateStoreData CreateNewStoreData(System.Web.HttpContext context, int timeout)
        {
            return new SessionStateStoreData(new SessionStateItemCollection(),
                SessionStateUtility.GetSessionStaticObjects(context),
                timeout);
        }

        /// <summary>
        /// SessionStateProviderBase.SetItemExpireCallback
        /// </summary>
        public override bool SetItemExpireCallback(SessionStateItemExpireCallback expireCallback)
        {
            return false;
        }

        /// <summary>
        /// Serialize is called by the SetAndReleaseItemExclusive method to 
        /// convert the SessionStateItemCollection into a Base64 string to    
        /// be stored in MongoDB.
        /// </summary>
        private string Serialize(SessionStateItemCollection items)
        {
            using(MemoryStream ms = new MemoryStream())
            using (BinaryWriter writer = new BinaryWriter(ms))
            {
                if (items != null)
                    items.Serialize(writer);

                writer.Close();

                return Convert.ToBase64String(ms.ToArray());
            }
        }

        /// <summary>
        /// SessionStateProviderBase.SetAndReleaseItemExclusive
        /// </summary>
        public override void SetAndReleaseItemExclusive(HttpContext context,
          string id,
          SessionStateStoreData item,
          object lockId,
          bool newItem)
        {
            // Serialize the SessionStateItemCollection as a string.
            string sessItems = Serialize((SessionStateItemCollection)item.Items);

            MongoServer conn = GetConnection();
            MongoCollection sessionCollection = GetSessionCollection(conn);                        
            BsonDocument insertDoc = null;

            try
            {
                if (newItem)
                {
                    insertDoc = new BsonDocument();
                    insertDoc.Add("_id", id);
                    insertDoc.Add("ApplicationName", ApplicationName);
                    insertDoc.Add("Created", DateTime.Now.ToUniversalTime());
                    insertDoc.Add("Expires", DateTime.Now.AddMinutes((Double)item.Timeout).ToUniversalTime());
                    insertDoc.Add("LockDate", DateTime.Now.ToUniversalTime());
                    insertDoc.Add("LockId", 0);
                    insertDoc.Add("Timeout", item.Timeout);
                    insertDoc.Add("Locked", false);
                    insertDoc.Add("SessionItems", sessItems);
                    insertDoc.Add("Flags", 0);                

                    var query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName), Query.LT("Expires", DateTime.Now.ToUniversalTime()));
                    sessionCollection.Remove(query, _safeMode); 
                    sessionCollection.Insert(insertDoc, _safeMode);
                }
                else
                {
                    var query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName), Query.EQ("LockId", (Int32)lockId));
                    var update = Update.Set("Expires", DateTime.Now.AddMinutes((Double)item.Timeout).ToUniversalTime());
                    update.Set("SessionItems", sessItems);
                    update.Set("Locked", false);
                    sessionCollection.Update(query, update, _safeMode);
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "SetAndReleaseItemExclusive");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Disconnect();
            }
        }        

        /// <summary>
        /// SessionStateProviderBase.GetItem
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
        /// SessionStateProviderBase.GetItemExclusive
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
        /// GetSessionStoreItem is called by both the GetItem and 
        /// GetItemExclusive methods. GetSessionStoreItem retrieves the 
        /// session data from the data source. If the lockRecord parameter
        /// is true (in the case of GetItemExclusive), then GetSessionStoreItem
        /// locks the record and sets a new LockId and LockDate.
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

            MongoServer conn = GetConnection();
            MongoCollection sessionCollection = GetSessionCollection(conn);
            
            // DateTime to check if current session item is expired.
            DateTime expires;
            // String to hold serialized SessionStateItemCollection.
            string serializedItems = "";
            // True if a record is found in the database.
            bool foundRecord = false;
            // True if the returned session item is expired and needs to be deleted.
            bool deleteData = false;
            // Timeout value from the data store.
            int timeout = 0;
            QueryComplete query;
            try
            {                
                // lockRecord is true when called from GetItemExclusive and
                // false when called from GetItem.
                // Obtain a lock if possible. Ignore the record if it is expired.
                if (lockRecord)
                {
                    query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName), Query.EQ("Locked", false), Query.GT("Expires", DateTime.Now.ToUniversalTime()));
                    var update = Update.Set("Locked", true);
                    update.Set("LockDate", DateTime.Now.ToUniversalTime());
                    var result = sessionCollection.Update(query, update, _safeMode);
                    
                    if (result.DocumentsAffected == 0)
                    {
                        // No record was updated because the record was locked or not found.
                        locked = true;
                    }
                    else
                    {
                        // The record was updated.
                        locked = false;
                    }
                }

                // Retrieve the current session item information.
                query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName",ApplicationName));
                var results = sessionCollection.FindOneAs<BsonDocument>(query);
                
                if (results != null)
                {
                    expires = results["Expires"].AsDateTime;

                    if (expires < DateTime.Now.ToUniversalTime())
                    {
                        // The record was expired. Mark it as not locked.
                        locked = false;
                        // The session was expired. Mark the data for deletion.
                        deleteData = true;
                    }
                    else
                        foundRecord = true;

                    serializedItems = results["SessionItems"].AsString;
                    lockId = results["LockId"].AsInt32;
                    lockAge = DateTime.Now.ToUniversalTime().Subtract(results["LockDate"].AsDateTime);
                    actionFlags = (SessionStateActions)results["Flags"].AsInt32;
                    timeout = results["Timeout"].AsInt32;
                }                

                // If the returned session item is expired, 
                // delete the record from the data source.
                if (deleteData)
                {
                    query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName));
                    sessionCollection.Remove(query, _safeMode);
                }

                // The record was not found. Ensure that locked is false.
                if (!foundRecord)
                    locked = false;

                // If the record was found and you obtained a lock, then set 
                // the lockId, clear the actionFlags,
                // and create the SessionStateStoreItem to return.
                if (foundRecord && !locked)
                {
                    lockId = (int)lockId + 1;

                    query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName));
                    var update = Update.Set("LockId", (int)lockId);
                    update.Set("Flags", 0);
                    sessionCollection.Update(query, update, _safeMode);                    

                    // If the actionFlags parameter is not InitializeItem, 
                    // deserialize the stored SessionStateItemCollection.
                    if (actionFlags == SessionStateActions.InitializeItem)
                        item = CreateNewStoreData(context, (int)_config.Timeout.TotalMinutes);
                    else
                        item = Deserialize(context, serializedItems, timeout);
                }
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "GetSessionStoreItem");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Disconnect();
            }

            return item;
        }

        private SessionStateStoreData Deserialize(HttpContext context,
         string serializedItems, int timeout)
        {
            using (MemoryStream ms =
              new MemoryStream(Convert.FromBase64String(serializedItems)))
            {

                SessionStateItemCollection sessionItems =
                  new SessionStateItemCollection();

                if (ms.Length > 0)
                {
                    using (BinaryReader reader = new BinaryReader(ms))
                    {
                        sessionItems = SessionStateItemCollection.Deserialize(reader);
                    }
                }

                return new SessionStateStoreData(sessionItems,
                  SessionStateUtility.GetSessionStaticObjects(context),
                  timeout);
            }
        }

        public override void CreateUninitializedItem(System.Web.HttpContext context, string id, int timeout)
        {
            MongoServer conn = GetConnection();
            MongoCollection sessionCollection = GetSessionCollection(conn);
            BsonDocument doc = new BsonDocument();
            doc.Add("_id", id);
            doc.Add("ApplicationName", ApplicationName);
            doc.Add("Created", DateTime.Now.ToUniversalTime());
            doc.Add("Expires", DateTime.Now.AddMinutes((Double)timeout).ToUniversalTime());
            doc.Add("LockDate", DateTime.Now.ToUniversalTime());
            doc.Add("LockId", 0);
            doc.Add("Timeout", timeout);
            doc.Add("Locked", false);
            doc.Add("SessionItems", "");
            doc.Add("Flags", 1);                       

            try 
            { 
                var result = sessionCollection.Insert(doc, _safeMode); 
                if (!result.Ok)
                {
                    throw new Exception(result.ErrorMessage);
                }
            }       
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "CreateUninitializedItem");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Disconnect();
            }
        }

        /// <summary>
        /// This is a helper function that writes exception detail to the 
        /// event log. Exceptions are written to the event log as a security
        /// measure to ensure private database details are not returned to 
        /// browser. If a method does not return a status or Boolean
        /// indicating the action succeeded or failed, the caller also 
        /// throws a generic exception.
        /// </summary>
        private void WriteToEventLog(Exception e, string action)
        {
            using (EventLog log = new EventLog())
            {
                log.Source = _eventSource;
                log.Log = _eventLog;

                string message =
                  String.Format("An exception occurred communicating with the data source.\n\nAction: {0}\n\nException: {1}", 
                  action, e.ToString());                

                log.WriteEntry(message);
            }
        }

        public override void Dispose()
        {            
        }

        public override void EndRequest(System.Web.HttpContext context)
        {
            
        }       

        public override void InitializeRequest(System.Web.HttpContext context)
        {
            
        }

        public override void ReleaseItemExclusive(System.Web.HttpContext context, string id, object lockId)
        {
            MongoServer conn = GetConnection();
            MongoCollection sessionCollection = GetSessionCollection(conn);

            var query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName), Query.EQ("LockId", (Int32)lockId));
            var update = Update.Set("Locked", false);
            update.Set("Expires", DateTime.Now.AddMinutes(_config.Timeout.TotalMinutes).ToUniversalTime());            
            

            try
            {
                sessionCollection.Update(query, update, _safeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ReleaseItemExclusive");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Disconnect();
            }
        }

        public override void RemoveItem(System.Web.HttpContext context, string id, object lockId, SessionStateStoreData item)
        {
            MongoServer conn = GetConnection();
            MongoCollection sessionCollection = GetSessionCollection(conn);

            var query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName), Query.EQ("LockId", (Int32)lockId));

            try
            {
                sessionCollection.Remove(query, _safeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "RemoveItem");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Disconnect();
            }
        }

        public override void ResetItemTimeout(System.Web.HttpContext context, string id)
        {
            MongoServer conn = GetConnection();
            MongoCollection sessionCollection = GetSessionCollection(conn);
            var query = Query.And(Query.EQ("_id", id), Query.EQ("ApplicationName", ApplicationName));
            var update = Update.Set("Expires", DateTime.Now.AddMinutes(_config.Timeout.TotalMinutes).ToUniversalTime());

            try
            {
                sessionCollection.Update(query, update, _safeMode);
            }
            catch (Exception e)
            {
                if (WriteExceptionsToEventLog)
                {
                    WriteToEventLog(e, "ResetItemTimeout");
                    throw new ProviderException(_exceptionMessage);
                }
                else
                    throw e;
            }
            finally
            {
                conn.Disconnect();
            }
        }       
    }
}
