/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the OpenSimulator Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Threading;
using log4net;
using Nini.Config;
using OpenMetaverse;
using OpenSim.Data;
using OpenSim.Framework;
using OpenSim.Services.Base;
using OpenSim.Services.Interfaces;

namespace OpenSim.Services.AssetService
{
    public class TSAssetConnector : ServiceBase, IAssetService
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);
        private static readonly object m_commandLock = new object();
        private static bool m_commandsRegistered;

        private readonly Dictionary<sbyte, IAssetDataPlugin> m_typeDatabases = new Dictionary<sbyte, IAssetDataPlugin>();
        private readonly Dictionary<string, IAssetDataPlugin> m_connectionDatabases = new Dictionary<string, IAssetDataPlugin>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<UUID, IAssetDataPlugin> m_assetLocationCache = new Dictionary<UUID, IAssetDataPlugin>();
        private readonly ConcurrentQueue<AssetBase> m_fallbackMigrationQueue = new ConcurrentQueue<AssetBase>();
        private readonly ConcurrentDictionary<UUID, byte> m_fallbackMigrationSet = new ConcurrentDictionary<UUID, byte>();
        private readonly object m_cacheLock = new object();
        private readonly HashSet<sbyte> m_allowedTypes = new HashSet<sbyte>();

        private readonly List<IAssetDataPlugin> m_probeOrder = new List<IAssetDataPlugin>();

        private IAssetDataPlugin m_defaultDatabase;
        private IAssetLoader m_assetLoader;
        private IAssetService m_fallbackService;
        private Thread m_fallbackMigrationThread;

        private bool m_enableFallbackAutoMigration;
        private bool m_enableFallbackAutoDelete;
        private int m_migrationCheckIntervalMs = 60000;
        private int m_migrationBatchSize = 25;
        private int m_migrationLowTrafficMaxRequests = 3;
        private int m_migrationQueueMax = 50000;
        private int m_requestsInWindow;

        public TSAssetConnector(IConfigSource config)
            : base(config)
        {
            IConfig assetConfig = config.Configs["AssetService"];
            if (assetConfig == null)
                throw new Exception("No AssetService configuration");

            IConfig tsConfig = config.Configs["TSAssetService"];
            IConfig dbConfig = config.Configs["DatabaseService"];

            if (tsConfig == null || !tsConfig.GetBoolean("Enabled", false))
                throw new Exception("TSAssetConnector disabled. Set [TSAssetService] Enabled = true to enable.");

            string storageProvider = string.Empty;
            string defaultConnectionString = string.Empty;

            if (tsConfig != null)
            {
                storageProvider = tsConfig.GetString("StorageProvider", string.Empty);
                defaultConnectionString = tsConfig.GetString("ConnectionString", string.Empty);
            }

            if (string.IsNullOrEmpty(storageProvider))
                storageProvider = assetConfig.GetString("StorageProvider", string.Empty);

            if (string.IsNullOrEmpty(defaultConnectionString))
                defaultConnectionString = assetConfig.GetString("ConnectionString", string.Empty);

            if (dbConfig != null)
            {
                if (string.IsNullOrEmpty(storageProvider))
                    storageProvider = dbConfig.GetString("StorageProvider", string.Empty);

                if (string.IsNullOrEmpty(defaultConnectionString))
                    defaultConnectionString = dbConfig.GetString("ConnectionString", string.Empty);
            }

            if (string.IsNullOrEmpty(storageProvider))
                throw new Exception("No StorageProvider configured");

            if (string.IsNullOrEmpty(defaultConnectionString))
                throw new Exception("Missing database connection string");

            RegisterConsoleCommands();

            ParseAllowedTypes(tsConfig);

            if (tsConfig != null)
            {
                m_enableFallbackAutoMigration = tsConfig.GetBoolean("EnableFallbackAutoMigration", false);
                m_enableFallbackAutoDelete = tsConfig.GetBoolean("EnableFallbackAutoDelete", false);
                m_migrationCheckIntervalMs = Math.Max(5000, tsConfig.GetInt("MigrationCheckIntervalSeconds", 60) * 1000);
                m_migrationBatchSize = Math.Max(1, tsConfig.GetInt("MigrationBatchSize", 25));
                m_migrationLowTrafficMaxRequests = Math.Max(0, tsConfig.GetInt("MigrationLowTrafficMaxRequests", 3));
                m_migrationQueueMax = Math.Max(100, tsConfig.GetInt("MigrationQueueMax", 50000));
            }

            m_defaultDatabase = CreateAndInitDatabase(storageProvider, defaultConnectionString);
            AddProbeDatabase(m_defaultDatabase);

            if (tsConfig != null)
            {
                ParseTypedDatabaseMappings(tsConfig.GetString("AssetDatabases", string.Empty), storageProvider);
                ParseTypedDatabaseMappingsFromKeys(tsConfig, storageProvider);
            }

            string loaderName = assetConfig.GetString("DefaultAssetLoader", string.Empty);
            if (!string.IsNullOrEmpty(loaderName))
            {
                m_assetLoader = LoadPlugin<IAssetLoader>(loaderName);
                if (m_assetLoader == null)
                    throw new Exception(string.Format("Asset loader could not be loaded from {0}", loaderName));

                bool assetLoaderEnabled = assetConfig.GetBoolean("AssetLoaderEnabled", true);
                if (assetLoaderEnabled)
                {
                    string loaderArgs = assetConfig.GetString("AssetLoaderArgs", string.Empty);
                    m_log.InfoFormat("[TSASSET SERVICE]: Loading default asset set from {0}", loaderArgs);

                    m_assetLoader.ForEachDefaultXmlAsset(
                        loaderArgs,
                        delegate(AssetBase a)
                        {
                            if (a == null)
                                return;

                            if (Get(a.ID) == null)
                                Store(a);
                        });
                }
            }

            string fallbackServiceName = assetConfig.GetString("FallbackService", string.Empty);
            if (!string.IsNullOrEmpty(fallbackServiceName))
            {
                object[] args = new object[] { config };
                m_fallbackService = LoadPlugin<IAssetService>(fallbackServiceName, args);
                if (m_fallbackService != null)
                {
                    m_log.Info("[TSASSET SERVICE]: Fallback service loaded");

                    if (m_enableFallbackAutoMigration)
                    {
                        m_fallbackMigrationThread = new Thread(FallbackMigrationWorker);
                        m_fallbackMigrationThread.IsBackground = true;
                        m_fallbackMigrationThread.Name = "TSAssetFallbackMigration";
                        m_fallbackMigrationThread.Start();
                        m_log.Info("[TSASSET SERVICE]: Fallback auto-migration worker enabled");
                    }
                }
                else
                    m_log.Error("[TSASSET SERVICE]: Failed to load fallback service");
            }

            m_log.InfoFormat("[TSASSET SERVICE]: Enabled with {0} type routes", m_typeDatabases.Count);
        }

        public TSAssetConnector(IConfigSource config, string configName)
            : this(config)
        {
        }

        public AssetBase Get(string id)
        {
            Interlocked.Increment(ref m_requestsInWindow);

            if (!UUID.TryParse(id, out UUID assetID))
            {
                m_log.WarnFormat("[TSASSET SERVICE]: Could not parse requested asset id {0}", id);
                return null;
            }

            IAssetDataPlugin cachedDatabase = null;
            lock (m_cacheLock)
            {
                m_assetLocationCache.TryGetValue(assetID, out cachedDatabase);
            }

            if (cachedDatabase != null)
            {
                try
                {
                    AssetBase cachedAsset = cachedDatabase.GetAsset(assetID);
                    if (cachedAsset != null)
                        return cachedAsset;
                }
                catch (Exception e)
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Cached database lookup failed for asset {0}: {1}", assetID, e.Message);
                }
            }

            for (int i = 0; i < m_probeOrder.Count; i++)
            {
                IAssetDataPlugin db = m_probeOrder[i];

                try
                {
                    AssetBase asset = db.GetAsset(assetID);
                    if (asset != null)
                    {
                        lock (m_cacheLock)
                        {
                            m_assetLocationCache[assetID] = db;
                        }
                        return asset;
                    }
                }
                catch (Exception e)
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Asset lookup failed for asset {0}: {1}", assetID, e.Message);
                }
            }

            if (m_fallbackService != null)
            {
                try
                {
                    AssetBase fallbackAsset = m_fallbackService.Get(id);
                    if (fallbackAsset != null)
                    {
                        string storedId = Store(fallbackAsset);
                        bool storeOk = !storedId.Equals(UUID.Zero.ToString(), StringComparison.Ordinal);

                        if (storeOk && m_enableFallbackAutoDelete)
                            TryDeleteFromFallback(id);

                        if (m_enableFallbackAutoMigration && !storeOk)
                            EnqueueFallbackMigration(fallbackAsset);

                        return fallbackAsset;
                    }
                }
                catch (Exception e)
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Fallback lookup failed for asset {0}: {1}", id, e.Message);
                }
            }

            return null;
        }

        public AssetBase Get(string id, string ForeignAssetService, bool StoreOnLocalGrid)
        {
            return Get(id);
        }

        public AssetMetadata GetMetadata(string id)
        {
            AssetBase asset = Get(id);
            return asset != null ? asset.Metadata : null;
        }

        public byte[] GetData(string id)
        {
            AssetBase asset = Get(id);
            return asset != null ? asset.Data : null;
        }

        public AssetBase GetCached(string id)
        {
            return Get(id);
        }

        public bool Get(string id, object sender, AssetRetrieved handler)
        {
            handler(id, sender, Get(id));
            return true;
        }

        public void Get(string id, string ForeignAssetService, bool StoreOnLocalGrid, SimpleAssetRetrieved callBack)
        {
            callBack(Get(id));
        }

        public bool[] AssetsExist(string[] ids)
        {
            if (ids == null || ids.Length == 0)
                return new bool[0];

            bool[] results = new bool[ids.Length];
            UUID[] uuids = new UUID[ids.Length];
            bool[] valid = new bool[ids.Length];

            for (int i = 0; i < ids.Length; i++)
            {
                if (UUID.TryParse(ids[i], out UUID parsed))
                {
                    uuids[i] = parsed;
                    valid[i] = true;
                }
            }

            for (int dbIndex = 0; dbIndex < m_probeOrder.Count; dbIndex++)
            {
                IAssetDataPlugin db = m_probeOrder[dbIndex];
                UUID[] query = BuildPendingUuidList(uuids, valid, results);

                if (query.Length == 0)
                    break;

                try
                {
                    bool[] exists = db.AssetsExist(query);
                    MergeExistenceResults(query, exists, uuids, valid, results, db);
                }
                catch (Exception e)
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: AssetsExist failed on a database: {0}", e.Message);
                }
            }

            return results;
        }

        public string Store(AssetBase asset)
        {
            if (asset == null)
                return UUID.Zero.ToString();

            IAssetDataPlugin db = ResolveDatabaseForType(asset.Type);
            if (db == null)
            {
                m_log.WarnFormat("[TSASSET SERVICE]: No database resolved for asset {0}, type {1}", asset.FullID, asset.Type);
                return UUID.Zero.ToString();
            }

            try
            {
                bool stored = db.StoreAsset(asset);
                if (!stored)
                    return UUID.Zero.ToString();

                lock (m_cacheLock)
                {
                    m_assetLocationCache[asset.FullID] = db;
                }

                return asset.ID;
            }
            catch (Exception e)
            {
                m_log.ErrorFormat("[TSASSET SERVICE]: Error storing asset {0}: {1}", asset.FullID, e.Message);
                return UUID.Zero.ToString();
            }
        }

        public bool UpdateContent(string id, byte[] data)
        {
            AssetBase asset = Get(id);
            if (asset == null)
                return false;

            asset.Data = data;
            return Store(asset) != UUID.Zero.ToString();
        }

        public bool Delete(string id)
        {
            if (!UUID.TryParse(id, out UUID assetID))
                return false;

            IAssetDataPlugin cachedDatabase = null;
            lock (m_cacheLock)
            {
                m_assetLocationCache.TryGetValue(assetID, out cachedDatabase);
            }

            if (cachedDatabase != null)
            {
                try
                {
                    bool deleted = cachedDatabase.Delete(id);
                    if (deleted)
                    {
                        lock (m_cacheLock)
                        {
                            m_assetLocationCache.Remove(assetID);
                        }
                        return true;
                    }
                }
                catch (Exception e)
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Cached delete failed for asset {0}: {1}", assetID, e.Message);
                }
            }

            for (int i = 0; i < m_probeOrder.Count; i++)
            {
                try
                {
                    if (m_probeOrder[i].Delete(id))
                    {
                        lock (m_cacheLock)
                        {
                            m_assetLocationCache.Remove(assetID);
                        }
                        return true;
                    }
                }
                catch (Exception e)
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Delete failed for asset {0}: {1}", assetID, e.Message);
                }
            }

            return false;
        }

        private static UUID[] BuildPendingUuidList(UUID[] uuids, bool[] valid, bool[] results)
        {
            List<UUID> pending = new List<UUID>(uuids.Length);
            for (int i = 0; i < uuids.Length; i++)
            {
                if (valid[i] && !results[i])
                    pending.Add(uuids[i]);
            }

            return pending.ToArray();
        }

        private void MergeExistenceResults(UUID[] query, bool[] exists, UUID[] sourceIds, bool[] valid, bool[] results, IAssetDataPlugin db)
        {
            if (exists == null)
                return;

            int len = Math.Min(query.Length, exists.Length);
            for (int i = 0; i < len; i++)
            {
                if (!exists[i])
                    continue;

                UUID foundId = query[i];

                for (int sourceIndex = 0; sourceIndex < sourceIds.Length; sourceIndex++)
                {
                    if (valid[sourceIndex] && sourceIds[sourceIndex] == foundId)
                    {
                        results[sourceIndex] = true;
                    }
                }

                lock (m_cacheLock)
                {
                    m_assetLocationCache[foundId] = db;
                }
            }
        }

        private void ParseAllowedTypes(IConfig tsConfig)
        {
            if (tsConfig == null)
                return;

            string raw = tsConfig.GetString("TSAssetType", string.Empty);
            if (string.IsNullOrWhiteSpace(raw))
                return;

            string[] tokens = raw.Split(new char[] { ',', ';', ' ', '\t', '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < tokens.Length; i++)
            {
                if (TSAssetTypeTokenParser.TryParseAssetTypeToken(tokens[i], out sbyte type))
                    m_allowedTypes.Add(type);
                else
                    m_log.WarnFormat("[TSASSET SERVICE]: Ignoring invalid TSAssetType entry '{0}'", tokens[i]);
            }
        }

        private void ParseTypedDatabaseMappings(string rawMappings, string storageProvider)
        {
            if (string.IsNullOrWhiteSpace(rawMappings))
                return;

            string[] lines = rawMappings.Split(new char[] { '\r', '\n', '|' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < lines.Length; i++)
            {
                string line = lines[i].Trim();
                if (string.IsNullOrEmpty(line))
                    continue;

                line = line.Trim('"');
                if (line.EndsWith(";;", StringComparison.Ordinal))
                    line = line.Substring(0, line.Length - 2);
                else
                    line = line.TrimEnd(';');

                int sep = line.IndexOf(':');
                if (sep <= 0 || sep == line.Length - 1)
                    continue;

                string typeToken = line.Substring(0, sep).Trim();
                string connectionString = line.Substring(sep + 1).Trim();

                if (!TSAssetTypeTokenParser.TryParseAssetTypeToken(typeToken, out sbyte assetType))
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Ignoring invalid asset type mapping '{0}'", typeToken);
                    continue;
                }

                if (m_allowedTypes.Count > 0 && !m_allowedTypes.Contains(assetType))
                    continue;

                if (string.IsNullOrEmpty(connectionString))
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Ignoring empty connection string for asset type {0}", assetType);
                    continue;
                }

                IAssetDataPlugin db = GetOrCreateDatabase(storageProvider, connectionString);
                m_typeDatabases[assetType] = db;
                AddProbeDatabase(db);
            }
        }

        private void ParseTypedDatabaseMappingsFromKeys(IConfig tsConfig, string storageProvider)
        {
            if (tsConfig == null)
                return;

            string[] keys = tsConfig.GetKeys();
            if (keys == null || keys.Length == 0)
                return;

            for (int i = 0; i < keys.Length; i++)
            {
                string key = keys[i];
                if (string.IsNullOrWhiteSpace(key))
                    continue;

                string prefix = null;
                if (key.StartsWith("AssetDatabase_", StringComparison.OrdinalIgnoreCase))
                    prefix = "AssetDatabase_";
                else if (key.StartsWith("AssetDatabase.", StringComparison.OrdinalIgnoreCase))
                    prefix = "AssetDatabase.";

                if (prefix == null)
                    continue;

                string typeToken = key.Substring(prefix.Length).Trim();
                if (!TSAssetTypeTokenParser.TryParseAssetTypeToken(typeToken, out sbyte assetType))
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Ignoring invalid asset type key '{0}'", key);
                    continue;
                }

                if (m_allowedTypes.Count > 0 && !m_allowedTypes.Contains(assetType))
                    continue;

                string connectionString = tsConfig.GetString(key, string.Empty).Trim();
                if (connectionString.Length >= 2 &&
                    connectionString[0] == '"' &&
                    connectionString[connectionString.Length - 1] == '"')
                {
                    connectionString = connectionString.Substring(1, connectionString.Length - 2).Trim();
                }

                if (string.IsNullOrEmpty(connectionString))
                {
                    m_log.WarnFormat("[TSASSET SERVICE]: Ignoring empty connection string for asset type key '{0}'", key);
                    continue;
                }

                IAssetDataPlugin db = GetOrCreateDatabase(storageProvider, connectionString);
                m_typeDatabases[assetType] = db;
                AddProbeDatabase(db);
            }
        }

        private IAssetDataPlugin ResolveDatabaseForType(sbyte type)
        {
            if (m_typeDatabases.TryGetValue(type, out IAssetDataPlugin db))
                return db;

            return m_defaultDatabase;
        }

        private IAssetDataPlugin GetOrCreateDatabase(string storageProvider, string connectionString)
        {
            if (m_connectionDatabases.TryGetValue(connectionString, out IAssetDataPlugin existing))
                return existing;

            IAssetDataPlugin created = CreateAndInitDatabase(storageProvider, connectionString);
            m_connectionDatabases[connectionString] = created;
            return created;
        }

        private IAssetDataPlugin CreateAndInitDatabase(string storageProvider, string connectionString)
        {
            IAssetDataPlugin database = LoadPlugin<IAssetDataPlugin>(storageProvider);
            if (database == null)
                throw new Exception(string.Format("Could not find a storage interface in the module {0}", storageProvider));

            database.Initialise(connectionString);
            return database;
        }

        private void AddProbeDatabase(IAssetDataPlugin db)
        {
            for (int i = 0; i < m_probeOrder.Count; i++)
            {
                if (object.ReferenceEquals(m_probeOrder[i], db))
                    return;
            }

            m_probeOrder.Add(db);
        }

        private void EnqueueFallbackMigration(AssetBase asset)
        {
            if (asset == null)
                return;

            if (m_fallbackMigrationSet.Count >= m_migrationQueueMax)
                return;

            if (m_fallbackMigrationSet.TryAdd(asset.FullID, 0))
                m_fallbackMigrationQueue.Enqueue(asset);
        }

        private void FallbackMigrationWorker()
        {
            while (true)
            {
                Thread.Sleep(m_migrationCheckIntervalMs);

                int requests = Interlocked.Exchange(ref m_requestsInWindow, 0);
                if (requests > m_migrationLowTrafficMaxRequests)
                    continue;

                int migrated = 0;
                while (migrated < m_migrationBatchSize && m_fallbackMigrationQueue.TryDequeue(out AssetBase asset))
                {
                    try
                    {
                        string id = Store(asset);
                        if (!id.Equals(UUID.Zero.ToString(), StringComparison.Ordinal))
                        {
                            if (m_enableFallbackAutoDelete)
                                TryDeleteFromFallback(asset.ID);

                            migrated++;
                        }
                    }
                    catch (Exception e)
                    {
                        m_log.WarnFormat("[TSASSET SERVICE]: Auto-migration failed for asset {0}: {1}", asset != null ? asset.FullID.ToString() : "<null>", e.Message);
                    }
                    finally
                    {
                        if (asset != null)
                            m_fallbackMigrationSet.TryRemove(asset.FullID, out _);
                    }
                }

                if (migrated > 0)
                    m_log.InfoFormat("[TSASSET SERVICE]: Auto-migrated {0} fallback assets during low traffic", migrated);
            }
        }

        private void TryDeleteFromFallback(string id)
        {
            if (m_fallbackService == null || string.IsNullOrEmpty(id))
                return;

            try
            {
                if (!m_fallbackService.Delete(id))
                    m_log.WarnFormat("[TSASSET SERVICE]: Fallback auto-delete failed for asset {0}", id);
            }
            catch (Exception e)
            {
                m_log.WarnFormat("[TSASSET SERVICE]: Fallback auto-delete exception for asset {0}: {1}", id, e.Message);
            }
        }

        private void RegisterConsoleCommands()
        {
            if (MainConsole.Instance == null)
                return;

            lock (m_commandLock)
            {
                if (m_commandsRegistered)
                    return;

                MainConsole.Instance.Commands.AddCommand(
                    "tsasset",
                    false,
                    "tsmove",
                    "tsmove <from> <to> --force [--reset] [--batch=<n>] [--timeout=<sec>]",
                    "Move tsasset rows between tables (requires --force). Supports resume/reset and batch/timeout overrides",
                    HandleTsMove);

                MainConsole.Instance.Commands.AddCommand(
                    "tsasset",
                    false,
                    "tsshowmove",
                    "tsshowmove <from> <to>",
                    "Preview tsasset move counts without writing changes",
                    HandleTsShowMove);

                MainConsole.Instance.Commands.AddCommand(
                    "tsasset",
                    false,
                    "tsfind",
                    "tsfind <asset-id>",
                    "Find tsasset table and index status for one asset id",
                    HandleTsFind);

                MainConsole.Instance.Commands.AddCommand(
                    "tsasset",
                    false,
                    "tsverify",
                    "tsverify [all|assets|<type>|assets_<type>]",
                    "Verify tsasset table/index consistency",
                    HandleTsVerify);

                MainConsole.Instance.Commands.AddCommand(
                    "tsasset",
                    false,
                    "tsreindex",
                    "tsreindex [all|assets|<type>|assets_<type>] --force [--batch=<n>] [--timeout=<sec>]",
                    "Rebuild tsasset index entries for typed tables or clean legacy index entries",
                    HandleTsReindex);

                MainConsole.Instance.Commands.AddCommand(
                    "tsasset",
                    false,
                    "tscleanlegacy",
                    "tscleanlegacy --force [--batch=<n>] [--timeout=<sec>]",
                    "Remove tsassets_index rows that still point to legacy assets table",
                    HandleTsCleanLegacy);

                m_commandsRegistered = true;
            }
        }

        private void HandleTsMove(string module, string[] args)
        {
            HandleTsMoveInternal(args, false);
        }

        private void HandleTsShowMove(string module, string[] args)
        {
            HandleTsMoveInternal(args, true);
        }

        private void HandleTsFind(string module, string[] args)
        {
            if (args == null || args.Length < 2)
            {
                MainConsole.Instance.Output("Syntax: tsfind <asset-id>");
                return;
            }

            string assetId = args[1];
            List<IAssetDataPlugin> uniqueDatabases = BuildUniqueDatabases();

            int supportedDatabases = 0;
            bool foundAny = false;

            for (int i = 0; i < uniqueDatabases.Count; i++)
            {
                if (!(uniqueDatabases[i] is ITSAssetAdminData adminData))
                    continue;

                supportedDatabases++;

                if (!adminData.TryFindAssetLocation(assetId, out TSAssetFindReport report, out string errorMessage))
                {
                    MainConsole.Instance.Output(string.Format("tsfind failed on database #{0}: {1}", i + 1, errorMessage));
                    return;
                }

                if (!report.Found)
                    continue;

                foundAny = true;
                MainConsole.Instance.Output(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "tsfind {0}: table={1}, index={2}, index-type={3}",
                        report.AssetId,
                        string.IsNullOrEmpty(report.TableName) ? "<unknown>" : report.TableName,
                        report.HasIndexEntry ? "yes" : "no",
                        report.HasIndexEntry ? report.IndexAssetType.ToString(CultureInfo.InvariantCulture) : "n/a"));
            }

            if (supportedDatabases == 0)
            {
                MainConsole.Instance.Output("tsfind is not supported by the configured asset data plugin(s)");
                return;
            }

            if (!foundAny)
                MainConsole.Instance.Output(string.Format(CultureInfo.InvariantCulture, "tsfind {0}: not found", assetId));
        }

        private void HandleTsVerify(string module, string[] args)
        {
            string scope = (args != null && args.Length >= 2) ? args[1] : "all";

            List<IAssetDataPlugin> uniqueDatabases = BuildUniqueDatabases();

            int supportedDatabases = 0;
            int totalTablesChecked = 0;
            int totalRows = 0;
            int totalMissingIndexRows = 0;
            int totalWrongIndexTypeRows = 0;
            int totalOrphanIndexRows = 0;
            int totalLegacyRowsWithIndex = 0;

            for (int i = 0; i < uniqueDatabases.Count; i++)
            {
                if (!(uniqueDatabases[i] is ITSAssetAdminData adminData))
                    continue;

                supportedDatabases++;

                if (!adminData.TryVerifyAssets(scope, out TSAssetVerifyReport report, out string errorMessage))
                {
                    MainConsole.Instance.Output(string.Format("tsverify failed on database #{0}: {1}", i + 1, errorMessage));
                    return;
                }

                totalTablesChecked += report.TablesChecked;
                totalRows += report.TotalRows;
                totalMissingIndexRows += report.MissingIndexRows;
                totalWrongIndexTypeRows += report.WrongIndexTypeRows;
                totalOrphanIndexRows += report.OrphanIndexRows;
                totalLegacyRowsWithIndex += report.LegacyRowsWithIndex;
            }

            if (supportedDatabases == 0)
            {
                MainConsole.Instance.Output("tsverify is not supported by the configured asset data plugin(s)");
                return;
            }

            MainConsole.Instance.Output(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "tsverify {0}: tables={1}, rows={2}, missing-index={3}, wrong-index-type={4}, orphan-index={5}, legacy-with-index={6}",
                    scope,
                    totalTablesChecked,
                    totalRows,
                    totalMissingIndexRows,
                    totalWrongIndexTypeRows,
                    totalOrphanIndexRows,
                    totalLegacyRowsWithIndex));
        }

        private void HandleTsReindex(string module, string[] args)
        {
            bool hasExplicitScope = args != null && args.Length >= 2 && !args[1].StartsWith("-", StringComparison.Ordinal);
            string scope = hasExplicitScope ? args[1] : "all";
            int optionsStartIndex = hasExplicitScope ? 2 : 1;

            if (!TryParseAdminWriteOptions(args, optionsStartIndex, out TSAssetMoveOptions options, out string optionError))
            {
                MainConsole.Instance.Output(optionError);
                return;
            }

            List<IAssetDataPlugin> uniqueDatabases = BuildUniqueDatabases();
            int supportedDatabases = 0;

            TSAssetReindexReport total = new TSAssetReindexReport
            {
                Scope = scope,
                TablesProcessed = 0,
                RowsScanned = 0,
                IndexRowsUpserted = 0,
                IndexRowsDeleted = 0
            };

            for (int i = 0; i < uniqueDatabases.Count; i++)
            {
                if (!(uniqueDatabases[i] is ITSAssetAdminData adminData))
                    continue;

                supportedDatabases++;

                if (!adminData.TryReindexAssets(scope, options, out TSAssetReindexReport report, out string errorMessage))
                {
                    MainConsole.Instance.Output(string.Format("tsreindex failed on database #{0}: {1}", i + 1, errorMessage));
                    return;
                }

                total.TablesProcessed += report.TablesProcessed;
                total.RowsScanned += report.RowsScanned;
                total.IndexRowsUpserted += report.IndexRowsUpserted;
                total.IndexRowsDeleted += report.IndexRowsDeleted;
            }

            if (supportedDatabases == 0)
            {
                MainConsole.Instance.Output("tsreindex is not supported by the configured asset data plugin(s)");
                return;
            }

            MainConsole.Instance.Output(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "tsreindex {0}: tables={1}, scanned={2}, index-upserted={3}, index-deleted={4}",
                    scope,
                    total.TablesProcessed,
                    total.RowsScanned,
                    total.IndexRowsUpserted,
                    total.IndexRowsDeleted));
        }

        private void HandleTsCleanLegacy(string module, string[] args)
        {
            if (!TryParseAdminWriteOptions(args, 1, out TSAssetMoveOptions options, out string optionError))
            {
                MainConsole.Instance.Output(optionError);
                return;
            }

            List<IAssetDataPlugin> uniqueDatabases = BuildUniqueDatabases();
            int supportedDatabases = 0;

            TSAssetReindexReport total = new TSAssetReindexReport
            {
                Scope = "assets",
                TablesProcessed = 0,
                RowsScanned = 0,
                IndexRowsUpserted = 0,
                IndexRowsDeleted = 0
            };

            for (int i = 0; i < uniqueDatabases.Count; i++)
            {
                if (!(uniqueDatabases[i] is ITSAssetAdminData adminData))
                    continue;

                supportedDatabases++;

                if (!adminData.TryCleanLegacyIndex(options, out TSAssetReindexReport report, out string errorMessage))
                {
                    MainConsole.Instance.Output(string.Format("tscleanlegacy failed on database #{0}: {1}", i + 1, errorMessage));
                    return;
                }

                total.TablesProcessed += report.TablesProcessed;
                total.RowsScanned += report.RowsScanned;
                total.IndexRowsUpserted += report.IndexRowsUpserted;
                total.IndexRowsDeleted += report.IndexRowsDeleted;
            }

            if (supportedDatabases == 0)
            {
                MainConsole.Instance.Output("tscleanlegacy is not supported by the configured asset data plugin(s)");
                return;
            }

            MainConsole.Instance.Output(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "tscleanlegacy: tables={0}, scanned={1}, index-deleted={2}",
                    total.TablesProcessed,
                    total.RowsScanned,
                    total.IndexRowsDeleted));
        }

        private bool TryParseAdminWriteOptions(string[] args, int startIndex, out TSAssetMoveOptions options, out string error)
        {
            options = new TSAssetMoveOptions
            {
                ResetCheckpoint = false,
                BatchSize = 0,
                CommandTimeoutSeconds = 0
            };

            error = string.Empty;
            bool hasForce = false;

            if (args == null)
            {
                error = "Missing command arguments";
                return false;
            }

            for (int argIndex = startIndex; argIndex < args.Length; argIndex++)
            {
                string flag = args[argIndex];

                if (flag.Equals("--force", StringComparison.OrdinalIgnoreCase) ||
                    flag.Equals("-f", StringComparison.OrdinalIgnoreCase))
                {
                    hasForce = true;
                    continue;
                }

                if (flag.Equals("--reset", StringComparison.OrdinalIgnoreCase))
                {
                    options.ResetCheckpoint = true;
                    continue;
                }

                if (flag.StartsWith("--batch=", StringComparison.OrdinalIgnoreCase))
                {
                    string value = flag.Substring("--batch=".Length).Trim();
                    if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int batchSize) || batchSize <= 0)
                    {
                        error = "Invalid --batch value. Example: --batch=2000";
                        return false;
                    }

                    options.BatchSize = batchSize;
                    continue;
                }

                if (flag.StartsWith("--timeout=", StringComparison.OrdinalIgnoreCase))
                {
                    string value = flag.Substring("--timeout=".Length).Trim();
                    if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int timeoutSeconds) || timeoutSeconds <= 0)
                    {
                        error = "Invalid --timeout value. Example: --timeout=600";
                        return false;
                    }

                    options.CommandTimeoutSeconds = timeoutSeconds;
                    continue;
                }
            }

            if (!hasForce)
            {
                error = "Write command aborted: missing --force flag";
                return false;
            }

            return true;
        }

        private List<IAssetDataPlugin> BuildUniqueDatabases()
        {
            List<IAssetDataPlugin> uniqueDatabases = new List<IAssetDataPlugin>();
            for (int i = 0; i < m_probeOrder.Count; i++)
            {
                IAssetDataPlugin db = m_probeOrder[i];
                bool alreadyAdded = false;

                for (int j = 0; j < uniqueDatabases.Count; j++)
                {
                    if (object.ReferenceEquals(uniqueDatabases[j], db))
                    {
                        alreadyAdded = true;
                        break;
                    }
                }

                if (!alreadyAdded)
                    uniqueDatabases.Add(db);
            }

            return uniqueDatabases;
        }

        private void HandleTsMoveInternal(string[] args, bool previewOnly)
        {
            if (args == null || args.Length < 3)
            {
                MainConsole.Instance.Output(previewOnly ? "Syntax: tsshowmove <from> <to>" : "Syntax: tsmove <from> <to> --force");
                MainConsole.Instance.Output("Examples: tsmove assets 7 | tsmove 7 assets | tsmove assets_7 assets");
                return;
            }

            string from = args[1];
            string to = args[2];
            TSAssetMoveOptions moveOptions = new TSAssetMoveOptions
            {
                ResetCheckpoint = false,
                BatchSize = 0,
                CommandTimeoutSeconds = 0
            };

            if (!previewOnly)
            {
                if (!TryParseAdminWriteOptions(args, 3, out moveOptions, out string optionError))
                {
                    MainConsole.Instance.Output(optionError.Replace("Write command", "tsmove"));
                    MainConsole.Instance.Output(string.Format(CultureInfo.InvariantCulture, "Preview first: tsshowmove {0} {1}", from, to));
                    MainConsole.Instance.Output(string.Format(CultureInfo.InvariantCulture, "Execute move: tsmove {0} {1} --force", from, to));
                    return;
                }
            }

            List<IAssetDataPlugin> uniqueDatabases = BuildUniqueDatabases();

            int supportedDatabases = 0;
            int totalCandidates = 0;
            int totalAlreadyInTarget = 0;
            int totalInserted = 0;
            int totalDeletedFromSource = 0;
            int totalIndexAffected = 0;

            for (int i = 0; i < uniqueDatabases.Count; i++)
            {
                if (!(uniqueDatabases[i] is ITSAssetAdminData adminData))
                    continue;

                supportedDatabases++;

                bool ok;
                TSAssetMoveReport report;
                string errorMessage;

                if (previewOnly)
                    ok = adminData.TryPreviewMoveAssets(from, to, out report, out errorMessage);
                else
                    ok = adminData.TryMoveAssets(from, to, moveOptions, out report, out errorMessage);

                if (!ok)
                {
                    MainConsole.Instance.Output(string.Format("{0} failed on database #{1}: {2}", previewOnly ? "tsshowmove" : "tsmove", i + 1, errorMessage));
                    return;
                }

                totalCandidates += report.CandidateCount;
                totalAlreadyInTarget += report.AlreadyInTargetCount;
                totalInserted += report.InsertedCount;
                totalDeletedFromSource += report.DeletedFromSourceCount;
                totalIndexAffected += report.IndexAffectedCount;
            }

            if (supportedDatabases == 0)
            {
                MainConsole.Instance.Output(string.Format("{0} is not supported by the configured asset data plugin(s)", previewOnly ? "tsshowmove" : "tsmove"));
                return;
            }

            MainConsole.Instance.Output(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "{0} {1} -> {2}: candidates={3}, inserted={4}, already-in-target={5}, deleted-from-source={6}, index-affected={7}",
                    previewOnly ? "tsshowmove" : "tsmove",
                    from,
                    to,
                    totalCandidates,
                    totalInserted,
                    totalAlreadyInTarget,
                    totalDeletedFromSource,
                    totalIndexAffected));
        }
    }
}
