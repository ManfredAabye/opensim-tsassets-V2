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
using System.Data;
using System.Data.SQLite;
using System.Globalization;
using System.IO;
using System.Reflection;
using log4net;
using OpenMetaverse;
using OpenSim.Data;
using OpenSim.Framework;

namespace OpenSim.Data.SQLite
{
    /// <summary>
    /// SQLite TSAsset provider that stores each asset type in a dedicated SQLite file
    /// to avoid unbounded growth of a single assets database file.
    /// </summary>
    public class SQLitetsAssetData : AssetDataBase
    {
        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        private const string LegacyTableName = "assets";
        private const string IndexTableName = "tsassets_index";

        private const string SelectAssetSQL = "select * from assets where UUID=:UUID";
        private const string UpdateAssetSQL = "update assets set Name=:Name, Description=:Description, Type=:Type, Local=:Local, Temporary=:Temporary, asset_flags=:Flags, CreatorID=:CreatorID, Data=:Data where UUID=:UUID";
        private const string InsertAssetSQL = "insert into assets(UUID, Name, Description, Type, Local, Temporary, asset_flags, CreatorID, Data) values(:UUID, :Name, :Description, :Type, :Local, :Temporary, :Flags, :CreatorID, :Data)";
        private const string DeleteAssetSQL = "delete from assets where UUID=:UUID";
        private const string SelectLegacyMetadataSQL = "select Name, Description, Type, Temporary, asset_flags, UUID, CreatorID from assets limit :start, :count";

        private readonly object m_lock = new object();
        private readonly Dictionary<sbyte, SQLiteConnection> m_typeConnections = new Dictionary<sbyte, SQLiteConnection>();

        private SQLiteConnection m_indexConn;
        private string m_connectionString;
        private string m_mainDbPath;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public override string Version
        {
            get
            {
                Module module = GetType().Module;
                Version dllVersion = module.Assembly.GetName().Version;
                return string.Format(CultureInfo.InvariantCulture, "{0}.{1}.{2}.{3}", dllVersion.Major, dllVersion.Minor, dllVersion.Build, dllVersion.Revision);
            }
        }

        public override string Name
        {
            get { return "SQLite TSAsset storage engine"; }
        }

        public override void Initialise()
        {
            Initialise("URI=file:Asset.db");
        }

        public override void Initialise(string connect)
        {
            DllmapConfigHelper.RegisterAssembly(typeof(SQLiteConnection).Assembly);

            m_connectionString = string.IsNullOrWhiteSpace(connect) ? "URI=file:Asset.db" : connect;
            m_mainDbPath = ResolveMainDbPath(m_connectionString);

            m_indexConn = new SQLiteConnection(m_connectionString);
            m_indexConn.Open();

            Migration migration = new Migration(m_indexConn, Assembly, "AssetStore");
            migration.Update();

            EnsureIndexTable(m_indexConn);
        }

        public override void Dispose()
        {
            lock (m_lock)
            {
                foreach (KeyValuePair<sbyte, SQLiteConnection> kvp in m_typeConnections)
                {
                    kvp.Value.Close();
                }

                m_typeConnections.Clear();

                if (m_indexConn != null)
                {
                    m_indexConn.Close();
                    m_indexConn = null;
                }
            }
        }

        public override AssetBase GetAsset(UUID uuid)
        {
            lock (m_lock)
            {
                if (TryGetIndexedType(uuid, out sbyte type))
                {
                    AssetBase typed = GetAssetFromTypeTable(uuid, type);
                    if (typed != null)
                        return typed;

                    // Cleanup stale index rows if typed data vanished.
                    DeleteIndexRow(uuid);
                }

                // Legacy compatibility for existing non-sharded SQLite assets.
                AssetBase legacy = GetLegacyAsset(uuid);
                if (legacy != null)
                {
                    StoreAssetInternal(legacy);
                    return legacy;
                }

                return null;
            }
        }

        public override bool StoreAsset(AssetBase asset)
        {
            if (asset == null)
                return false;

            lock (m_lock)
            {
                return StoreAssetInternal(asset);
            }
        }

        public override bool[] AssetsExist(UUID[] uuids)
        {
            if (uuids == null || uuids.Length == 0)
                return new bool[0];

            lock (m_lock)
            {
                bool[] result = new bool[uuids.Length];
                HashSet<UUID> known = QueryExistingIds(m_indexConn, IndexTableName, "UUID", uuids);

                for (int i = 0; i < uuids.Length; i++)
                    result[i] = known.Contains(uuids[i]);

                UUID[] pending = BuildMissingIds(uuids, result);
                if (pending.Length > 0)
                {
                    HashSet<UUID> legacy = QueryExistingIds(m_indexConn, LegacyTableName, "UUID", pending);
                    for (int i = 0; i < uuids.Length; i++)
                    {
                        if (!result[i] && legacy.Contains(uuids[i]))
                            result[i] = true;
                    }
                }

                return result;
            }
        }

        public override List<AssetMetadata> FetchAssetMetadataSet(int start, int count)
        {
            List<AssetMetadata> result = new List<AssetMetadata>(Math.Max(0, count));
            if (count <= 0)
                return result;

            lock (m_lock)
            {
                int indexedCount = Convert.ToInt32(ExecuteScalar(m_indexConn, "select count(*) from tsassets_index"), CultureInfo.InvariantCulture);
                if (indexedCount == 0)
                {
                    // Legacy-only fallback path.
                    using (SQLiteCommand cmd = new SQLiteCommand(SelectLegacyMetadataSQL, m_indexConn))
                    {
                        cmd.Parameters.Add(new SQLiteParameter(":start", start));
                        cmd.Parameters.Add(new SQLiteParameter(":count", count));
                        using (IDataReader reader = cmd.ExecuteReader())
                        {
                            while (reader.Read())
                                result.Add(BuildAssetMetadata(reader));
                        }
                    }

                    return result;
                }

                using (SQLiteCommand cmd = new SQLiteCommand("select UUID, Type from tsassets_index order by updated_at desc limit :start, :count", m_indexConn))
                {
                    cmd.Parameters.Add(new SQLiteParameter(":start", start));
                    cmd.Parameters.Add(new SQLiteParameter(":count", count));

                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            UUID id = new UUID(reader["UUID"].ToString());
                            sbyte type = Convert.ToSByte(reader["Type"], CultureInfo.InvariantCulture);
                            AssetMetadata metadata = GetMetadataByType(id, type);
                            if (metadata != null)
                                result.Add(metadata);
                        }
                    }
                }

                return result;
            }
        }

        public override bool Delete(string id)
        {
            if (!UUID.TryParse(id, out UUID assetId))
                return false;

            lock (m_lock)
            {
                bool deleted = false;

                if (TryGetIndexedType(assetId, out sbyte type))
                {
                    SQLiteConnection typeConn = GetOrCreateTypeConnection(type);
                    using (SQLiteCommand cmd = new SQLiteCommand(DeleteAssetSQL, typeConn))
                    {
                        cmd.Parameters.Add(new SQLiteParameter(":UUID", assetId.ToString()));
                        deleted = cmd.ExecuteNonQuery() > 0;
                    }

                    if (!deleted)
                    {
                        string legacyPath = BuildLegacyTypeShardPath(type);
                        string preferredPath = BuildPreferredTypeShardPath(type);
                        if (!legacyPath.Equals(preferredPath, StringComparison.OrdinalIgnoreCase) && File.Exists(legacyPath))
                        {
                            using (SQLiteConnection legacyConn = new SQLiteConnection(string.Format(CultureInfo.InvariantCulture, "Data Source={0};Version=3", legacyPath)))
                            {
                                legacyConn.Open();
                                using (SQLiteCommand legacyDelete = new SQLiteCommand(DeleteAssetSQL, legacyConn))
                                {
                                    legacyDelete.Parameters.Add(new SQLiteParameter(":UUID", assetId.ToString()));
                                    deleted = legacyDelete.ExecuteNonQuery() > 0;
                                }
                            }
                        }
                    }

                    DeleteIndexRow(assetId);
                    return deleted;
                }

                using (SQLiteCommand cmd = new SQLiteCommand(DeleteAssetSQL, m_indexConn))
                {
                    cmd.Parameters.Add(new SQLiteParameter(":UUID", assetId.ToString()));
                    deleted = cmd.ExecuteNonQuery() > 0;
                }

                DeleteIndexRow(assetId);
                return deleted;
            }
        }

        private bool StoreAssetInternal(AssetBase asset)
        {
            string assetName = asset.Name ?? string.Empty;
            if (assetName.Length > AssetBase.MAX_ASSET_NAME)
            {
                assetName = assetName.Substring(0, AssetBase.MAX_ASSET_NAME);
                m_log.WarnFormat("[TSASSET DB]: Name for asset {0} truncated to {1} chars", asset.ID, assetName.Length);
            }

            string assetDescription = asset.Description ?? string.Empty;
            if (assetDescription.Length > AssetBase.MAX_ASSET_DESC)
            {
                assetDescription = assetDescription.Substring(0, AssetBase.MAX_ASSET_DESC);
                m_log.WarnFormat("[TSASSET DB]: Description for asset {0} truncated to {1} chars", asset.ID, assetDescription.Length);
            }

            SQLiteConnection typeConn = GetOrCreateTypeConnection(asset.Type);
            bool exists = AssetExistsInConnection(typeConn, asset.FullID);

            string sql = exists ? UpdateAssetSQL : InsertAssetSQL;
            using (SQLiteCommand cmd = new SQLiteCommand(sql, typeConn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", asset.FullID.ToString()));
                cmd.Parameters.Add(new SQLiteParameter(":Name", assetName));
                cmd.Parameters.Add(new SQLiteParameter(":Description", assetDescription));
                cmd.Parameters.Add(new SQLiteParameter(":Type", asset.Type));
                cmd.Parameters.Add(new SQLiteParameter(":Local", asset.Local));
                cmd.Parameters.Add(new SQLiteParameter(":Temporary", asset.Temporary));
                cmd.Parameters.Add(new SQLiteParameter(":Flags", asset.Flags));
                cmd.Parameters.Add(new SQLiteParameter(":CreatorID", asset.Metadata.CreatorID));
                cmd.Parameters.Add(new SQLiteParameter(":Data", asset.Data));
                cmd.ExecuteNonQuery();
            }

            UpsertIndexRow(asset.FullID, asset.Type);
            return true;
        }

        private void EnsureIndexTable(SQLiteConnection conn)
        {
            using (SQLiteCommand cmd = new SQLiteCommand(
                "create table if not exists tsassets_index(UUID text not null primary key, Type integer not null, updated_at integer not null)",
                conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private void EnsureAssetTable(SQLiteConnection conn)
        {
            using (SQLiteCommand cmd = new SQLiteCommand(
                "create table if not exists assets(UUID text not null primary key, Name, Description, Type, Local, Temporary, asset_flags integer not null default 0, CreatorID varchar(128) default '', Data)",
                conn))
            {
                cmd.ExecuteNonQuery();
            }
        }

        private SQLiteConnection GetOrCreateTypeConnection(sbyte type)
        {
            if (m_typeConnections.TryGetValue(type, out SQLiteConnection existing))
                return existing;

            string path = BuildPreferredTypeShardPath(type);
            string shardConnString = string.Format(CultureInfo.InvariantCulture, "Data Source={0};Version=3", path);
            SQLiteConnection conn = new SQLiteConnection(shardConnString);
            conn.Open();
            EnsureAssetTable(conn);
            m_typeConnections[type] = conn;
            return conn;
        }

        private string BuildPreferredTypeShardPath(sbyte type)
        {
            string mainPath = m_mainDbPath;
            if (string.IsNullOrEmpty(mainPath))
                mainPath = Path.GetFullPath("Asset.db");

            string extension = Path.GetExtension(mainPath);
            if (string.IsNullOrEmpty(extension))
                extension = ".db";

            string withoutExtension = mainPath.Substring(0, mainPath.Length - extension.Length);
            string storageName = TSAssetTypeTokenParser.GetStorageTypeNameOrDefault(type);
            return string.Format(CultureInfo.InvariantCulture, "{0}.{1}{2}", withoutExtension, storageName, extension);
        }

        private string BuildLegacyTypeShardPath(sbyte type)
        {
            string mainPath = m_mainDbPath;
            if (string.IsNullOrEmpty(mainPath))
                mainPath = Path.GetFullPath("Asset.db");

            string extension = Path.GetExtension(mainPath);
            if (string.IsNullOrEmpty(extension))
                extension = ".db";

            string withoutExtension = mainPath.Substring(0, mainPath.Length - extension.Length);
            return string.Format(CultureInfo.InvariantCulture, "{0}.assets_{1}{2}", withoutExtension, type, extension);
        }

        private static string ResolveMainDbPath(string connectionString)
        {
            try
            {
                SQLiteConnectionStringBuilder builder = new SQLiteConnectionStringBuilder(connectionString);
                if (!string.IsNullOrWhiteSpace(builder.DataSource))
                    return Path.GetFullPath(builder.DataSource);
            }
            catch
            {
            }

            string lower = connectionString ?? string.Empty;
            int uriIndex = lower.IndexOf("URI=file:", StringComparison.OrdinalIgnoreCase);
            if (uriIndex >= 0)
            {
                string uriValue = lower.Substring(uriIndex + "URI=file:".Length);
                int cut = uriValue.IndexOfAny(new[] { ',', ';' });
                if (cut >= 0)
                    uriValue = uriValue.Substring(0, cut);

                if (!string.IsNullOrWhiteSpace(uriValue))
                    return Path.GetFullPath(uriValue.Trim());
            }

            int dataSourceIndex = lower.IndexOf("Data Source=", StringComparison.OrdinalIgnoreCase);
            if (dataSourceIndex >= 0)
            {
                string value = lower.Substring(dataSourceIndex + "Data Source=".Length);
                int cut = value.IndexOf(';');
                if (cut >= 0)
                    value = value.Substring(0, cut);

                value = value.Trim().Trim('"');
                if (!string.IsNullOrWhiteSpace(value))
                    return Path.GetFullPath(value);
            }

            return Path.GetFullPath("Asset.db");
        }

        private bool TryGetIndexedType(UUID id, out sbyte type)
        {
            using (SQLiteCommand cmd = new SQLiteCommand("select Type from tsassets_index where UUID=:UUID", m_indexConn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                object value = cmd.ExecuteScalar();
                if (value == null || value is DBNull)
                {
                    type = 0;
                    return false;
                }

                type = Convert.ToSByte(value, CultureInfo.InvariantCulture);
                return true;
            }
        }

        private void UpsertIndexRow(UUID id, sbyte type)
        {
            using (SQLiteCommand cmd = new SQLiteCommand(
                "insert into tsassets_index(UUID, Type, updated_at) values(:UUID, :Type, :updated_at) " +
                "on conflict(UUID) do update set Type=excluded.Type, updated_at=excluded.updated_at",
                m_indexConn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                cmd.Parameters.Add(new SQLiteParameter(":Type", type));
                cmd.Parameters.Add(new SQLiteParameter(":updated_at", (int)Utils.DateTimeToUnixTime(DateTime.UtcNow)));
                cmd.ExecuteNonQuery();
            }
        }

        private void DeleteIndexRow(UUID id)
        {
            using (SQLiteCommand cmd = new SQLiteCommand("delete from tsassets_index where UUID=:UUID", m_indexConn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                cmd.ExecuteNonQuery();
            }
        }

        private AssetBase GetAssetFromTypeTable(UUID id, sbyte type)
        {
            SQLiteConnection conn = GetOrCreateTypeConnection(type);
            using (SQLiteCommand cmd = new SQLiteCommand(SelectAssetSQL, conn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return BuildAsset(reader);
                }
            }

            string legacyPath = BuildLegacyTypeShardPath(type);
            string preferredPath = BuildPreferredTypeShardPath(type);
            if (!legacyPath.Equals(preferredPath, StringComparison.OrdinalIgnoreCase) && File.Exists(legacyPath))
            {
                using (SQLiteConnection legacyConn = new SQLiteConnection(string.Format(CultureInfo.InvariantCulture, "Data Source={0};Version=3", legacyPath)))
                {
                    legacyConn.Open();
                    using (SQLiteCommand cmd = new SQLiteCommand(SelectAssetSQL, legacyConn))
                    {
                        cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                        using (IDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                                return BuildAsset(reader);
                        }
                    }
                }
            }

            return null;
        }

        private AssetBase GetLegacyAsset(UUID id)
        {
            using (SQLiteCommand cmd = new SQLiteCommand(SelectAssetSQL, m_indexConn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return BuildAsset(reader);
                }
            }

            return null;
        }

        private AssetMetadata GetMetadataByType(UUID id, sbyte type)
        {
            SQLiteConnection conn = GetOrCreateTypeConnection(type);
            using (SQLiteCommand cmd = new SQLiteCommand("select Name, Description, Type, Temporary, asset_flags, UUID, CreatorID from assets where UUID=:UUID", conn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return BuildAssetMetadata(reader);
                }
            }

            string legacyPath = BuildLegacyTypeShardPath(type);
            string preferredPath = BuildPreferredTypeShardPath(type);
            if (!legacyPath.Equals(preferredPath, StringComparison.OrdinalIgnoreCase) && File.Exists(legacyPath))
            {
                using (SQLiteConnection legacyConn = new SQLiteConnection(string.Format(CultureInfo.InvariantCulture, "Data Source={0};Version=3", legacyPath)))
                {
                    legacyConn.Open();
                    using (SQLiteCommand cmd = new SQLiteCommand("select Name, Description, Type, Temporary, asset_flags, UUID, CreatorID from assets where UUID=:UUID", legacyConn))
                    {
                        cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                        using (IDataReader reader = cmd.ExecuteReader())
                        {
                            if (reader.Read())
                                return BuildAssetMetadata(reader);
                        }
                    }
                }
            }

            // Legacy fallback in case index points to missing shard data.
            using (SQLiteCommand cmd = new SQLiteCommand("select Name, Description, Type, Temporary, asset_flags, UUID, CreatorID from assets where UUID=:UUID", m_indexConn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    if (reader.Read())
                        return BuildAssetMetadata(reader);
                }
            }

            return null;
        }

        private static AssetBase BuildAsset(IDataReader row)
        {
            AssetBase asset = new AssetBase(
                new UUID((string)row["UUID"]),
                (string)row["Name"],
                Convert.ToSByte(row["Type"], CultureInfo.InvariantCulture),
                row["CreatorID"].ToString());

            asset.Description = (string)row["Description"];
            asset.Local = Convert.ToBoolean(row["Local"], CultureInfo.InvariantCulture);
            asset.Temporary = Convert.ToBoolean(row["Temporary"], CultureInfo.InvariantCulture);
            asset.Flags = (AssetFlags)Convert.ToInt32(row["asset_flags"], CultureInfo.InvariantCulture);
            asset.Data = (byte[])row["Data"];
            return asset;
        }

        private static AssetMetadata BuildAssetMetadata(IDataReader row)
        {
            AssetMetadata metadata = new AssetMetadata();
            metadata.FullID = new UUID((string)row["UUID"]);
            metadata.Name = (string)row["Name"];
            metadata.Description = (string)row["Description"];
            metadata.Type = Convert.ToSByte(row["Type"], CultureInfo.InvariantCulture);
            metadata.Temporary = Convert.ToBoolean(row["Temporary"], CultureInfo.InvariantCulture);
            metadata.Flags = (AssetFlags)Convert.ToInt32(row["asset_flags"], CultureInfo.InvariantCulture);
            metadata.CreatorID = row["CreatorID"].ToString();
            metadata.SHA1 = Array.Empty<byte>();
            return metadata;
        }

        private static object ExecuteScalar(SQLiteConnection conn, string sql)
        {
            using (SQLiteCommand cmd = new SQLiteCommand(sql, conn))
                return cmd.ExecuteScalar();
        }

        private static bool AssetExistsInConnection(SQLiteConnection conn, UUID id)
        {
            using (SQLiteCommand cmd = new SQLiteCommand("select 1 from assets where UUID=:UUID", conn))
            {
                cmd.Parameters.Add(new SQLiteParameter(":UUID", id.ToString()));
                object scalar = cmd.ExecuteScalar();
                return scalar != null && !(scalar is DBNull);
            }
        }

        private static HashSet<UUID> QueryExistingIds(SQLiteConnection conn, string table, string idColumn, UUID[] ids)
        {
            HashSet<UUID> result = new HashSet<UUID>();
            if (ids == null || ids.Length == 0)
                return result;

            const int batchSize = 200;
            for (int offset = 0; offset < ids.Length; offset += batchSize)
            {
                int take = Math.Min(batchSize, ids.Length - offset);
                string[] placeholders = new string[take];
                using (SQLiteCommand cmd = conn.CreateCommand())
                {
                    for (int i = 0; i < take; i++)
                    {
                        string paramName = ":id" + i.ToString(CultureInfo.InvariantCulture);
                        placeholders[i] = paramName;
                        cmd.Parameters.Add(new SQLiteParameter(paramName, ids[offset + i].ToString()));
                    }

                    cmd.CommandText = string.Format(
                        CultureInfo.InvariantCulture,
                        "select {0} from {1} where {0} in ({2})",
                        idColumn,
                        table,
                        string.Join(",", placeholders));

                    using (IDataReader reader = cmd.ExecuteReader())
                    {
                        while (reader.Read())
                            result.Add(new UUID(reader[idColumn].ToString()));
                    }
                }
            }

            return result;
        }

        private static UUID[] BuildMissingIds(UUID[] ids, bool[] existing)
        {
            List<UUID> pending = new List<UUID>();
            for (int i = 0; i < ids.Length; i++)
            {
                if (!existing[i])
                    pending.Add(ids[i]);
            }

            return pending.ToArray();
        }
    }
}