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
using System.IO;
using log4net.Config;
using NUnit.Framework;
using NUnit.Framework.Constraints;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Tests.Common;
using System.Data.Common;
using log4net;

// DBMS-specific:
using MySql.Data.MySqlClient;
using OpenSim.Data.MySQL;

using Mono.Data.Sqlite;
using OpenSim.Data.SQLite;

namespace OpenSim.Data.Tests
{
    [TestFixture(Description = "Asset store tests (SQLite)")]
    public class SQLiteAssetTests :  AssetTests<SqliteConnection, SQLiteAssetData>
    {
    }

    [TestFixture(Description = "Asset store tests (SQLite TSAsset)")]
    public class SQLiteTsAssetTests : AssetTests<SqliteConnection, SQLitetsAssetData>
    {
        protected override void ClearDB()
        {
            DropTables("assets", "tsassets_index");
            ResetMigrations("AssetStore");

            string mainDbPath = GetMainSqlitePath(m_connStr);
            if (string.IsNullOrEmpty(mainDbPath))
                return;

            string extension = Path.GetExtension(mainDbPath);
            if (string.IsNullOrEmpty(extension))
                extension = ".db";

            string baseName = mainDbPath.Substring(0, mainDbPath.Length - extension.Length);
            string shardPattern = Path.GetFileName(baseName) + ".*" + extension;
            string dir = Path.GetDirectoryName(mainDbPath);
            if (string.IsNullOrEmpty(dir) || !Directory.Exists(dir))
                return;

            foreach (string file in Directory.GetFiles(dir, shardPattern))
            {
                try { File.Delete(file); }
                catch { }
            }
        }

        [Test]
        public void T030_StoreReadKnownNegativeAndMeshTypes_WithTypeShardFiles()
        {
            TestHelpers.InMethod();

            UUID legacyId = UUID.Random();
            UUID meshId = UUID.Random();

            AssetBase legacyMaterial = new AssetBase(legacyId, "legacy material", -2, UUID.Random().ToString());
            legacyMaterial.Data = new byte[] { 1, 2, 3 };

            AssetBase mesh = new AssetBase(meshId, "mesh asset", 49, UUID.Random().ToString());
            mesh.Data = new byte[] { 4, 5, 6 };

            Assert.IsTrue(m_db.StoreAsset(legacyMaterial), "StoreAsset should succeed for type -2");
            Assert.IsTrue(m_db.StoreAsset(mesh), "StoreAsset should succeed for type 49");

            AssetBase legacyMaterialRead = m_db.GetAsset(legacyId);
            AssetBase meshRead = m_db.GetAsset(meshId);

            Assert.That(legacyMaterialRead, Is.Not.Null);
            Assert.That(meshRead, Is.Not.Null);
            Assert.That(legacyMaterialRead.Type, Is.EqualTo((sbyte)-2));
            Assert.That(meshRead.Type, Is.EqualTo((sbyte)49));

            bool[] exists = m_db.AssetsExist(new[] { legacyId, meshId });
            Assert.That(exists[0], Is.True);
            Assert.That(exists[1], Is.True);

            string shardMinus2 = GetShardPath(m_connStr, -2);
            string shard49 = GetShardPath(m_connStr, 49);
            Assert.That(File.Exists(shardMinus2), Is.True, "Expected type shard file for -2 to exist");
            Assert.That(File.Exists(shard49), Is.True, "Expected type shard file for 49 to exist");
        }

        private static string GetShardPath(string connStr, int assetType)
        {
            string mainDbPath = GetMainSqlitePath(connStr);
            string extension = Path.GetExtension(mainDbPath);
            if (string.IsNullOrEmpty(extension))
                extension = ".db";

            string baseName = mainDbPath.Substring(0, mainDbPath.Length - extension.Length);
            string typeName = TSAssetTypeTokenParser.GetStorageTypeNameOrDefault((sbyte)assetType);
            return baseName + "." + typeName + extension;
        }

        private static string GetMainSqlitePath(string connStr)
        {
            if (string.IsNullOrEmpty(connStr))
                return string.Empty;

            int uriIdx = connStr.IndexOf("URI=file:", StringComparison.OrdinalIgnoreCase);
            if (uriIdx >= 0)
            {
                string path = connStr.Substring(uriIdx + "URI=file:".Length);
                int cut = path.IndexOfAny(new[] { ',', ';' });
                if (cut >= 0)
                    path = path.Substring(0, cut);

                return Path.GetFullPath(path.Trim());
            }

            int dsIdx = connStr.IndexOf("Data Source=", StringComparison.OrdinalIgnoreCase);
            if (dsIdx >= 0)
            {
                string path = connStr.Substring(dsIdx + "Data Source=".Length);
                int cut = path.IndexOf(';');
                if (cut >= 0)
                    path = path.Substring(0, cut);

                return Path.GetFullPath(path.Trim().Trim('"'));
            }

            return string.Empty;
        }
    }

    [TestFixture(Description = "Asset store tests (MySQL)")]
    public class MySqlAssetTests : AssetTests<MySqlConnection, MySQLAssetData>
    {
    }

    [TestFixture(Description = "Asset store tests (MySQL TSAsset)")]
    public class MySqlTsAssetTests : AssetTests<MySqlConnection, MySQLtsAssetData>
    {
        protected override void ClearDB()
        {
            DropTables(
                "`tsassets_index`",
                "`Texture`",
                "`Material`",
                "`Mesh`",
                "`assets_0`",
                "`assets_-2`",
                "`assets_49`");

            ResetMigrations("TSAssetStore");
        }

        [Test]
        public void T030_StoreReadKnownNegativeAndMeshTypes()
        {
            TestHelpers.InMethod();

            UUID legacyId = UUID.Random();
            UUID meshId = UUID.Random();

            AssetBase legacyMaterial = new AssetBase(legacyId, "legacy material", -2, UUID.Random().ToString());
            legacyMaterial.Data = new byte[] { 1, 2, 3 };

            AssetBase mesh = new AssetBase(meshId, "mesh asset", 49, UUID.Random().ToString());
            mesh.Data = new byte[] { 4, 5, 6 };

            Assert.IsTrue(m_db.StoreAsset(legacyMaterial), "StoreAsset should succeed for type -2");
            Assert.IsTrue(m_db.StoreAsset(mesh), "StoreAsset should succeed for type 49");

            AssetBase legacyMaterialRead = m_db.GetAsset(legacyId);
            AssetBase meshRead = m_db.GetAsset(meshId);

            Assert.That(legacyMaterialRead, Is.Not.Null);
            Assert.That(meshRead, Is.Not.Null);
            Assert.That(legacyMaterialRead.Type, Is.EqualTo((sbyte)-2));
            Assert.That(meshRead.Type, Is.EqualTo((sbyte)49));

            bool[] exists = m_db.AssetsExist(new[] { legacyId, meshId });
            Assert.That(exists[0], Is.True);
            Assert.That(exists[1], Is.True);

            int tableCount = 0;
            ExecQuery(
                "SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('Material','Mesh')",
                true,
                reader =>
                {
                    tableCount = Convert.ToInt32(reader["cnt"]);
                    return false;
                });

            Assert.That(tableCount, Is.EqualTo(2), "Expected dynamic type tables Material and Mesh to exist");
        }

        [Test]
        public void T031_StoreReadBoundarySignedTinyIntAssetTypes()
        {
            TestHelpers.InMethod();

            UUID minId = UUID.Random();
            UUID maxId = UUID.Random();

            AssetBase minTypeAsset = new AssetBase(minId, "min type asset", -128, UUID.Random().ToString());
            minTypeAsset.Data = new byte[] { 10, 20, 30 };

            AssetBase maxTypeAsset = new AssetBase(maxId, "max type asset", 127, UUID.Random().ToString());
            maxTypeAsset.Data = new byte[] { 40, 50, 60 };

            Assert.IsTrue(m_db.StoreAsset(minTypeAsset), "StoreAsset should succeed for type -128");
            Assert.IsTrue(m_db.StoreAsset(maxTypeAsset), "StoreAsset should succeed for type 127");

            AssetBase minTypeRead = m_db.GetAsset(minId);
            AssetBase maxTypeRead = m_db.GetAsset(maxId);

            Assert.That(minTypeRead, Is.Not.Null);
            Assert.That(maxTypeRead, Is.Not.Null);
            Assert.That(minTypeRead.Type, Is.EqualTo((sbyte)-128));
            Assert.That(maxTypeRead.Type, Is.EqualTo((sbyte)127));

            bool[] exists = m_db.AssetsExist(new[] { minId, maxId });
            Assert.That(exists[0], Is.True);
            Assert.That(exists[1], Is.True);

            int tableCount = 0;
            ExecQuery(
                "SELECT COUNT(*) AS cnt FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name IN ('Type_-128','Type_127')",
                true,
                reader =>
                {
                    tableCount = Convert.ToInt32(reader["cnt"]);
                    return false;
                });

            Assert.That(tableCount, Is.EqualTo(2), "Expected dynamic type tables Type_-128 and Type_127 to exist");
        }
    }

    public class AssetTests<TConn, TAssetData> : BasicDataServiceTest<TConn, TAssetData>
        where TConn : DbConnection, new()
        where TAssetData : AssetDataBase, new()
    {
        protected TAssetData m_db;

        public UUID uuid1 = UUID.Random();
        public UUID uuid2 = UUID.Random();
        public UUID uuid3 = UUID.Random();

        public string critter1 = UUID.Random().ToString();
        public string critter2 = UUID.Random().ToString();
        public string critter3 = UUID.Random().ToString();

        public byte[] data1 = new byte[100];

        PropertyScrambler<AssetBase> scrambler = new PropertyScrambler<AssetBase>()
                .DontScramble(x => x.ID)
                .DontScramble(x => x.Type)
                .DontScramble(x => x.FullID)
                .DontScramble(x => x.Metadata.ID)
                .DontScramble(x => x.Metadata.CreatorID)
                .DontScramble(x => x.Metadata.ContentType)
                .DontScramble(x => x.Metadata.FullID)
                .DontScramble(x => x.Data);

        protected override void InitService(object service)
        {
            ClearDB();
            m_db = (TAssetData)service;
            m_db.Initialise(m_connStr);
        }

        protected virtual void ClearDB()
        {
            DropTables("assets");
            ResetMigrations("AssetStore");
        }


        [Test]
        public void T001_LoadEmpty()
        {
            TestHelpers.InMethod();

            bool[] exist = m_db.AssetsExist(new[] { uuid1, uuid2, uuid3 });
            Assert.IsFalse(exist[0]);
            Assert.IsFalse(exist[1]);
            Assert.IsFalse(exist[2]);
        }

        [Test]
        public void T010_StoreReadVerifyAssets()
        {
            TestHelpers.InMethod();

            AssetBase a1 = new AssetBase(uuid1, "asset one", (sbyte)AssetType.Texture, critter1.ToString());
            AssetBase a2 = new AssetBase(uuid2, "asset two", (sbyte)AssetType.Texture, critter2.ToString());
            AssetBase a3 = new AssetBase(uuid3, "asset three", (sbyte)AssetType.Texture, critter3.ToString());
            a1.Data = data1;
            a2.Data = data1;
            a3.Data = data1;

            scrambler.Scramble(a1);
            scrambler.Scramble(a2);
            scrambler.Scramble(a3);

            m_db.StoreAsset(a1);
            m_db.StoreAsset(a2);
            m_db.StoreAsset(a3);
            a1.UploadAttempts = 0;
            a2.UploadAttempts = 0;
            a3.UploadAttempts = 0;

            AssetBase a1a = m_db.GetAsset(uuid1);
            a1a.UploadAttempts = 0;
            Assert.That(a1a, Constraints.PropertyCompareConstraint(a1));

            AssetBase a2a = m_db.GetAsset(uuid2);
            a2a.UploadAttempts = 0;
            Assert.That(a2a, Constraints.PropertyCompareConstraint(a2));

            AssetBase a3a = m_db.GetAsset(uuid3);
            a3a.UploadAttempts = 0;
            Assert.That(a3a, Constraints.PropertyCompareConstraint(a3));

            scrambler.Scramble(a1a);
            scrambler.Scramble(a2a);
            scrambler.Scramble(a3a);

            m_db.StoreAsset(a1a);
            m_db.StoreAsset(a2a);
            m_db.StoreAsset(a3a);
            a1a.UploadAttempts = 0;
            a2a.UploadAttempts = 0;
            a3a.UploadAttempts = 0;

            AssetBase a1b = m_db.GetAsset(uuid1);
            a1b.UploadAttempts = 0;
            Assert.That(a1b, Constraints.PropertyCompareConstraint(a1a));

            AssetBase a2b = m_db.GetAsset(uuid2);
            a2b.UploadAttempts = 0;
            Assert.That(a2b, Constraints.PropertyCompareConstraint(a2a));

            AssetBase a3b = m_db.GetAsset(uuid3);
            a3b.UploadAttempts = 0;
            Assert.That(a3b, Constraints.PropertyCompareConstraint(a3a));

            bool[] exist = m_db.AssetsExist(new[] { uuid1, uuid2, uuid3 });
            Assert.IsTrue(exist[0]);
            Assert.IsTrue(exist[1]);
            Assert.IsTrue(exist[2]);

            List<AssetMetadata> metadatas = m_db.FetchAssetMetadataSet(0, 1000);

            Assert.That(metadatas.Count >= 3, "FetchAssetMetadataSet() should have returned at least 3 assets!");

            // It is possible that the Asset table is filled with data, in which case we don't try to find "our"
            // assets there:
            if (metadatas.Count < 1000)
            {
                AssetMetadata metadata = metadatas.Find(x => x.FullID == uuid1);
                Assert.That(metadata.Name, Is.EqualTo(a1b.Name));
                Assert.That(metadata.Description, Is.EqualTo(a1b.Description));
                Assert.That(metadata.Type, Is.EqualTo(a1b.Type));
                Assert.That(metadata.Temporary, Is.EqualTo(a1b.Temporary));
                Assert.That(metadata.FullID, Is.EqualTo(a1b.FullID));
            }
        }

        [Test]
        public void T020_CheckForWeirdCreatorID()
        {
            TestHelpers.InMethod();

            // It is expected that eventually the CreatorID might be an arbitrary string (an URI)
            // rather than a valid UUID (?).  This test is to make sure that the database layer does not
            // attempt to convert CreatorID to GUID, but just passes it both ways as a string.
            AssetBase a1 = new AssetBase(uuid1, "asset one", (sbyte)AssetType.Texture, critter1);
            AssetBase a2 = new AssetBase(uuid2, "asset two", (sbyte)AssetType.Texture, "This is not a GUID!");
            AssetBase a3 = new AssetBase(uuid3, "asset three", (sbyte)AssetType.Texture, "");
            a1.Data = data1;
            a2.Data = data1;
            a3.Data = data1;

            m_db.StoreAsset(a1);
            a1.UploadAttempts = 0;
            m_db.StoreAsset(a2);
            a2.UploadAttempts = 0;
            m_db.StoreAsset(a3);
            a3.UploadAttempts = 0;

            AssetBase a1a = m_db.GetAsset(uuid1);
            a1a.UploadAttempts = 0;
            Assert.That(a1a, Constraints.PropertyCompareConstraint(a1));

            AssetBase a2a = m_db.GetAsset(uuid2);
            a2a.UploadAttempts = 0;
            Assert.That(a2a, Constraints.PropertyCompareConstraint(a2));

            AssetBase a3a = m_db.GetAsset(uuid3);
            a3a.UploadAttempts = 0;
            Assert.That(a3a, Constraints.PropertyCompareConstraint(a3));
        }
    }
}