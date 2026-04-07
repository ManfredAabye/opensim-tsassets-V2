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
using System.Globalization;

namespace OpenSim.Data
{
    public static class TSAssetTypeTokenParser
    {
        private const string InventoryPrefix = "INVENTORY_";
        private const string AssetsPrefix = "assets_";
        private const string TypePrefix = "Type_";

        private static readonly Dictionary<string, sbyte> m_namedTypes = new Dictionary<string, sbyte>(StringComparer.OrdinalIgnoreCase)
        {
            ["INVENTORY_MATERIAL"] = -2,
            ["INVENTORY_TEXTURE"] = 0,
            ["INVENTORY_SOUND"] = 1,
            ["INVENTORY_CALLINGCARD"] = 2,
            ["INVENTORY_LANDMARK"] = 3,
            ["INVENTORY_CLOTHING"] = 5,
            ["INVENTORY_OBJECT"] = 6,
            ["INVENTORY_NOTECARD"] = 7,
            ["INVENTORY_FOLDER"] = 8,
            ["INVENTORY_SCRIPT"] = 10,
            ["INVENTORY_LSLBYTECODE"] = 11,
            ["INVENTORY_BODYPART"] = 13,
            ["INVENTORY_SOUNDWAV"] = 17,
            ["INVENTORY_IMAGETGA"] = 18,
            ["INVENTORY_IMAGEJPEG"] = 19,
            ["INVENTORY_ANIMATION"] = 20,
            ["INVENTORY_GESTURE"] = 21,
            ["INVENTORY_INVENTORY_SIMSTATE"] = 22,
            ["INVENTORY_LINK"] = 24,
            ["INVENTORY_LINKFOLDER"] = 25,
            ["INVENTORY_MARKETPLACE"] = 26,
            ["INVENTORY_MESH"] = 49,
            ["INVENTORY_SETTING"] = 56,
            ["INVENTORY_MATERIALPBR"] = 57
        };

        private static readonly Dictionary<string, sbyte> m_storageNames = new Dictionary<string, sbyte>(StringComparer.OrdinalIgnoreCase)
        {
            ["Material"] = -2,
            ["Texture"] = 0,
            ["Sound"] = 1,
            ["CallingCard"] = 2,
            ["Landmark"] = 3,
            ["Clothing"] = 5,
            ["Object"] = 6,
            ["Notecard"] = 7,
            ["Folder"] = 8,
            ["Script"] = 10,
            ["LslBytecode"] = 11,
            ["Bodypart"] = 13,
            ["SoundWav"] = 17,
            ["ImageTga"] = 18,
            ["ImageJpeg"] = 19,
            ["Animation"] = 20,
            ["Gesture"] = 21,
            ["InventorySimState"] = 22,
            ["Link"] = 24,
            ["LinkFolder"] = 25,
            ["Marketplace"] = 26,
            ["Mesh"] = 49,
            ["Setting"] = 56,
            ["MaterialPbr"] = 57
        };

        private static readonly Dictionary<sbyte, string> m_typeToStorageName = new Dictionary<sbyte, string>
        {
            [-2] = "Material",
            [0] = "Texture",
            [1] = "Sound",
            [2] = "CallingCard",
            [3] = "Landmark",
            [5] = "Clothing",
            [6] = "Object",
            [7] = "Notecard",
            [8] = "Folder",
            [10] = "Script",
            [11] = "LslBytecode",
            [13] = "Bodypart",
            [17] = "SoundWav",
            [18] = "ImageTga",
            [19] = "ImageJpeg",
            [20] = "Animation",
            [21] = "Gesture",
            [22] = "InventorySimState",
            [24] = "Link",
            [25] = "LinkFolder",
            [26] = "Marketplace",
            [49] = "Mesh",
            [56] = "Setting",
            [57] = "MaterialPbr"
        };

        public static bool TryParseAssetTypeToken(string token, out sbyte assetType)
        {
            assetType = 0;
            if (string.IsNullOrWhiteSpace(token))
                return false;

            string normalized = token.Trim();

            if (sbyte.TryParse(normalized, NumberStyles.Integer, CultureInfo.InvariantCulture, out assetType))
                return true;

            if (m_namedTypes.TryGetValue(normalized, out assetType))
                return true;

            if (m_storageNames.TryGetValue(normalized, out assetType))
                return true;

            if (normalized.StartsWith(TypePrefix, StringComparison.OrdinalIgnoreCase))
            {
                string typedSuffix = normalized.Substring(TypePrefix.Length);
                return sbyte.TryParse(typedSuffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out assetType);
            }

            if (!normalized.StartsWith(InventoryPrefix, StringComparison.OrdinalIgnoreCase))
                return false;

            string numericSuffix = normalized.Substring(InventoryPrefix.Length);
            return sbyte.TryParse(numericSuffix, NumberStyles.Integer, CultureInfo.InvariantCulture, out assetType);
        }

        public static bool TryParseAssetTypeFromTableOrTypeToken(string token, out bool isLegacyAssetsTable, out sbyte assetType)
        {
            isLegacyAssetsTable = false;
            assetType = 0;

            if (string.IsNullOrWhiteSpace(token))
                return false;

            string normalized = token.Trim();

            if (normalized.Equals("assets", StringComparison.OrdinalIgnoreCase))
            {
                isLegacyAssetsTable = true;
                return true;
            }

            if (TryParseAssetTypeToken(normalized, out assetType))
                return true;

            if (!normalized.StartsWith(AssetsPrefix, StringComparison.OrdinalIgnoreCase))
                return false;

            string suffix = normalized.Substring(AssetsPrefix.Length);
            return TryParseAssetTypeToken(suffix, out assetType);
        }

        public static bool TryGetStorageTypeName(sbyte assetType, out string storageTypeName)
        {
            return m_typeToStorageName.TryGetValue(assetType, out storageTypeName);
        }

        public static string GetStorageTypeNameOrDefault(sbyte assetType)
        {
            if (TryGetStorageTypeName(assetType, out string storageTypeName))
                return storageTypeName;

            return string.Format(CultureInfo.InvariantCulture, "Type_{0}", assetType);
        }
    }
}