# TSAsset

## Zweck

TSAsset trennt Assets nach Typ, damit große Bestände nicht in einer einzigen Tabelle oder Datei landen.

TSAsset ist optional durch die Konfiguration. Bei MySQL können damit die Assets wartbar sein und den Server nicht überfordern.

Bei SQLite können damit die Assets wartbar sein und die Datei nicht überfordern.

Vorteile:

- Bessere Skalierung bei großen Asset-Mengen
- Geringeres Risiko, dass ein einzelner Speicherbereich an Grenzen läuft
- Gezielte Wartung je Typ möglich

## Speicherverhalten

### MySQL TSAsset

- Provider: OpenSim.Data.MySQL.dll:MySQLtsAssetData
- Speichert in Tabellen pro Typ, z. B. assets_0, assets_49, assets_-2
- Hält einen Index in tsassets_index
- Legacy-Kompatibilität zur Tabelle assets bleibt erhalten

### SQLite TSAsset

- Provider: OpenSim.Data.SQLite.dll:SQLitetsAssetData
- Speichert pro Asset-Typ in eigener SQLite-Datei
- Hält einen kleinen Index in der Hauptdatenbank in tsassets_index
- Legacy-Kompatibilität zur Tabelle assets bleibt erhalten

Beispiel Dateinamen (bei Hauptdatei Asset.db):

- Asset.Texture.db
- Asset.Mesh.db
- Asset.Material.db

## Asset-Typnamen und Alias

Bevorzugt sollten sprechende Typnamen verwendet werden (lesbarer).
Numerische Typen bleiben vollständig kompatibel, damit alte Bestände weiter gelesen werden können.

Wichtig:

- INVENTORY_MATERIAL ist der interne Typname
- Material ist der Tabellen-/Dateiname
- -2 bleibt als Legacy-Typ-ID lesbar

Beispiele (Typ-ID - interner Name -> Tabellen-/Dateiname):

- -2 - INVENTORY_MATERIAL -> Material
- 0 - INVENTORY_TEXTURE -> Texture
- 1 - INVENTORY_SOUND -> Sound
- 2 - INVENTORY_CALLINGCARD -> CallingCard
- 3 - INVENTORY_LANDMARK -> Landmark
- 5 - INVENTORY_CLOTHING -> Clothing
- 6 - INVENTORY_OBJECT -> Object
- 7 - INVENTORY_NOTECARD -> Notecard
- 8 - INVENTORY_FOLDER -> Folder
- 10 - INVENTORY_SCRIPT -> Script
- 11 - INVENTORY_LSLBYTECODE -> LslBytecode
- 13 - INVENTORY_BODYPART -> Bodypart
- 17 - INVENTORY_SOUNDWAV -> SoundWav
- 18 - INVENTORY_IMAGETGA -> ImageTga
- 19 - INVENTORY_IMAGEJPEG -> ImageJpeg
- 20 - INVENTORY_ANIMATION -> Animation
- 21 - INVENTORY_GESTURE -> Gesture
- 22 - INVENTORY_INVENTORY_SIMSTATE -> InventorySimState
- 24 - INVENTORY_LINK -> Link
- 25 - INVENTORY_LINKFOLDER -> LinkFolder
- 26 - INVENTORY_MARKETPLACE -> Marketplace
- 49 - INVENTORY_MESH -> Mesh
- 56 - INVENTORY_SETTING -> Setting
- 57 - INVENTORY_MATERIALPBR -> MaterialPbr

Zusätzlich werden neue Werte weiterhin numerisch akzeptiert, z. B. Type_77 als Tabellenname oder 77 als Typ-ID.

## INI Beispiel: MySQL TSAsset in Robust.HG.ini

Wichtige Aktivierung im Abschnitt AssetService:

    [AssetService]
        ; TSAsset aktivieren
        LocalServiceModule = "OpenSim.Services.AssetService.dll:TSAssetConnector"

Im TSAssetService Abschnitt:

    [TSAssetService]
        Enabled = true

        StorageProvider = "OpenSim.Data.MySQL.dll:MySQLtsAssetData"
        ConnectionString = "Data Source=localhost;Database=robust;User ID=opensim;Password=opensim123;Old Guids=true;SslMode=None;"

        ; Optional: Nur bestimmte Typen erlauben
        ; Bevorzugt Aliasnamen:
        ; TSAssetType = "INVENTORY_TEXTURE,INVENTORY_MESH,INVENTORY_MATERIAL,INVENTORY_MATERIALPBR"
        ; Legacy-kompatibel weiterhin möglich:
        ; TSAssetType = "0,49,-2,57"

        EnableFallbackAutoMigration = false
        EnableFallbackAutoDelete = false

        ; Optional: Typ-Routing pro Datenbank
        AssetDatabase_INVENTORY_TEXTURE = "Data Source=localhost;Database=robust_tex;User ID=opensim;Password=opensim123;Old Guids=true;SslMode=None;"
        AssetDatabase_INVENTORY_MESH = "Data Source=localhost;Database=robust_mesh;User ID=opensim;Password=opensim123;Old Guids=true;SslMode=None;"
        AssetDatabase_INVENTORY_MATERIAL = "Data Source=localhost;Database=robust_mat;User ID=opensim;Password=opensim123;Old Guids=true;SslMode=None;"
        ; Legacy-kompatibel weiterhin möglich: AssetDatabase_0, AssetDatabase_49, AssetDatabase_-2

## INI Beispiel: SQLite TSAsset in Robust.HG.ini

Aktivierung im Abschnitt AssetService:

    [AssetService]
        LocalServiceModule = "OpenSim.Services.AssetService.dll:TSAssetConnector"

TSAssetService für SQLite:

    [TSAssetService]
        Enabled = true

        StorageProvider = "OpenSim.Data.SQLite.dll:SQLitetsAssetData"
        ConnectionString = "URI=file:Asset.db"

        ; Optional: Typ-Filter
        ; Bevorzugt Aliasnamen:
        ; TSAssetType = "INVENTORY_TEXTURE,INVENTORY_MESH,INVENTORY_MATERIAL"
        ; Legacy-kompatibel weiterhin möglich:
        ; TSAssetType = "0,49,-2"

Hinweis:

- Bei SQLite TSAsset erzeugt jeder Typ eine eigene Datei.
- Damit wird das Wachstum auf mehrere Dateien verteilt.

## Admin-Kommandos (TSAsset)

Je nach aktivem Provider verfügbar:

- tsshowmove [from] [to]
- tsmove [from] [to] --force [--reset] [--batch=[n]] [--timeout=[sec]]
- tsfind [asset-id]
- tsverify [all|assets|[type]|assets_[type]]
- tsreindex [all|assets|[type]|assets_[type]] --force
- tscleanlegacy --force

Token für from/to bei Move und Verify:

- assets
- [type] (z. B. 49)
- assets_[type] (z. B. assets_49)
- INVENTORY_[NAME] (z. B. INVENTORY_MESH)
- assets_INVENTORY_[NAME] (z. B. assets_INVENTORY_MESH)

## Wichtig zu Projektdateien

Die .csproj Dateien werden im OpenSimulator-Workflow über prebuild.xml erzeugt.

Daher:

- Dauerhafte Änderungen an Quellcode in .cs Dateien durchführen
- Für Projektstruktur-Regeln prebuild.xml verwenden
- Direkte manuelle Änderungen an erzeugten .csproj können bei Regeneration überschrieben werden

## Folgende Dateien wurden für TSAsset erstellt oder geändert

- OpenSim/Data/TSAssetTypeTokenParser.cs
- OpenSim/Data/MySQL/MySQLtsAssetData.cs
- OpenSim/Services/AssetService/TSAssetConnector.cs
- OpenSim/Data/SQLite/SQLitetsAssetData.cs
- OpenSim/Data/Tests/AssetTests.cs
- tsasset.md
Stand: aktuelle Arbeitskopie.
