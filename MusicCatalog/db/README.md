# MusicCatalog Database

Create the local database:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -U postgres -d postgres -f .\MusicCatalog\db\create_database.sql
```

Create the table schema:

```powershell
& "C:\Program Files\PostgreSQL\18\bin\psql.exe" -h localhost -U postgres -d musiccatalog -f .\MusicCatalog\db\schema.sql
```

The app reload button drops and recreates `music_catalog` from `src/MusicCatalog.Server/data/mp3tag.html`, using the source file's header row as the table columns.
