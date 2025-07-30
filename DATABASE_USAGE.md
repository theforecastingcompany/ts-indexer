# Multi-Database Support

The ts-indexer now supports multiple database versions for debugging and development purposes. You can specify custom database paths for all commands that interact with the database.

## Commands with Database Support

All the following commands now accept a `--database` parameter:

- `index` - Create/update time series index
- `search` - Search indexed time series  
- `status` - Show database status and statistics
- `repair` - Repair database inconsistencies
- `clean` - Clean database and progress files
- `debug` - Debug specific datasets

## Usage Examples

### Basic Usage (Default Database)
```bash
# Uses default ts_indexer.db
./ts-indexer search "beijing"
./ts-indexer status
```

### Using Custom Database Paths
```bash
# Create a development index
./ts-indexer index \
  --bucket tfc-modeling-data-eu-west-3 \
  --database dev_catalog.db \
  --max-files 10

# Search in development catalog
./ts-indexer search "subway" --database dev_catalog.db

# Create a production index
./ts-indexer index \
  --bucket tfc-modeling-data-eu-west-3 \
  --database prod_catalog.db

# Search in production catalog
./ts-indexer search "subway" --database prod_catalog.db
```

### Version Management Workflow
```bash
# Create versioned catalogs
./ts-indexer index --database catalog_v1.0.db --max-files 100
./ts-indexer index --database catalog_v1.1.db --max-files 150  

# Compare between versions
./ts-indexer status --database catalog_v1.0.db
./ts-indexer status --database catalog_v1.1.db

# Repair specific version
./ts-indexer repair --database catalog_v1.0.db --dry-run

# Clean specific version
./ts-indexer clean --database old_catalog.db --force
```

### Testing and Development
```bash
# Create test catalog with limited data
./ts-indexer index \
  --database test_catalog.db \
  --filter "beijing_subway_30min,hzmetro" \
  --max-files 5

# Debug specific dataset in test catalog
./ts-indexer debug hzmetro --database test_catalog.db

# Interactive search in test catalog
./ts-indexer search "" --interactive --database test_catalog.db
```

## Database File Naming

The system automatically handles related database files:
- Main database: `custom_name.db`
- WAL file: `custom_name.db-wal`
- Shared memory: `custom_name.db-shm`
- Readonly copy: `custom_name_readonly.db`

## Benefits

1. **Version Control**: Maintain multiple catalog versions for comparison
2. **Safe Testing**: Test changes on separate catalogs without affecting production
3. **Debugging**: Create minimal catalogs for specific debugging scenarios
4. **Parallel Development**: Multiple developers can work with separate catalogs
5. **Rollback Capability**: Keep previous working versions as backup

## Default Behavior

When `--database` is not specified, all commands default to using `ts_indexer.db` for backward compatibility.