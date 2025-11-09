# Checkpoint System Guide

## Overview

The descriptive statistics script now includes an intelligent checkpoint system that:
1. Saves valid dataset to Parquet format for faster reads
2. Creates checkpoints after each major computation step
3. Automatically detects and loads from checkpoints on restart

## Key Improvements

### 1. Valid Dataset Saved to Parquet
- **Location**: `traffic_checkpoints/valid_data/`
- **Format**: Parquet (columnar format with compression)
- **Benefits**:
  - 10-50x better compression than CSV
  - Much faster read performance (columnar storage)
  - Automatically splits into multiple files for parallel processing
  - Preserves schema and data types

### 2. Checkpoint After Every Step
The script saves checkpoints for:
- `valid_data/` - Filtered valid records (~1.6B records)
- `district_stats/` - District-level statistics
- `road_stats/` - Road-level statistics
- `lane_stats/` - Lane-level statistics
- `hourly_stats/` - Hourly traffic patterns
- `dow_stats/` - Day-of-week patterns

### 3. Quick Restart Capability
On restart, the script:
1. Checks if checkpoints exist
2. Loads from checkpoint if available
3. Skips recomputation for completed steps
4. Only computes missing steps

## Usage

### First Run (Full Computation)
```bash
./run_descriptive_stats.sh
```
- Processes all 1.6B records
- Computes all statistics
- Saves checkpoints for future runs

### Subsequent Runs (Fast Restart)
```bash
./run_descriptive_stats.sh
```
- Automatically detects checkpoints
- Loads from saved Parquet files
- Skips completed computations
- Much faster execution

### Clear Checkpoints
```bash
./clear_checkpoints.sh
```
Interactive menu allows you to:
1. Clear ALL checkpoints (full recomputation)
2. Clear only statistics (keep valid_data)
3. Clear only valid_data
4. Cancel

Or manually:
```bash
# Clear all
rm -rf traffic_checkpoints/

# Clear specific checkpoint
rm -rf traffic_checkpoints/district_stats/
```

## Performance Benefits

### Without Checkpoints
- Load 1.6B CSV records: ~15-30 minutes
- Data validation: ~5-10 minutes
- Each statistic computation: ~2-5 minutes each
- **Total first run**: ~30-60 minutes

### With Checkpoints (Restart)
- Load valid_data Parquet: ~2-3 minutes
- Load statistics from checkpoints: ~10-30 seconds each
- **Total restart time**: ~3-5 minutes

## Checkpoint Structure

```
traffic_checkpoints/
├── valid_data/
│   ├── part-00000-*.parquet
│   ├── part-00001-*.parquet
│   └── ... (multiple partitions)
├── district_stats/
│   └── part-00000-*.parquet
├── road_stats/
│   └── part-00000-*.parquet
├── lane_stats/
│   └── part-00000-*.parquet
├── hourly_stats/
│   └── part-00000-*.parquet
└── dow_stats/
    └── part-00000-*.parquet
```

## Best Practices

1. **Keep valid_data checkpoint**: This is the most expensive to recompute
2. **Clear statistics checkpoints**: If you modify analysis logic
3. **Clear all checkpoints**: If source data changes
4. **Monitor disk space**: Checkpoints use ~50-100GB for this dataset

## Troubleshooting

### Checkpoint corrupted?
```bash
# Clear that specific checkpoint
rm -rf traffic_checkpoints/district_stats/
# Re-run script - it will recompute only that step
./run_descriptive_stats.sh
```

### Source data changed?
```bash
# Clear all checkpoints to force full recomputation
./clear_checkpoints.sh  # Choose option 1
./run_descriptive_stats.sh
```

### Out of disk space?
```bash
# Clear statistics but keep valid_data
./clear_checkpoints.sh  # Choose option 2
```

## Technical Details

### Parquet Format Benefits
- **Columnar storage**: Only reads columns you need
- **Compression**: Typically 5-10x smaller than CSV
- **Type preservation**: No schema inference needed
- **Predicate pushdown**: Faster filtering
- **Splittable**: Automatic parallelization

### Checkpoint Detection
```python
def checkpoint_exists(spark, path):
    """Check if a checkpoint path exists"""
    try:
        spark.read.parquet(path).limit(1).count()
        return True
    except:
        return False
```

### Example: District Stats Checkpoint
```python
if checkpoint_exists(spark, DISTRICT_CHECKPOINT):
    # Load from checkpoint
    district_stats = spark.read.parquet(DISTRICT_CHECKPOINT)
else:
    # Compute and save checkpoint
    district_stats = statistics_by_district(df_numeric)
    district_stats.write.mode("overwrite").parquet(DISTRICT_CHECKPOINT)
```
