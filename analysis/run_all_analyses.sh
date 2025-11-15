#!/bin/bash

# Master script to run all traffic data analyses
# Runs all analysis scripts in sequence

echo "========================================"
echo "Hong Kong Traffic Data Analysis Suite"
echo "========================================"
echo ""
echo "This will run all 4 analysis modules:"
echo "  1. Data Quality & EDA"
echo "  2. Temporal Pattern Analysis"
echo "  3. Spatial & Congestion Analysis"
echo "  4. Traffic Flow Theory"
echo ""
echo "========================================"
echo ""

# Set Spark submit configurations
SPARK_MEMORY="--driver-memory 8G --executor-memory 8G"
SPARK_CONF="--conf spark.driver.maxResultSize=4G --conf spark.executor.memoryOverhead=2G"

# Create logs directory
mkdir -p analysis_logs

# Timestamp for this run
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_DIR="analysis_logs/${TIMESTAMP}"
mkdir -p ${LOG_DIR}

echo "Logs will be saved to: ${LOG_DIR}"
echo ""

# Function to run analysis and log output
run_analysis() {
    local script_name=$1
    local analysis_name=$2
    local log_file="${LOG_DIR}/${script_name}.log"

    echo "========================================"
    echo "Running: ${analysis_name}"
    echo "Script: ${script_name}"
    echo "Log: ${log_file}"
    echo "========================================"
    echo ""

    start_time=$(date +%s)

    spark-submit ${SPARK_MEMORY} ${SPARK_CONF} ${script_name} 2>&1 | tee ${log_file}

    exit_code=${PIPESTATUS[0]}
    end_time=$(date +%s)
    duration=$((end_time - start_time))

    if [ ${exit_code} -eq 0 ]; then
        echo ""
        echo "✓ ${analysis_name} completed successfully in ${duration}s"
        echo ""
    else
        echo ""
        echo "✗ ${analysis_name} FAILED with exit code ${exit_code}"
        echo "Check log: ${log_file}"
        echo ""

        # Ask if user wants to continue
        read -p "Continue with remaining analyses? (y/n) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Analysis suite stopped."
            exit 1
        fi
    fi
}

# Record overall start time
overall_start=$(date +%s)

# Run each analysis in sequence
run_analysis "01_data_quality_eda.py" "Data Quality & EDA"
run_analysis "02_temporal_patterns.py" "Temporal Pattern Analysis"
run_analysis "03_spatial_congestion.py" "Spatial & Congestion Analysis"
run_analysis "04_traffic_flow_theory.py" "Traffic Flow Theory"

# Calculate total time
overall_end=$(date +%s)
total_duration=$((overall_end - overall_start))
hours=$((total_duration / 3600))
minutes=$(((total_duration % 3600) / 60))
seconds=$((total_duration % 60))

echo ""
echo "========================================"
echo "ALL ANALYSES COMPLETED"
echo "========================================"
echo "Total time: ${hours}h ${minutes}m ${seconds}s"
echo "Logs saved to: ${LOG_DIR}"
echo ""
echo "Results are in:"
echo "  - analysis_results/01_eda/"
echo "  - analysis_results/02_temporal_patterns/"
echo "  - analysis_results/03_spatial_congestion/"
echo "  - analysis_results/04_traffic_flow_theory/"
echo ""
echo "Next steps:"
echo "  1. Review the CSV outputs"
echo "  2. Create visualizations using Python/R"
echo "  3. Generate reports and dashboards"
echo "========================================"
