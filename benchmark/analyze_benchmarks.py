#!/usr/bin/env python3
"""
Comprehensive Benchmark Analysis and Visualization

This script analyzes the benchmark results comparing PySpark, Hive, and Pandas
across different dataset sizes (10% to 90% subsets).

It generates:
1. Performance comparison charts
2. Scalability analysis
3. Phase-by-phase breakdowns
4. Statistical summaries
"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path

# Set style for better-looking plots
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

def load_data(csv_path):
    """Load and preprocess benchmark data."""
    df = pd.read_csv(csv_path)

    # Extract dataset size percentage from details field
    df['dataset_pct'] = df['details'].str.extract(r'subset_(\d+)pct')[0].astype(float)

    # Fill NaN dataset_pct values by propagating within the same uuid
    # (Total Execution rows don't have path in details, just SUCCESS/FAILED)
    df['dataset_pct'] = df.groupby('uuid')['dataset_pct'].ffill().bfill()

    # Convert duration to float if needed
    df['duration_seconds'] = pd.to_numeric(df['duration_seconds'], errors='coerce')

    # Extract framework and analysis type from tool name
    # Tool format: "Framework-AnalysisType" (e.g., "PySpark-BusyRoad")
    df[['framework', 'analysis_type']] = df['tool'].str.split('-', n=1, expand=True)

    return df

def get_summary_stats(df):
    """Calculate summary statistics by tool, dataset size, and phase."""
    # Group by uuid to get per-test statistics
    summary = df.groupby(['uuid', 'tool', 'dataset_pct', 'phase', 'total_records']).agg({
        'duration_seconds': 'sum',
        'timestamp': 'first'
    }).reset_index()

    return summary

def get_total_execution_times(df):
    """Extract total execution times for each test run."""
    total_times = df[df['phase'] == 'Total Execution'].copy()

    # Calculate average across multiple runs
    avg_times = total_times.groupby(['tool', 'dataset_pct', 'total_records']).agg({
        'duration_seconds': ['mean', 'std', 'min', 'max', 'count']
    }).reset_index()

    avg_times.columns = ['tool', 'dataset_pct', 'total_records', 'mean_duration',
                         'std_duration', 'min_duration', 'max_duration', 'num_runs']

    return avg_times

def get_phase_breakdown(df):
    """Get average duration for each phase by tool and dataset size."""
    # Exclude Total Execution to avoid double counting
    phases = df[df['phase'] != 'Total Execution'].copy()

    avg_phases = phases.groupby(['tool', 'dataset_pct', 'phase']).agg({
        'duration_seconds': 'mean'
    }).reset_index()

    return avg_phases

def plot_total_execution_comparison(avg_times, output_dir):
    """Plot total execution time comparison across tools and dataset sizes."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Plot 1: Line plot showing execution time vs dataset size
    for tool in avg_times['tool'].unique():
        tool_data = avg_times[avg_times['tool'] == tool].sort_values('dataset_pct')
        ax1.plot(tool_data['dataset_pct'], tool_data['mean_duration'],
                marker='o', linewidth=2, label=tool, markersize=8)

        # Add error bars
        ax1.fill_between(tool_data['dataset_pct'],
                         tool_data['mean_duration'] - tool_data['std_duration'],
                         tool_data['mean_duration'] + tool_data['std_duration'],
                         alpha=0.2)

    ax1.set_xlabel('Dataset Size (%)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax1.set_title('Total Execution Time vs Dataset Size', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)

    # Plot 2: Bar plot for direct comparison at each dataset size
    dataset_sizes = sorted(avg_times['dataset_pct'].unique())
    x = np.arange(len(dataset_sizes))
    width = 0.25

    tools = sorted(avg_times['tool'].unique())
    colors = ['#2E86AB', '#A23B72', '#F18F01']

    for idx, tool in enumerate(tools):
        tool_data = avg_times[avg_times['tool'] == tool].sort_values('dataset_pct')
        offset = (idx - 1) * width

        # Create array with NaN for missing data points
        heights = []
        positions = []
        for size_idx, size in enumerate(dataset_sizes):
            matching_data = tool_data[tool_data['dataset_pct'] == size]
            if len(matching_data) > 0:
                heights.append(matching_data.iloc[0]['mean_duration'])
                positions.append(size_idx)

        if heights:  # Only plot if there's data
            bars = ax2.bar([x[p] + offset for p in positions], heights, width,
                          label=tool, color=colors[idx % len(colors)], alpha=0.8)

            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                if height > 0:
                    ax2.text(bar.get_x() + bar.get_width()/2., height,
                            f'{height:.1f}s',
                            ha='center', va='bottom', fontsize=8)

    ax2.set_xlabel('Dataset Size (%)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Execution Time (seconds)', fontsize=12, fontweight='bold')
    ax2.set_title('Tool Comparison by Dataset Size', fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels([f'{int(size)}%' for size in dataset_sizes])
    ax2.legend(fontsize=11)
    ax2.grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(f'{output_dir}/01_total_execution_comparison.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_dir}/01_total_execution_comparison.png")
    plt.close()

def plot_phase_breakdown(phase_data, output_dir):
    """Plot phase-by-phase breakdown for each tool."""
    tools = sorted(phase_data['tool'].unique())

    fig, axes = plt.subplots(1, len(tools), figsize=(18, 6))
    if len(tools) == 1:
        axes = [axes]

    phases_order = ['Initialize', 'Read Data', 'Clean Data', 'Execute Analysis', 'Cleanup']
    colors = plt.cm.Set3(np.linspace(0, 1, len(phases_order)))

    for idx, tool in enumerate(tools):
        tool_data = phase_data[phase_data['tool'] == tool]
        dataset_sizes = sorted(tool_data['dataset_pct'].unique())

        # Create stacked bar chart
        bottom = np.zeros(len(dataset_sizes))

        for phase_idx, phase in enumerate(phases_order):
            phase_values = []
            for size in dataset_sizes:
                val = tool_data[(tool_data['dataset_pct'] == size) &
                               (tool_data['phase'] == phase)]['duration_seconds'].values
                phase_values.append(val[0] if len(val) > 0 else 0)

            axes[idx].bar(range(len(dataset_sizes)), phase_values,
                         bottom=bottom, label=phase, color=colors[phase_idx], alpha=0.8)
            bottom += phase_values

        axes[idx].set_xlabel('Dataset Size (%)', fontsize=11, fontweight='bold')
        axes[idx].set_ylabel('Time (seconds)', fontsize=11, fontweight='bold')
        axes[idx].set_title(f'{tool} - Phase Breakdown', fontsize=12, fontweight='bold')
        axes[idx].set_xticks(range(len(dataset_sizes)))
        axes[idx].set_xticklabels([f'{int(s)}%' for s in dataset_sizes], rotation=45)
        axes[idx].legend(fontsize=8, loc='upper left')
        axes[idx].grid(True, alpha=0.3, axis='y')

    plt.tight_layout()
    plt.savefig(f'{output_dir}/02_phase_breakdown.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_dir}/02_phase_breakdown.png")
    plt.close()

def plot_scalability_analysis(avg_times, output_dir):
    """Analyze and plot scalability (time vs data size)."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Plot 1: Throughput (records/second)
    avg_times['throughput'] = avg_times['total_records'] / avg_times['mean_duration']

    for tool in avg_times['tool'].unique():
        tool_data = avg_times[avg_times['tool'] == tool].sort_values('dataset_pct')
        ax1.plot(tool_data['total_records'] / 1e6, tool_data['throughput'] / 1e6,
                marker='o', linewidth=2, label=tool, markersize=8)

    ax1.set_xlabel('Dataset Size (Million Records)', fontsize=12, fontweight='bold')
    ax1.set_ylabel('Throughput (Million Records/Second)', fontsize=12, fontweight='bold')
    ax1.set_title('Processing Throughput by Tool', fontsize=14, fontweight='bold')
    ax1.legend(fontsize=11)
    ax1.grid(True, alpha=0.3)

    # Plot 2: Speedup relative to smallest dataset
    for tool in avg_times['tool'].unique():
        tool_data = avg_times[avg_times['tool'] == tool].sort_values('dataset_pct')
        if len(tool_data) > 0:
            baseline_time = tool_data.iloc[0]['mean_duration']
            baseline_size = tool_data.iloc[0]['total_records']

            # Calculate ideal linear scaling
            ideal_scaling = (tool_data['total_records'] / baseline_size) * baseline_time
            actual_time = tool_data['mean_duration']

            # Efficiency (ideal/actual - closer to 1 is better)
            efficiency = ideal_scaling / actual_time

            ax2.plot(tool_data['dataset_pct'], efficiency,
                    marker='o', linewidth=2, label=tool, markersize=8)

    ax2.axhline(y=1.0, color='gray', linestyle='--', label='Perfect Linear Scaling', alpha=0.5)
    ax2.set_xlabel('Dataset Size (%)', fontsize=12, fontweight='bold')
    ax2.set_ylabel('Scaling Efficiency', fontsize=12, fontweight='bold')
    ax2.set_title('Scaling Efficiency (Higher is Better)', fontsize=14, fontweight='bold')
    ax2.legend(fontsize=11)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/03_scalability_analysis.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_dir}/03_scalability_analysis.png")
    plt.close()

def plot_phase_comparison(phase_data, output_dir):
    """Compare specific phases across tools."""
    phases_to_compare = ['Read Data', 'Clean Data', 'Execute Analysis']

    fig, axes = plt.subplots(1, len(phases_to_compare), figsize=(18, 5))

    for idx, phase in enumerate(phases_to_compare):
        phase_subset = phase_data[phase_data['phase'] == phase]

        for tool in sorted(phase_subset['tool'].unique()):
            tool_data = phase_subset[phase_subset['tool'] == tool].sort_values('dataset_pct')
            axes[idx].plot(tool_data['dataset_pct'], tool_data['duration_seconds'],
                          marker='o', linewidth=2, label=tool, markersize=8)

        axes[idx].set_xlabel('Dataset Size (%)', fontsize=11, fontweight='bold')
        axes[idx].set_ylabel('Time (seconds)', fontsize=11, fontweight='bold')
        axes[idx].set_title(f'{phase} - Tool Comparison', fontsize=12, fontweight='bold')
        axes[idx].legend(fontsize=10)
        axes[idx].grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/04_phase_comparison.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_dir}/04_phase_comparison.png")
    plt.close()

def plot_heatmap_comparison(avg_times, output_dir):
    """Create heatmap showing execution times."""
    if len(avg_times) == 0:
        print("⚠ Skipping heatmap - no data available")
        return

    # Pivot data for heatmap
    heatmap_data = avg_times.pivot(index='tool', columns='dataset_pct', values='mean_duration')

    if heatmap_data.empty:
        print("⚠ Skipping heatmap - pivot resulted in empty data")
        return

    plt.figure(figsize=(12, 6))
    sns.heatmap(heatmap_data, annot=True, fmt='.2f', cmap='YlOrRd',
                cbar_kws={'label': 'Execution Time (seconds)'})
    plt.xlabel('Dataset Size (%)', fontsize=12, fontweight='bold')
    plt.ylabel('Tool', fontsize=12, fontweight='bold')
    plt.title('Execution Time Heatmap (seconds)', fontsize=14, fontweight='bold')
    plt.tight_layout()
    plt.savefig(f'{output_dir}/05_heatmap_comparison.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_dir}/05_heatmap_comparison.png")
    plt.close()

def plot_speedup_ratios(avg_times, output_dir):
    """Calculate and plot speedup ratios between tools."""
    dataset_sizes = sorted(avg_times['dataset_pct'].unique())

    fig, ax = plt.subplots(figsize=(14, 6))

    # Get unique tools (which are now just framework names within same analysis type)
    tools = sorted(avg_times['tool'].unique())

    # Generate all pairwise comparisons
    comparisons = []
    if 'PySpark' in [t.split('-')[0] for t in tools] and 'Hive' in [t.split('-')[0] for t in tools]:
        pyspark_tool = [t for t in tools if t.split('-')[0] == 'PySpark'][0] if [t for t in tools if t.split('-')[0] == 'PySpark'] else None
        hive_tool = [t for t in tools if t.split('-')[0] == 'Hive'][0] if [t for t in tools if t.split('-')[0] == 'Hive'] else None
        if pyspark_tool and hive_tool:
            comparisons.append((pyspark_tool, hive_tool, 'PySpark vs Hive'))

    if 'PySpark' in [t.split('-')[0] for t in tools] and 'Pandas' in [t.split('-')[0] for t in tools]:
        pyspark_tool = [t for t in tools if t.split('-')[0] == 'PySpark'][0] if [t for t in tools if t.split('-')[0] == 'PySpark'] else None
        pandas_tool = [t for t in tools if t.split('-')[0] == 'Pandas'][0] if [t for t in tools if t.split('-')[0] == 'Pandas'] else None
        if pyspark_tool and pandas_tool:
            comparisons.append((pyspark_tool, pandas_tool, 'PySpark vs Pandas'))

    if 'Hive' in [t.split('-')[0] for t in tools] and 'Pandas' in [t.split('-')[0] for t in tools]:
        hive_tool = [t for t in tools if t.split('-')[0] == 'Hive'][0] if [t for t in tools if t.split('-')[0] == 'Hive'] else None
        pandas_tool = [t for t in tools if t.split('-')[0] == 'Pandas'][0] if [t for t in tools if t.split('-')[0] == 'Pandas'] else None
        if hive_tool and pandas_tool:
            comparisons.append((hive_tool, pandas_tool, 'Hive vs Pandas'))

    for base_tool, compare_tool, label in comparisons:
        speedups = []
        sizes_with_data = []

        for size in dataset_sizes:
            base_data = avg_times[(avg_times['tool'] == base_tool) &
                                 (avg_times['dataset_pct'] == size)]
            compare_data = avg_times[(avg_times['tool'] == compare_tool) &
                                    (avg_times['dataset_pct'] == size)]

            if len(base_data) > 0 and len(compare_data) > 0:
                # Speedup: compare_time / base_time (> 1 means base is faster)
                speedup = compare_data.iloc[0]['mean_duration'] / base_data.iloc[0]['mean_duration']
                speedups.append(speedup)
                sizes_with_data.append(size)

        if speedups:
            ax.plot(sizes_with_data, speedups, marker='o', linewidth=2,
                   label=label, markersize=8)

    ax.axhline(y=1.0, color='gray', linestyle='--', alpha=0.5,
              label='Equal Performance')
    ax.set_xlabel('Dataset Size (%)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Speedup Ratio', fontsize=12, fontweight='bold')
    ax.set_title('Performance Speedup Ratios (>1 means first tool is faster)',
                fontsize=14, fontweight='bold')
    ax.legend(fontsize=11)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(f'{output_dir}/06_speedup_ratios.png', dpi=300, bbox_inches='tight')
    print(f"✓ Saved: {output_dir}/06_speedup_ratios.png")
    plt.close()

def generate_statistics_report(df, avg_times, phase_data, output_dir):
    """Generate detailed statistics report."""
    report_lines = []
    report_lines.append("=" * 80)
    report_lines.append("BENCHMARK ANALYSIS REPORT")
    report_lines.append("=" * 80)
    report_lines.append("")

    # Overview
    report_lines.append("1. OVERVIEW")
    report_lines.append("-" * 80)
    report_lines.append(f"Total test runs: {df['uuid'].nunique()}")
    report_lines.append(f"Tools tested: {', '.join(sorted(df['tool'].unique()))}")
    report_lines.append(f"Dataset sizes: {sorted(df['dataset_pct'].dropna().unique())}")
    report_lines.append(f"Phases tracked: {', '.join(sorted(df['phase'].unique()))}")
    report_lines.append("")

    # Overall performance summary
    report_lines.append("2. OVERALL PERFORMANCE SUMMARY")
    report_lines.append("-" * 80)
    for tool in sorted(avg_times['tool'].unique()):
        tool_data = avg_times[avg_times['tool'] == tool]
        report_lines.append(f"\n{tool}:")
        report_lines.append(f"  Average execution time: {tool_data['mean_duration'].mean():.2f}s")
        report_lines.append(f"  Min execution time: {tool_data['mean_duration'].min():.2f}s")
        report_lines.append(f"  Max execution time: {tool_data['mean_duration'].max():.2f}s")
        report_lines.append(f"  Total runs: {tool_data['num_runs'].sum():.0f}")
    report_lines.append("")

    # Best performer by dataset size
    report_lines.append("3. FASTEST TOOL BY DATASET SIZE")
    report_lines.append("-" * 80)
    for size in sorted(avg_times['dataset_pct'].unique()):
        size_data = avg_times[avg_times['dataset_pct'] == size].sort_values('mean_duration')
        if len(size_data) > 0:
            fastest = size_data.iloc[0]
            report_lines.append(f"{int(size)}% dataset: {fastest['tool']} ({fastest['mean_duration']:.2f}s)")
    report_lines.append("")

    # Phase analysis
    report_lines.append("4. PHASE ANALYSIS (AVERAGE TIMES)")
    report_lines.append("-" * 80)
    for phase in ['Read Data', 'Clean Data', 'Execute Analysis']:
        report_lines.append(f"\n{phase}:")
        phase_subset = phase_data[phase_data['phase'] == phase]
        for tool in sorted(phase_subset['tool'].unique()):
            tool_phase = phase_subset[phase_subset['tool'] == tool]
            avg_time = tool_phase['duration_seconds'].mean()
            report_lines.append(f"  {tool}: {avg_time:.3f}s (avg)")
    report_lines.append("")

    # Scalability insights
    report_lines.append("5. SCALABILITY INSIGHTS")
    report_lines.append("-" * 80)
    for tool in sorted(avg_times['tool'].unique()):
        tool_data = avg_times[avg_times['tool'] == tool].sort_values('dataset_pct')
        if len(tool_data) >= 2:
            first = tool_data.iloc[0]
            last = tool_data.iloc[-1]
            size_increase = last['total_records'] / first['total_records']
            time_increase = last['mean_duration'] / first['mean_duration']
            efficiency = size_increase / time_increase

            report_lines.append(f"\n{tool}:")
            report_lines.append(f"  Dataset size increased: {size_increase:.1f}x")
            report_lines.append(f"  Execution time increased: {time_increase:.1f}x")
            report_lines.append(f"  Scaling efficiency: {efficiency:.2f} (1.0 = perfect linear)")
    report_lines.append("")

    # Recommendations
    report_lines.append("6. RECOMMENDATIONS")
    report_lines.append("-" * 80)

    # Find overall fastest
    overall_avg = avg_times.groupby('tool')['mean_duration'].mean().sort_values()
    fastest_overall = overall_avg.index[0]

    report_lines.append(f"• Overall fastest tool: {fastest_overall}")

    # Find best scaling
    scalability_scores = {}
    for tool in sorted(avg_times['tool'].unique()):
        tool_data = avg_times[avg_times['tool'] == tool].sort_values('dataset_pct')
        if len(tool_data) >= 2:
            first = tool_data.iloc[0]
            last = tool_data.iloc[-1]
            size_increase = last['total_records'] / first['total_records']
            time_increase = last['mean_duration'] / first['mean_duration']
            scalability_scores[tool] = size_increase / time_increase

    if scalability_scores:
        best_scaling = max(scalability_scores, key=scalability_scores.get)
        report_lines.append(f"• Best scaling efficiency: {best_scaling}")

    report_lines.append("")
    report_lines.append("=" * 80)

    # Write report
    report_path = f"{output_dir}/benchmark_report.txt"
    with open(report_path, 'w') as f:
        f.write('\n'.join(report_lines))

    print(f"✓ Saved: {report_path}")

    # Also print to console
    print("\n" + "\n".join(report_lines))

def main():
    """Main analysis function."""
    print("\n" + "=" * 80)
    print("BENCHMARK DATA ANALYSIS")
    print("=" * 80 + "\n")

    # Setup paths
    csv_path = "benchmark/all_test.csv"
    base_output_dir = "benchmark/visualizations"
    Path(base_output_dir).mkdir(exist_ok=True)

    # Load data
    print("Loading data...")
    df = load_data(csv_path)
    print(f"✓ Loaded {len(df)} records from {csv_path}")
    print(f"  - Unique test runs: {df['uuid'].nunique()}")
    print(f"  - Tools: {', '.join(sorted(df['tool'].unique()))}")
    print(f"  - Analysis types: {', '.join(sorted(df['analysis_type'].unique()))}")
    print(f"  - Frameworks: {', '.join(sorted(df['framework'].unique()))}")
    print(f"  - Dataset sizes: {sorted(df['dataset_pct'].dropna().unique())}\n")

    # Process each analysis type separately
    analysis_types = sorted(df['analysis_type'].unique())

    for analysis_type in analysis_types:
        print("\n" + "=" * 80)
        print(f"PROCESSING: {analysis_type.upper()} ANALYSIS")
        print("=" * 80 + "\n")

        # Filter data for this analysis type
        df_filtered = df[df['analysis_type'] == analysis_type].copy()

        # Create separate output directory for this analysis type
        output_dir = f"{base_output_dir}/{analysis_type}"
        Path(output_dir).mkdir(exist_ok=True)

        print(f"Analyzing {analysis_type}...")
        print(f"  - Records: {len(df_filtered)}")
        print(f"  - Test runs: {df_filtered['uuid'].nunique()}")
        print(f"  - Frameworks: {', '.join(sorted(df_filtered['framework'].unique()))}\n")

        # Calculate summaries
        print("Calculating summaries...")
        avg_times = get_total_execution_times(df_filtered)
        phase_data = get_phase_breakdown(df_filtered)
        print(f"✓ Processed {len(avg_times)} test configurations\n")

        # Generate visualizations
        print("Generating visualizations...")
        plot_total_execution_comparison(avg_times, output_dir)
        plot_phase_breakdown(phase_data, output_dir)
        plot_scalability_analysis(avg_times, output_dir)
        plot_phase_comparison(phase_data, output_dir)
        plot_heatmap_comparison(avg_times, output_dir)
        plot_speedup_ratios(avg_times, output_dir)

        print("\n" + "-" * 80)
        print(f"Generating statistics report for {analysis_type}...")
        print("-" * 80)
        generate_statistics_report(df_filtered, avg_times, phase_data, output_dir)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE!")
    print("=" * 80)
    print(f"\nAll visualizations and reports saved to:")
    for analysis_type in analysis_types:
        print(f"  - {base_output_dir}/{analysis_type}/")

if __name__ == "__main__":
    main()
