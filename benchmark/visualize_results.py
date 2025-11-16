import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re

def visualize_benchmark_results(csv_path="benchmark/all_test.csv", output_dir="benchmark/visualizations"):
    """
    Generates visualizations from benchmark results.

    Args:
        csv_path (str): Path to the all_test.csv file.
        output_dir (str): Directory to save the generated plots.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print(f"Error: CSV file not found at {csv_path}")
        return
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return

    # Convert timestamp to datetime and ensure numeric types
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['duration_seconds'] = pd.to_numeric(df['duration_seconds'])
    df['records_processed'] = pd.to_numeric(df['records_processed'])
    df['total_records'] = pd.to_numeric(df['total_records'])


    # Extract dataset percentage from 'details' column for scaling analysis
    def extract_dataset_percentage(details_str):
        match = re.search(r'subset_(\d+)pct', details_str)
        if match:
            return int(match.group(1))
        return None
    
    df['dataset_percentage'] = df['details'].apply(extract_dataset_percentage)
    print("Dataset percentages extracted for visualization.")
    print(df['dataset_percentage'].unique())
    
    # visualization 1: Execution Time Break down Comparison Across Tools in differnet data size by every tool
    def visualization_1(dataframe, output_directory):
        tools = dataframe['tool'].unique().tolist()
        print("Tools found for visualization 1:", tools)

        for tool in tools:
            tool_df = dataframe[dataframe['tool'] == tool]
            plt.figure(figsize=(12, 8))
            sns.barplot(x='phase', y='duration_seconds', hue='dataset_percentage', data=tool_df)
            plt.title(f'Execution Time Breakdown for {tool} by Phase and Dataset Size')
            plt.xlabel('Phase')
            plt.ylabel('Duration (seconds)')
            plt.legend(title='Dataset Percentage')
            plt.xticks(rotation=45)
            plt.tight_layout()
            output_path = os.path.join(output_directory, f'{tool}_execution_time_breakdown.png')
            plt.savefig(output_path)
            plt.close()
            print(f"Saved visualization for {tool} at {output_path}")
    # visualization 1.1: Execution Time Comparison in per phase 



    # visualization 2: Total Execution Time vs. Data Size
    def visualization_2(dataframe, output_directory):
        total_time_df = dataframe[dataframe['phase'] == 'Total Execution']
        
        plt.figure(figsize=(12, 8))
        sns.lineplot(x='dataset_percentage', y='duration_seconds', hue='tool', data=total_time_df, marker='o')
        plt.title('Total Execution Time vs. Dataset Size')
        plt.xlabel('Dataset Size (%)')
        plt.ylabel('Total Execution Time (seconds)')
        plt.grid(True)
        plt.legend(title='Tool')
        plt.tight_layout()
        output_path = os.path.join(output_directory, 'total_execution_time_vs_size.png')
        plt.savefig(output_path)
        plt.close()
        print(f"Saved total execution time visualization at {output_path}")

    # visualization 3: Phase-by-Phase Execution Time Breakdown per Tool
    def visualization_3(dataframe, output_directory):
        # Use the largest dataset size for a clear comparison
        largest_percentage = dataframe['dataset_percentage'].max()
        if pd.isna(largest_percentage):
            print("Could not determine the largest dataset percentage for visualization 3. Skipping.")
            return
            
        df_largest = dataframe[dataframe['dataset_percentage'] == largest_percentage]
        
        # Focus on core phases
        core_phases_df = df_largest[df_largest['phase'].isin(['Read Data', 'Clean Data', 'Execute Analysis'])]

        plt.figure(figsize=(12, 8))
        sns.barplot(x='phase', y='duration_seconds', hue='tool', data=core_phases_df)
        plt.title(f'Phase-by-Phase Execution Time Breakdown ({int(largest_percentage)}% Dataset)')
        plt.xlabel('Phase')
        plt.ylabel('Duration (seconds)')
        plt.legend(title='Tool')
        plt.tight_layout()
        output_path = os.path.join(output_directory, 'phase_breakdown_per_tool.png')
        plt.savefig(output_path)
        plt.close()
        print(f"Saved phase-by-phase breakdown visualization at {output_path}")

    
    visualization_1(df, output_dir)
    visualization_2(df, output_dir)
    visualization_3(df, output_dir)

    




if __name__ == "__main__":
    visualize_benchmark_results()
