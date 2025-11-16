I have created a Python script `benchmark/visualize_results.py` to generate visualizations from your `benchmark/all_test.csv` file.

To run the script, you need to have `pandas`, `matplotlib`, and `seaborn` installed. If you don't have them, you can install them using pip:
```bash
pip install pandas matplotlib seaborn
```

Then, you can run the script from your terminal:
```bash
python3 benchmark/visualize_results.py
```

The script will generate three PNG image files in the `benchmark/visualizations/` directory:
1. `total_execution_time_comparison.png`: Compares the total execution time of different tools.
2. `phase_breakdown_per_tool.png`: Shows the phase-by-phase breakdown of execution time for each tool.
3. `scaling_with_dataset_size.png`: Illustrates how execution time scales with dataset size for each tool.

Please run the script and check the `benchmark/visualizations/` directory for the generated plots.