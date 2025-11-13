import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import glob
import re
import math
from typing import List, Dict

# --- Global Configuration ---
# Easily change the "focus" for detailed plots here
FOCUS_CONFIG = '1M-R10'
# Define a record count for the cross-configuration comparison
COMPARISON_RECORDS = 100_000_000

plt.style.use('default')
PLOT_STYLE = {
    'figure.figsize': (14, 7), 'font.size': 12, 'axes.titlesize': 16,
    'axes.labelsize': 14, 'xtick.labelsize': 12, 'ytick.labelsize': 12,
    'legend.fontsize': 11, 'lines.linewidth': 3, 'lines.markersize': 9,
    'axes.grid': True, 'grid.alpha': 0.4, 'axes.spines.top': False,
    'axes.spines.right': False, 'font.family': 'serif'
}
plt.rcParams.update(PLOT_STYLE)

# Define color palettes
COLORS = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']
ELEMENT_COLORS = {'Actual': '#2196F3', 'Theoretical': '#4CAF50', 'SST': '#FF9800'}


def load_and_prepare_data_partition(file_path: str) -> pd.DataFrame:
    """Loads and preprocesses the scalability data from the provided CSV."""
    print(f"Loading data from {file_path}...")
    try:
        df = pd.read_csv(file_path)
        print("Data loaded and preprocessed successfully.")
        return df
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return pd.DataFrame()
    except KeyError as e:
        print(f"Error: A required column was not found in the CSV: {e}")
        return pd.DataFrame()


def load_and_prepare_data(file_pattern: str) -> pd.DataFrame:
    """Loads and prepares bloom filter analysis data from multiple CSV files."""
    print("--- Loading and Preparing Data ---")
    all_data = []

    def extract_config(filename: str):
        match = re.search(r'(\d+M-R\d+)', Path(filename).stem)
        return match.group(1) if match else None

    def calculate_theoretical_blooms(records: int, ratio: int) -> int:
        leaf_count = math.ceil(records / 100000)
        if leaf_count <= 1: return 1
        total, current_nodes = leaf_count, leaf_count
        while current_nodes > 1:
            parents = math.ceil(current_nodes / ratio)
            total += parents
            current_nodes = parents
        return total

    for file_path in glob.glob(file_pattern):
        config_name = extract_config(file_path)
        if not config_name: continue
        
        try:
            ratio = int(re.search(r'-R(\d+)', config_name).group(1))
            df = pd.read_csv(file_path).apply(pd.to_numeric, errors='coerce').dropna()
            
            for _, row in df.iterrows():
                records = int(row['dbSize'])
                all_data.append({
                    'Config': config_name, 'Records': records,
                    'Selectivity': int(row['realDataPercentage']),
                    'Bloom_Checks': row['avgMultiBloomChecks'],
                    'Leaf_Bloom_Checks': row['avgMultiLeafBloomChecks'],
                    'SST_Checks': row['avgMultiSSTChecks'],
                    'Theoretical_Blooms': calculate_theoretical_blooms(records, ratio)
                })
            print(f"  > Processed {file_path}")
        except Exception as e:
            print(f"  - Error processing {file_path}: {e}")

    print("--- Data Preparation Complete ---\n")
    return pd.DataFrame(all_data)

def _plot_efficiency_vs_dbsize(ax: plt.Axes, data: pd.DataFrame, config: str, color: str):
    """Plots bloom checks vs theoretical tree size for a focus config."""
    subset = data[(data['Config'] == config) & (data['Selectivity'] == 0)].sort_values('Records')
    if subset.empty:
        ax.text(0.5, 0.5, f'No data for focus config:\n{config}', ha='center', va='center'); return

    x = np.arange(len(subset))
    ax.bar(x - 0.2, subset['Bloom_Checks']/3, 0.4, label='Filters Checked in a Single Tree', color=color)
    ax.bar(x + 0.2, subset['Theoretical_Blooms'], 0.4, label='Filters in a Single Tree', color=ELEMENT_COLORS['Theoretical'])
    ax.set_title(f'Utilization vs. DB Size ({config})', fontweight='bold')
    ax.set_xlabel('Database Size', fontweight='bold')
    ax.set_xticks(x); ax.set_xticklabels([f"{r/1e6:.0f}M" for r in subset['Records']])
    ax.legend()

def _plot_efficiency_vs_config(ax: plt.Axes, data: pd.DataFrame, records: int):
    """Compares efficiency across all detected configs for a fixed DB size."""
    subset = data[(data['Records'] == records) & (data['Selectivity'] == 0)].sort_values('Config')
    if subset.empty:
        ax.text(0.5, 0.5, f'No data for {records/1e6:.0f}M records', ha='center', va='center'); return
        
    x = np.arange(len(subset))
    ax.bar(x - 0.2, subset['Bloom_Checks']/3, 0.4, label='Filters Checked in a Single Tree', color=ELEMENT_COLORS['Actual'])
    ax.bar(x + 0.2, subset['Theoretical_Blooms'], 0.4, label='Filters in a Single Tree', color=ELEMENT_COLORS['Theoretical'])
    ax.set_title(f'Efficiency vs. Configuration ({records/1e6:.0f}M Recs)', fontweight='bold')
    ax.set_xlabel('Configuration', fontweight='bold')
    ax.set_xticks(x); ax.set_xticklabels(subset['Config'])
    ax.legend()

def create_efficiency_plots(data: pd.DataFrame, focus_config: str, all_configs: List[str], comparison_records: int):
    """Creates individual and combined plots for bloom filter efficiency."""
    print("--- Generating Bloom Efficiency Plots ---")
    Path("plots").mkdir(exist_ok=True, parents=True)
    focus_color = COLORS[all_configs.index(focus_config) % len(COLORS)]

    # Individual plots
    fig, ax = plt.subplots(figsize=(8, 6))
    _plot_efficiency_vs_dbsize(ax, data, focus_config, focus_color)
    ax.set_ylabel('Number of Checks / Filters', fontweight='bold')
    plt.tight_layout(); plt.savefig(f"plots/efficiency_vs_dbsize_{focus_config}.png", dpi=300); plt.close()
    print(f"  > Individual plot saved: plots/efficiency_vs_dbsize_{focus_config}.png")

    fig, ax = plt.subplots(figsize=(8, 6))
    _plot_efficiency_vs_config(ax, data, comparison_records)
    ax.set_ylabel('Number of Checks / Filters', fontweight='bold')
    plt.tight_layout(); plt.savefig(f"plots/efficiency_vs_config_{int(comparison_records/1e6)}M.png", dpi=300); plt.close()
    print(f"  > Individual plot saved: plots/efficiency_vs_config_{int(comparison_records/1e6)}M.png")
    
    # Combined plot
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6), sharey=True)
    fig.suptitle('Bloom Filter Utilization and Efficiency', fontsize=18, fontweight='bold')
    _plot_efficiency_vs_dbsize(ax1, data, focus_config, focus_color)
    _plot_efficiency_vs_config(ax2, data, comparison_records)
    ax1.set_ylabel('Number of Checks / Filters', fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95]); plt.savefig("plots/efficiency_combined.png", dpi=300); plt.close()
    print(f"  > Combined plot saved: plots/efficiency_combined.png")

def create_leaf_resource_plots(data: pd.DataFrame, all_configs: List[str], comparison_records: int):
    """Creates a plot for each config showing leaf-level resource use vs. selectivity."""
    print("\n--- Generating Leaf Resource Plots (One Per Config) ---")
    
    for i, config in enumerate(all_configs):
        subset = data[(data['Config'] == config) & (data['Records'] == comparison_records)].sort_values('Selectivity')
        if subset.empty: continue

        fig, ax = plt.subplots(figsize=(8, 6))
        color = COLORS[i % len(COLORS)]
        ax.plot(subset['Selectivity'], subset['Leaf_Bloom_Checks'], 'o-', label='Leaf Bloom Checks', color=color)
        ax.plot(subset['Selectivity'], subset['SST_Checks'], 's-', label='SST File Checks', color=ELEMENT_COLORS['SST'])
        
        ax.set_title(f'Selectivity Impact ({config}, {comparison_records/1e6:.0f}M Recs)', fontweight='bold')
        ax.set_xlabel('Query Selectivity (%)', fontweight='bold')
        ax.set_ylabel('Number of Checks', fontweight='bold')

        ax.legend()
        plt.tight_layout()
        save_path = f"plots/leaf_resource_{config}.png"
        plt.savefig(save_path, dpi=300); plt.close()
        print(f"  > Plot saved: {save_path}")


def partition_efficiency_plots(data: pd.DataFrame, comparison_records: int):
    """Creates individual  plots for partition efficiency."""
    print("--- Generating Partition Efficiency Plots ---")
    Path("plots").mkdir(exist_ok=True, parents=True)

    # Individual plots
    fig, ax = plt.subplots(figsize=(10, 8))
    _partition_efficiency(ax, data)
    ax.set_ylabel('Query Latency (ms)', fontweight='bold')
    plt.tight_layout(); plt.savefig(f"plots/partition_efficiency.png", dpi=300); plt.close()
    print(f"  > Individual plot saved: plots/partition_efficiency.png")


def _partition_efficiency(ax: plt.Axes, data: pd.DataFrame):
    subset = data[(data['realDataPercentage'] == 0)].sort_values('itemsPerPartition')
    subset1 = data[(data['realDataPercentage'] == 20)].sort_values('itemsPerPartition')
    subset2 = data[(data['realDataPercentage'] == 50)].sort_values('itemsPerPartition')
    if subset.empty:
            ax.text(0.5, 0.5, f'No data for focus ', ha='center', va='center'); return

    x = np.arange(len(subset))
    ax.bar(x - 0.2, subset['avgMultiTime']/1000, 0.2, label='Selectivity 0%')
    ax.bar(x, subset1['avgMultiTime']/1000, 0.2, label='Selectivity 20%', color=ELEMENT_COLORS['Theoretical'])
    ax.bar(x + 0.2, subset2['avgMultiTime']/1000, 0.2, label='Selectivity 50% ', color=ELEMENT_COLORS['SST'])
    ax.set_title(f'Partition size efficiency', fontweight='bold')
    ax.set_xlabel('Partition size', fontweight='bold')
    #ax.set_xticks(x); ax.set_xticklabels([f"{r/1e6:.0f}M" for r in subset['itemsPerPartition']])
    ax.set_xticks(x); ax.set_xticklabels([f"{r}" for r in subset['itemsPerPartition']])

    ax.legend()

def main():
    """Main function to run the complete analysis."""
    # Use the provided file names
    data = load_and_prepare_data("data/exp_1_comprehensive_checks_*.csv")
    
    if not data.empty:
        all_configs = sorted(data['Config'].unique())
        focus_config = FOCUS_CONFIG if FOCUS_CONFIG in all_configs else all_configs[0]
        if focus_config != FOCUS_CONFIG:
            print(f"\nWarning: Focus config '{FOCUS_CONFIG}' not found. Falling back to '{focus_config}'.")

    #    create_efficiency_plots(data, focus_config, all_configs, COMPARISON_RECORDS)
    #    create_leaf_resource_plots(data, all_configs, COMPARISON_RECORDS)
        print("\nAll plots generated successfully.")
    else:
        print("\nNo data loaded. Stopping script.")


    data_partition = load_and_prepare_data_partition("data/exp_5_partition_efficiency.csv")
    if not data_partition.empty:
 
        partition_efficiency_plots(data_partition, COMPARISON_RECORDS)
        print("\nAll plots generated successfully.")
    else:
        print("\nNo data loaded. Stopping script.")


if __name__ == "__main__":
    main()