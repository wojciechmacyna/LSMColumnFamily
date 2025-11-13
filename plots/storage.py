import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from typing import List
import glob

# --- Configuration ---

# Consistent style settings for all plots
PLOT_STYLE = {
    'figure.figsize': (8, 6), 'font.size': 12, 'axes.titlesize': 16,
    'axes.labelsize': 14, 'xtick.labelsize': 12, 'ytick.labelsize': 12,
    'legend.fontsize': 11, 'lines.linewidth': 3, 'lines.markersize': 9,
    'axes.grid': True, 'grid.alpha': 0.5, 'axes.spines.top': False,
    'axes.spines.right': False, 'font.family': 'serif'
}

# Consistent colors for the breakdown plot
BREAKDOWN_COLORS = {'disk': '#A23B72', 'memory': '#F18F01', 'database': '#2E86AB','total': '#C73E1D'}

def load_and_prepare_data(file_paths: List[str]) -> pd.DataFrame:
    """
    Loads and processes data from CSV files for plotting.
    """
    all_data = []
    print("--- Loading and Processing Data ---")

    for file_path in file_paths:
        try:
            df = pd.read_csv(file_path)
            
            # --- Perform Calculations ---
            ratio = df['bloomTreeRatio'].iloc[0]
            config_name = file_path.replace('data/exp_1_bloom_metrics_', '').replace('.csv', '')
            df['Config'] = config_name
            df['Ratio'] = ratio
            
            # Calculate total size and convert to MB for breakdown plot
            df['Total_Bloom_Size_Bytes'] = df['bloomDiskSize'] + df['blomMemSize']
            df['Disk_MB'] = df['bloomDiskSize'] / (1024 * 1024)
            df['Memory_MB'] = df['blomMemSize'] / (1024 * 1024)
            
            # Estimate DB size and calculate final overhead percentage
            df['Database_Size_Bytes'] = df['numRecords'] * 26.8
            df['Storage_Percentage'] = (df['Total_Bloom_Size_Bytes'] / df['Database_Size_Bytes']) * 100
            
            all_data.append(df)
            print(f"  - Processed {file_path} (Config: {df['Config'].iloc[0]})")
            
            print("Bloom disk Size")
            print(df['Disk_MB'].values )
            print("Bloom memory Size")
            print(df['Memory_MB'].values)
            print("Storage_Percentage")
            print(df['Storage_Percentage'].values)
            print("Database_Size_Bytes")
            print(df['Database_Size_Bytes'].values /(1024*1024*1024))

        except Exception as e:
            print(f"  - Could not process {file_path}: {e}")
            
    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


def create_comparison_plot(data: pd.DataFrame, save_path: str):
    """
    Generates the main line plot comparing storage overhead across all configs.
    """
    if data.empty:
        return
        
    print(f"\n--- Generating Comparison Plot ---")
    plt.style.use('default')
    plt.rcParams.update(PLOT_STYLE)
    fig, ax = plt.subplots()

    # Map database sizes to evenly spaced positions
    x_labels = sorted(data['numRecords'].unique())
    value_to_index = {value: i for i, value in enumerate(x_labels)}
    
    configs = data.sort_values('Ratio')['Config'].unique()
    colors = plt.get_cmap('viridis')(np.linspace(0, 1, len(configs)))

    for i, config in enumerate(configs):
        config_data = data[data['Config'] == config].sort_values('numRecords')
        x_positions = config_data['numRecords'].map(value_to_index)
        ax.plot(x_positions, config_data['Storage_Percentage'], 'o-', label=config[-5:], color=colors[i])


    # Formatting
    ax.set_xticks(list(value_to_index.values()))
    ax.set_xticklabels([f'{int(val/1e6)}M' for val in x_labels])
    ax.set_xlabel('Database Size (Number of Records)')
    ax.set_ylabel('Total Bloom Filter Storage Overhead (%)')
    ax.legend(title='Configuration', frameon=True, fancybox=True, shadow=True)

    plt.tight_layout()
    Path(save_path).parent.mkdir(exist_ok=True)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    # plt.show()
    plt.close()
    print(f"  - Comparison plot saved to: {save_path}")

    print("Configs plotted")
    print(data['Config'])


def create_breakdown_plot(config_data: pd.DataFrame, save_path: str):
    """
    Generates a stacked bar chart showing the storage breakdown for one config.
    """
    if config_data.empty:
        return

    config_name = config_data['Config']
    print(f"  - Generating breakdown plot for {config_name}...")
    
    plt.style.use('default')
    plt.rcParams.update(PLOT_STYLE)
    fig, ax = plt.subplots()

    # Ensure data is sorted for consistent plotting
    config_data = config_data.sort_values('numRecords')
    
    x_labels = config_data['numRecords']
    x_positions = np.arange(len(x_labels))

    # Plot stacked bars
    ax.bar(x_positions, config_data['Disk_MB'], width=0.6, label='Disk Storage', color=BREAKDOWN_COLORS['disk'])
    ax.bar(x_positions, config_data['Memory_MB'], width=0.6, bottom=config_data['Disk_MB'], label='Memory Storage', color=BREAKDOWN_COLORS['memory'])

    # Add total size labels above each bar
    for i, total in enumerate(config_data['Disk_MB'] + config_data['Memory_MB']):
        ax.text(i, total * 1.02, f'{total:.1f} MB', ha='center', va='bottom', fontweight='bold')

    # Formatting
    ax.set_xticks(x_positions)
    ax.set_xticklabels([f'{int(val/1e6)}M' for val in x_labels])
    ax.set_ylabel('Storage Size (MB)')
    ax.set_xlabel('Database Size (Number of Records)')
    ax.legend()
    
    plt.tight_layout()
    Path(save_path).parent.mkdir(exist_ok=True)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    # plt.show()
    plt.close()
    print(f"    - Breakdown plot saved to: {save_path}")


def main():
    """Main function to run the complete analysis and plotting script."""
    file_paths = glob.glob("data/exp_1_bloom_metrics*.csv")
    Path("plots/storage").mkdir(parents=True, exist_ok=True)
    data = load_and_prepare_data(file_paths)

    # --- Generate All Plots ---

    create_comparison_plot(data, "plots/storage/comparison_all_configs.png")
    
    if not data.empty:
        print("\n--- Generating Individual Breakdown Plots ---")
        for config_name in data['Config'].unique():
            config_data = data[data['Config'] == config_name]
            safe_filename = f"breakdown_{config_name.replace(' ', '_')}.png"
            create_breakdown_plot(config_data, f"plots/storage/{safe_filename}")

    # --- Print Final Summary ---
    if not data.empty:
        print("\n--- Data Summary ---")
        summary = data.sort_values(['Ratio', 'numRecords'])
        for _, row in summary.iterrows():
           print(f"  {row['Config']:<10} | {row['numRecords']/1e6:>4.0f}M Records | Overhead: {row['Storage_Percentage']:.2f}%")

if __name__ == "__main__":
    main()
