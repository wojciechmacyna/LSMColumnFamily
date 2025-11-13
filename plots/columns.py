import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
from pathlib import Path
from typing import List, Dict

# --- Global Configuration ---
sns.set_theme(style="whitegrid")
PLOT_STYLE = {
    "axes.titlesize": 16, "axes.labelsize": 14, "xtick.labelsize": 12,
    "ytick.labelsize": 12, "legend.fontsize": 11, "font.family": "serif",
}
plt.rcParams.update(PLOT_STYLE)
BAR_COLORS = ['#2E86AB', '#A23B72']
LINE_COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b']
LINE_MARKERS = ['o', 's', '^', 'D', 'P', 'X']

def load_and_prepare_data(file_path: str) -> pd.DataFrame:
    """Loads and preprocesses the scalability data from the provided CSV."""
    print(f"Loading data from {file_path}...")
    try:
        df = pd.read_csv(file_path)
        df['Multi_Time_ms'] = df['avgHierarchicalMultiTime'] / 1000
        df['Single_Time_ms'] = df['avgHierarchicalSingleTime'] / 1000
        df['Speedup'] = df['Single_Time_ms'] / df['Multi_Time_ms']
        print("Data loaded and preprocessed successfully.")
        return df
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return pd.DataFrame()
    except KeyError as e:
        print(f"Error: A required column was not found in the CSV: {e}")
        return pd.DataFrame()

def _create_bar_subplot(ax: plt.Axes, df_subset: pd.DataFrame, percentage: int, show_legend: bool, show_ylabel: bool):
    """Helper function to draw a single bar chart on a given subplot (ax)."""
    x = np.arange(len(df_subset['numColumns']))
    width = 0.4
    
    bars1 = ax.bar(x - width/2, df_subset['Multi_Time_ms'], width, label='Multi-Column', color=BAR_COLORS[0])
    bars2 = ax.bar(x + width/2, df_subset['Single_Time_ms'], width, label='Single-Column', color=BAR_COLORS[1])

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2.0, bar.get_height(), f'{bar.get_height():.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width()/2.0, bar.get_height(), f'{bar.get_height():.1f}', ha='center', va='bottom', fontsize=9, fontweight='bold')

    ax.set_title(f'{percentage}% Positive Queries', fontsize=14, fontweight='bold')
    ax.set_xlabel('Number of Columns', fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(df_subset['numColumns'])
    
    if show_legend:
        ax.legend(loc='upper left')
    if show_ylabel:
        ax.set_ylabel('Query Time (ms)', fontweight='bold')
    
    # Adjust y-axis limit to make space for labels, returning the new limit
    new_ylim_top = ax.get_ylim()[1] * 1.15
    ax.set_ylim(top=new_ylim_top)
    return new_ylim_top

def create_bar_plots(df: pd.DataFrame, percentages: List[int]):
    """Creates both individual and a combined bar plot."""
    print("\nGenerating bar plots...")
    # --- Create and Save Individual Bar Plots ---
    for p in percentages:
        plt.figure(figsize=(8, 6))
        ax = plt.gca()
        subset = df[df['realDataPercentage'] == p].sort_values('numColumns')
        _create_bar_subplot(ax, subset, p, show_legend=True, show_ylabel=True)
        save_path = f"plots/columns/scalability_bar_{p}pct.png"
        plt.tight_layout()
        Path(save_path).parent.mkdir(exist_ok=True, parents=True)
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  > Individual plot saved to {save_path}")

    # --- Create and Save Combined Bar Plot ---
    fig, axes = plt.subplots(1, 3, figsize=(22, 7), sharey=True)
    fig.suptitle('Multi-Column Query Performance by Selectivity', fontsize=20, fontweight='bold')
    
    max_ylim = 0
    for i, p in enumerate(percentages):
        subset = df[df['realDataPercentage'] == p].sort_values('numColumns')
        # Pass booleans to control legend and y-label display based on position
        ylim_top = _create_bar_subplot(axes[i], subset, p, show_legend=(i==0), show_ylabel=(i==0))
        if ylim_top > max_ylim:
            max_ylim = ylim_top
    
    # Apply the max y-limit across all subplots for a consistent scale
    for ax in axes:
        ax.set_ylim(top=max_ylim)
        
    combined_save_path = "plots/columns/scalability_bars_combined.png"
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    Path(combined_save_path).parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(combined_save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  > Combined plot saved to {combined_save_path}")

def create_line_plot(df: pd.DataFrame, percentages: List[int], save_path: str):
    """Creates line plots showing performance and speedup trends."""
    print("\nGenerating performance trend line plot...")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 6))
    fig.suptitle('Column Scalability Performance and Speedup Trends', fontsize=20, fontweight='bold')
    
    for i, p in enumerate(percentages):
        if p == 0:  # Skip when percentage = 0
            continue
        subset = df[df['realDataPercentage'] == p].sort_values('numColumns')
        ax1.plot(subset['numColumns'], subset['Multi_Time_ms'], marker=LINE_MARKERS[i], color=LINE_COLORS[i], label=f'{p}% Positive')
        ax2.plot(subset['numColumns'], subset['Speedup'], marker=LINE_MARKERS[i], color=LINE_COLORS[i], label=f'{p}% Positive')

    ax1.set_title('Multi-Column Performance Scaling', fontweight='bold')
    ax1.set_xlabel('Number of Columns', fontweight='bold')
    ax1.set_ylabel('Query Time (ms)', fontweight='bold')
    ax2.axhline(y=1, color='red', linestyle='--', label='No Speedup')
    ax2.set_title('Speedup Ratio (Single / Multi)', fontweight='bold')
    ax2.set_xlabel('Number of Columns', fontweight='bold')
    ax2.set_ylabel('Speedup Ratio', fontweight='bold')
    
    for ax in [ax1, ax2]:
        ax.legend()
        ax.set_xticks(df['numColumns'].unique())

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    Path(save_path).parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  > Line plot saved to {save_path}")


def create_line_plot_performance(df: pd.DataFrame, percentages: List[int], save_path: str):
    """Creates line plots showing performance  trends."""
    print("\nGenerating performance trend line plot...")

    fig, ax1 = plt.subplots(figsize=(10, 8))
    fig.suptitle('Column Scalability Performance Trends', fontsize=20, fontweight='bold')
    
    for i, p in enumerate(percentages):
        if p == 0:  # Skip when percentage = 0
            continue
        subset = df[df['realDataPercentage'] == p].sort_values('numColumns')
        ax1.plot(subset['numColumns'], subset['Multi_Time_ms'], marker=LINE_MARKERS[i], color=LINE_COLORS[i], label=f'{p}% Positive')
   

    ax1.set_title('Multi-Column Performance Scaling', fontweight='bold')
    ax1.set_xlabel('Number of Columns', fontweight='bold')
    ax1.set_ylabel('Query Time (ms)', fontweight='bold')
    
    for ax in [ax1]:
        ax.legend()
        ax.set_xticks(df['numColumns'].unique())

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    Path(save_path).parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  > Line plot saved to {save_path}")


def create_line_plot_speedup(df: pd.DataFrame, percentages: List[int], save_path: str):
    """Creates line plots showing performance and speedup trends."""
    print("\nGenerating performance trend line plot...")

    fig, ax2 = plt.subplots(figsize=(10, 8))
    fig.suptitle('Column Scalability Speedup Trends', fontsize=20, fontweight='bold')
    
    for i, p in enumerate(percentages):
        if p == 0:  # Skip when percentage = 0
            continue
        subset = df[df['realDataPercentage'] == p].sort_values('numColumns')
        ax2.plot(subset['numColumns'], subset['Speedup'], marker=LINE_MARKERS[i], color=LINE_COLORS[i], label=f'{p}% Positive')

    ax2.axhline(y=1, color='red', linestyle='--', label='No Speedup')
    ax2.set_title('Speedup Ratio (Single / Multi)', fontweight='bold')
    ax2.set_xlabel('Number of Columns', fontweight='bold')
    ax2.set_ylabel('Speedup Ratio', fontweight='bold')
    
    for ax in [ax2]:
        ax.legend()
        ax.set_xticks(df['numColumns'].unique())

    plt.tight_layout(rect=[0, 0, 1, 0.96])
    Path(save_path).parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  > Line plot saved to {save_path}")

def create_single_heatmap(data: pd.DataFrame, title: str, cmap: str, fmt: str, save_path: str):
    """Helper function to generate and save a single heatmap plot."""
    print(f"  > Generating heatmap for '{title}'...")
    plt.figure(figsize=(10, 8))
    ax = plt.gca()
    sns.heatmap(data, ax=ax, cmap=cmap, annot=True, fmt=fmt, linewidths=.5, annot_kws={"fontweight": "bold"})
    ax.set_title(title, fontsize=16, fontweight='bold')
    ax.set_xlabel('Number of Columns', fontweight='bold')
    ax.set_ylabel('Positive Query Pct (%)', fontweight='bold')
    
    plt.tight_layout()
    Path(save_path).parent.mkdir(exist_ok=True, parents=True)
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"    - Heatmap saved to {save_path}")

def create_all_heatmaps(df: pd.DataFrame):
    """Prepares data and creates separate heatmaps for each metric."""
    print("\nGenerating individual performance heatmaps...")
    
    # Configuration for each heatmap to be generated
    heatmap_configs = [
        {
            "metric": "Multi_Time_ms", "title": "Multi-Column Query Time (ms)", 
            "cmap": "YlOrRd", "fmt": ".1f", "filename": "plots/columns/heatmap_multi_time.png"
        },
        {
            "metric": "Single_Time_ms", "title": "Single-Column Query Time (ms)", 
            "cmap": "YlGnBu", "fmt": ".1f", "filename": "plots/columns/heatmap_single_time.png"
        },
        {
            "metric": "Speedup", "title": "Speedup Ratio (Single/Multi)", 
            "cmap": "RdYlGn", "fmt": ".2f", "filename": "plots/columns/heatmap_speedup_ratio.png"
        }
    ]
    
    for config in heatmap_configs:
        pivot_data = df.pivot_table(index='realDataPercentage', columns='numColumns', values=config['metric'])
        create_single_heatmap(pivot_data, config['title'], config['cmap'], config['fmt'], config['filename'])

def create_summary_table(df: pd.DataFrame):
    """Prints a formatted summary table of the results."""
    print("\n--- Summary Table: Multi-Column Performance (ms) and Speedup ---")
    summary_df = df[df['realDataPercentage'].isin([20, 60, 100])]
    summary_pivot = summary_df.pivot_table(
        index='numColumns', columns='realDataPercentage',
        values=['Multi_Time_ms', 'Single_Time_ms', 'Speedup']
    )
    summary_pivot.columns = [f'{val}_{pct}' for val, pct in summary_pivot.columns]
    
    print(f"{'Columns':<10}{'20% (Multi/Single)':<25}{'60% (Multi/Single)':<25}{'100% (Multi/Single)':<25}{'Avg Speedup':<15}")
    print("-" * 100)

    for index, row in summary_pivot.iterrows():
        c20 = f"{row.get('Multi_Time_ms_20', 0):.1f} / {row.get('Single_Time_ms_20', 0):.1f}"
        c60 = f"{row.get('Multi_Time_ms_60', 0):.1f} / {row.get('Single_Time_ms_60', 0):.1f}"
        c100 = f"{row.get('Multi_Time_ms_100', 0):.1f} / {row.get('Single_Time_ms_100', 0):.1f}"
        avg_speedup = row[[f'Speedup_{p}' for p in [20, 60, 100] if f'Speedup_{p}' in row]].mean()
        print(f"{index:<10}{c20:<25}{c60:<25}{c100:<25}{avg_speedup:.2f}x")
    print("-" * 100)

def main():
    """Main function to run the complete analysis and plotting script."""
    Path("plots/columns").mkdir(parents=True, exist_ok=True)
    data_file = "data/exp_8_timing_comparison.csv"
    df_timing = load_and_prepare_data(data_file)

    if df_timing is not None and not df_timing.empty:
        key_percentages = [0, 20, 40, 60, 80, 100]
        plot_df = df_timing.copy()
        create_bar_plots(plot_df, [20,60,100])
        create_line_plot(plot_df, key_percentages, "plots/columns/scalability_trends.png")
        create_line_plot_performance(plot_df, key_percentages, "plots/columns/scalability_performance_trends.png")
        create_line_plot_speedup(plot_df, key_percentages, "plots/columns/scalability_speedup_trends.png")
        create_all_heatmaps(df_timing)
        create_summary_table(df_timing)
        print("\nAll plots and summaries generated successfully.")

if __name__ == "__main__":
    main()
