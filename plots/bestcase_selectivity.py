#!/usr/bin/env python3
"""
Clean Query Performance Validation Plots
2 focused plots for validation section
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import glob
import re

# Academic styling
plt.style.use('default')
plt.rcParams.update({
    'figure.figsize': (14, 6),
    'font.size': 12,
    'axes.titlesize': 14,
    'axes.labelsize': 13,
    'xtick.labelsize': 11,
    'ytick.labelsize': 11,
    'legend.fontsize': 11,
    'lines.linewidth': 2.5,
    'lines.markersize': 8,
    'axes.grid': True,
    'grid.alpha': 0.3,
    'axes.spines.top': False,
    'axes.spines.right': False,
    'figure.facecolor': 'white',
    'axes.facecolor': 'white',
    'font.family': 'serif'
})

# Colors
COLORS = {
    'multi': '#2E86AB',      #  blue
    'single': '#A23B72',     #  magenta
    'accent': '#F18F01',     #  orange
    'success': '#C73E1D'     #  red
}

def clean_numeric_value(value):
    """Clean numeric values from CSV files"""
    try:
        return float(value)
    except:
        return np.nan

def load_validation_data():
    """Load experimental data for validation plots"""
    data = []
    
    # Load comprehensive analysis files
    comp_files = glob.glob("data/exp_1_mixed_query_summary_*.csv")
    
    for file_path in comp_files:
        config = extract_config_from_filename(file_path)
        if config:
            try:
                df = pd.read_csv(file_path, sep=';')
                if len(df.columns) == 1:
                    df = pd.read_csv(file_path, sep=',')
                
                df = df[df['dbSize'] != 'dbSize']
                
                # Clean numeric columns
                numeric_columns = ['dbSize', 'realDataPercentage','avgMultiTime','avgSingleTime']
                
                for col in numeric_columns:
                    if col in df.columns:
                        df[col] = df[col].apply(clean_numeric_value)                
                if len(df) > 0:
                    for _, row in df.iterrows():
                            data.append({
                                'Config': config,
                                'Records': row['dbSize'],
                                'Records_Label': f"{row['dbSize']/1e6:.0f}M",
                                'Selectivity': row['realDataPercentage'],
                                'Multi_Time': row['avgMultiTime'],
                                'Single_Time': row['avgSingleTime'],
                            })
                        
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
    
    return pd.DataFrame(data)

def extract_config_from_filename(filename: str) -> str:
    """Extract configuration from filename"""
    filename = Path(filename).name
    
    bloom_match = re.search(r'(\d+)M', filename)
    bloom_size = bloom_match.group(1) or bloom_match.group(2)  
    ratio_match = re.search(r'R(\d+)', filename)
    ratio = ratio_match.group(1)
    
    return f"Bloom{bloom_size}M_Ratio{ratio}"

def create_clean_validation_plots():
    """Create clean validation plots with just 2 focused comparisons"""
    data = load_validation_data()
    
    if data.empty:
        print("No validation data available")
        return

    configs = sorted(data['Config'].unique())
    print("Available configs:", configs)
    for config in configs:
        config_data = data[data['Config'] == config]
        # Create 2 focused plots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
        
        # Plot 1: Performance comparison across database sizes (0% selectivity)
    
        best_case = config_data[config_data['Selectivity'] == 0].sort_values('Records')
        
        if len(best_case) > 0:
            x = np.arange(len(best_case))
            width = 0.35
            
            bars1 = ax1.bar(x - width/2, best_case['Multi_Time'], width, 
                           label='Multi-Column Hierarchical', color=COLORS['multi'], alpha=0.8)
            bars2 = ax1.bar(x + width/2, best_case['Single_Time'], width, 
                           label='Single-Column Hierarchical', color=COLORS['single'], alpha=0.8)
            
            ax1.set_xlabel('Database Size', fontweight='bold')
            ax1.set_ylabel('Query Latency (μs)', fontweight='bold')
            ax1.set_title('Performance Comparison\n(Best Case: 0% Selectivity)', fontweight='bold')
            ax1.set_xticks(x)
            ax1.set_xticklabels(best_case['Records_Label'])
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        
        # Plot 2: Selectivity impact (100M records)
        selectivity_data = config_data[config_data['Records'] == 100_000_000].sort_values('Selectivity')
        
        if len(selectivity_data) > 0:
            ax2.plot(selectivity_data['Selectivity'], selectivity_data['Multi_Time'], 
                    'o-', label='Multi-Column Hierarchical', color=COLORS['multi'], linewidth=3, markersize=8)
            ax2.plot(selectivity_data['Selectivity'], selectivity_data['Single_Time'], 
                    's-', label='Single-Column Hierarchical', color=COLORS['single'], linewidth=3, markersize=8)
            
            ax2.set_xlabel('Query Selectivity (%)', fontweight='bold')
            ax2.set_ylabel('Query Latency (μs)', fontweight='bold')
            ax2.set_title('Selectivity Impact\n(100M Records)', fontweight='bold')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save plots
        Path("plots").mkdir(exist_ok=True)
        plt.savefig(f"plots/validation_clean{config}.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        print("\nClean validation plots generated successfully!")
        print(f"saved as plots/validation_clean{config}.png")

def create_clean_validation_plots_performance_comparison():
    
    data = load_validation_data()
    
    if data.empty:
        print("No validation data available")
        return

    configs = sorted(data['Config'].unique())
    print("Available configs:", configs)
    for config in configs:
        config_data = data[data['Config'] == config]
        # Create 2 focused plots
        fig, ax1 = plt.subplots(figsize=(8, 6))
        
        # Plot: Performance comparison across database sizes (0% selectivity)
    
        best_case = config_data[config_data['Selectivity'] == 0].sort_values('Records')
        
        if len(best_case) > 0:
            x = np.arange(len(best_case))
            width = 0.35
            
            bars1 = ax1.bar(x - width/2, best_case['Multi_Time']/1000, width, 
                           label='Multi-Column', color=COLORS['multi'], alpha=0.8)
            bars2 = ax1.bar(x + width/2, best_case['Single_Time']/1000, width, 
                           label='Single-Column', color=COLORS['single'], alpha=0.8)
            
            ax1.set_xlabel('Database Size', fontweight='bold')
            ax1.set_ylabel('Query Latency (ms)', fontweight='bold')
            ax1.set_title('Performance Comparison\n(Best Case: 0% Selectivity)', fontweight='bold')
            ax1.set_xticks(x)
            ax1.set_xticklabels(best_case['Records_Label'])
            ax1.legend()
            ax1.grid(True, alpha=0.3)
        
         
        plt.tight_layout()
        
        # Save plots
        Path("plots").mkdir(exist_ok=True)
        plt.savefig(f"plots/validation_clean_perf{config}.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        print("\nClean validation plots generated successfully!")
        print(f"saved as plots/validation_clean_perf{config}.png")


def create_clean_validation_selectivity_plots():
    """Create clean validation plots with just 2 focused comparisons"""
    data = load_validation_data()
    
    if data.empty:
        print("No validation data available")
        return

    configs = sorted(data['Config'].unique())
    print("Available configs:", configs)
    for config in configs:
        config_data = data[data['Config'] == config]
        # Create 2 focused plots
        fig, ax2 = plt.subplots(figsize=(8, 6))
        
              
        # Plot 2: Selectivity impact (100M records)
        selectivity_data = config_data[config_data['Records'] == 100_000_000].sort_values('Selectivity')
        
        if len(selectivity_data) > 0:
            ax2.plot(selectivity_data['Selectivity'], selectivity_data['Multi_Time']/1000, 
                    'o-', label='Multi-Column', color=COLORS['multi'], linewidth=3, markersize=8)
            ax2.plot(selectivity_data['Selectivity'], selectivity_data['Single_Time']/1000, 
                    's-', label='Single-Column', color=COLORS['single'], linewidth=3, markersize=8)
            
            ax2.set_xlabel('Query Selectivity (%)', fontweight='bold')
            ax2.set_ylabel('Query Latency (ms)', fontweight='bold')
            ax2.set_title('Selectivity Impact\n(100M Records)', fontweight='bold')
            ax2.legend()
            ax2.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # Save plots
        Path("plots").mkdir(exist_ok=True)
        plt.savefig(f"plots/validation_clean_selectivity{config}.png", dpi=300, bbox_inches='tight')
        plt.close()
        
        print("\nClean validation plots generated successfully!")
        print(f"saved as plots/validation_clean_selectivity{config}.png")



if __name__ == "__main__":
   # create_clean_validation_plots() 
   create_clean_validation_plots_performance_comparison()
   create_clean_validation_selectivity_plots()