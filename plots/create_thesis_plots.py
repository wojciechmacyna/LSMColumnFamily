#!/usr/bin/env python3
"""
Academic-Quality Plots for Thesis Chapter 4.1: Query Performance Analysis
Hierarchical Bloom Filter Experiments
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from pathlib import Path
import glob
import re
import seaborn as sns

# Academic paper styling
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams.update({
    'figure.figsize': (12, 8),
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
    'font.family': 'serif',
    'font.serif': ['Times New Roman', 'DejaVu Serif'],
    'mathtext.fontset': 'stix'
})

# Academic color palette
COLORS = {
    'multi': '#2E86AB',      #  blue
    'single': '#A23B72',     #  magenta
    'accent': '#F18F01',     #  orange
    'success': '#C73E1D',    #  red
    'neutral': '#6C757D',    #  gray
    'light_blue': '#87CEEB', # Light blue
    'light_red': '#FFB6C1'   # Light red
}

class ThesisPlotGenerator:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.comprehensive_data = {}
        self.bloom_metrics_data = {}
        self.results_dir = Path("plots")
        self.results_dir.mkdir(exist_ok=True)
        
    def _clean_numeric_value(self, value):
        """Clean numeric values from CSV files"""
        try:
            return float(value)
        except:
            return np.nan
    
    def load_data(self):
        """Load all experimental data"""
        print("Loading experimental data for thesis plots...")
        
        # Load comprehensive analysis files
        comp_files = glob.glob(str(self.data_dir / "exp_1_mixed_query_summary_*.csv"))
        print(f"Found {len(comp_files)} comprehensive analysis files")
        
        for file_path in comp_files:
            config = self._extract_config_from_filename(file_path)
            if config:
                try:
                    df = pd.read_csv(file_path, sep=';')
                    if len(df.columns) == 1:
                        df = pd.read_csv(file_path, sep=',')
                    
                    df = df[df['dbSize'] != 'dbSize']
                    
                    # Clean numeric columns
                    numeric_columns = ['dbSize', 'realDataPercentage', 'avgMultiTime','avgSingleTime']
                    
                    for col in numeric_columns:
                        if col in df.columns:
                            df[col] = df[col].apply(self._clean_numeric_value)
                    
                    if len(df) > 0:
                        self.comprehensive_data[config] = df
                        
                except Exception as e:
                    print(f"Error loading {file_path}: {e}")
        
    
    def _extract_config_from_filename(self, filename: str) -> str:
        """Extract configuration from filename"""
        filename = Path(filename).name
        
        bloom_match = re.search(r'(\d+)M', filename)
        bloom_size = bloom_match.group(1) or bloom_match.group(2)
        
        ratio_match = re.search(r'R(\d+)', filename)
        ratio = ratio_match.group(1)
        
        return f"Bloom{bloom_size}M_Ratio{ratio}"
    
    def plot_1_performance_overview(self):
        """Plot 1: Performance Overview - Latency Comparison"""
        print("Creating Plot 1: Performance Overview...")
        
        target_db_size = 100_000_000  # 100M records
        
        # Collect data for best case (0% selectivity) and worst case (100% selectivity)
        best_case_data = []
        worst_case_data = []
        
        for config, df in self.comprehensive_data.items():
            # Best case (0% selectivity)
            best_subset = df[(df['dbSize'] == target_db_size) & (df['realDataPercentage'] == 0)]
            if len(best_subset) > 0:
                row = best_subset.iloc[0]
                bloom_size = re.search(r'Bloom(\d+)M', config).group(1) if re.search(r'Bloom(\d+)M', config) else "1"
                ratio = re.search(r'Ratio(\d+)', config).group(1) if re.search(r'Ratio(\d+)', config) else "?"
                
                best_case_data.append({
                    'Configuration': f"{bloom_size}M-R{ratio}",
                    'Bloom_Size': int(bloom_size),
                    'Ratio': int(ratio) if ratio.isdigit() else 999,
                    'Multi_Time': row['avgMultiTime'],
                    'Single_Time': row['avgSingleTime']
                })
            
            # Worst case (100% selectivity)
            worst_subset = df[(df['dbSize'] == target_db_size) & (df['realDataPercentage'] == 100)]
            if len(worst_subset) > 0:
                row = worst_subset.iloc[0]
                bloom_size = re.search(r'Bloom(\d+)M', config).group(1) if re.search(r'Bloom(\d+)M', config) else "1"
                ratio = re.search(r'Ratio(\d+)', config).group(1) if re.search(r'Ratio(\d+)', config) else "?"
                
                worst_case_data.append({
                    'Configuration': f"{bloom_size}M-R{ratio}",
                    'Bloom_Size': int(bloom_size),
                    'Ratio': int(ratio) if ratio.isdigit() else 999,
                    'Multi_Time': row['avgMultiTime'],
                    'Single_Time': row['avgSingleTime']
                })
        
        best_df = pd.DataFrame(best_case_data).sort_values(['Bloom_Size', 'Ratio'])
        worst_df = pd.DataFrame(worst_case_data).sort_values(['Bloom_Size', 'Ratio'])
        
        # Create subplot
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))
        
        # Best case plot
        x = np.arange(len(best_df))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, best_df['Multi_Time'], width, 
                       label='Multi-Column Hierarchical', color=COLORS['multi'], alpha=0.8)
        bars2 = ax1.bar(x + width/2, best_df['Single_Time'], width, 
                       label='Single-Column Hierarchical', color=COLORS['single'], alpha=0.8)
        
        ax1.set_xlabel('Configuration (Bloom Size - Ratio)', fontweight='bold')
        ax1.set_ylabel('Query Latency (μs)', fontweight='bold')
        ax1.set_title('Best Case Performance\n(0% Selectivity - No Matches)', fontweight='bold', pad=20)
        ax1.set_xticks(x)
        ax1.set_xticklabels(best_df['Configuration'], rotation=45, ha='right')
        ax1.legend(frameon=True, fancybox=True, shadow=True)
        ax1.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{height:.0f}', ha='center', va='bottom', fontweight='bold', fontsize=10)
        
        # Worst case plot
        x2 = np.arange(len(worst_df))
        
        bars3 = ax2.bar(x2 - width/2, worst_df['Multi_Time'], width, 
                       label='Multi-Column Hierarchical', color=COLORS['multi'], alpha=0.8)
        bars4 = ax2.bar(x2 + width/2, worst_df['Single_Time'], width, 
                       label='Single-Column Hierarchical', color=COLORS['single'], alpha=0.8)
        
        ax2.set_xlabel('Configuration (Bloom Size - Ratio)', fontweight='bold')
        ax2.set_ylabel('Query Latency (μs)', fontweight='bold')
        ax2.set_title('Worst Case Performance\n(100% Selectivity - All Matches)', fontweight='bold', pad=20)
        ax2.set_xticks(x2)
        ax2.set_xticklabels(worst_df['Configuration'], rotation=45, ha='right')
        ax2.legend(frameon=True, fancybox=True, shadow=True)
        ax2.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar in bars3:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{height:.0f}', ha='center', va='bottom', fontweight='bold', fontsize=10)
        
        plt.tight_layout()
        plt.savefig(self.results_dir / "plot1_performance_overview.png", dpi=300, bbox_inches='tight')
        plt.close()



    def plot_1_performance_overview_0(self):
        """Plot 1: Performance Overview - Latency Comparison"""
        print("Creating Plot 1: Performance Overview...")
        
        target_db_size = 100_000_000  # 100M records
        
        # Collect data for best case (0% selectivity) and worst case (100% selectivity)
        best_case_data = []
        
        for config, df in self.comprehensive_data.items():
            # Best case (0% selectivity)
            best_subset = df[(df['dbSize'] == target_db_size) & (df['realDataPercentage'] == 0)]
            if len(best_subset) > 0:
                row = best_subset.iloc[0]
                bloom_size = re.search(r'Bloom(\d+)M', config).group(1) if re.search(r'Bloom(\d+)M', config) else "1"
                ratio = re.search(r'Ratio(\d+)', config).group(1) if re.search(r'Ratio(\d+)', config) else "?"
                
                best_case_data.append({
                    'Configuration': f"{bloom_size}M-R{ratio}",
                    'Bloom_Size': int(bloom_size),
                    'Ratio': int(ratio) if ratio.isdigit() else 999,
                    'Multi_Time': row['avgMultiTime'],
                    'Single_Time': row['avgSingleTime']
                })
            
            
        best_df = pd.DataFrame(best_case_data).sort_values(['Bloom_Size', 'Ratio'])
        
        
        # Create subplot
        fig, ax1 = plt.subplots(figsize=(8, 6))
        
        # Best case plot
        x = np.arange(len(best_df))
        width = 0.35
        
        bars1 = ax1.bar(x - width/2, best_df['Multi_Time']/1000, width, 
                       label='Multi-Column', color=COLORS['multi'], alpha=0.8)
        bars2 = ax1.bar(x + width/2, best_df['Single_Time']/1000, width, 
                       label='Single-Column', color=COLORS['single'], alpha=0.8)
        
        ax1.set_xlabel('Configuration (Bloom Size - Ratio)', fontweight='bold')
        ax1.set_ylabel('Query Latency (ms)', fontweight='bold')
        ax1.set_title('Best Case Performance\n(0% Selectivity - No Matches)', fontweight='bold', pad=20)
        ax1.set_xticks(x)
        ax1.set_xticklabels(best_df['Configuration'], rotation=45, ha='right')
        ax1.legend(frameon=True, fancybox=True, shadow=True)
        ax1.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar in bars1:
            height = bar.get_height()
            ax1.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{height:.0f}', ha='center', va='bottom', fontweight='bold', fontsize=10)
        
       
        plt.tight_layout()
        plt.savefig(self.results_dir / "plot1_performance_overview_0.png", dpi=300, bbox_inches='tight')
        plt.close()
  
    def plot_1_performance_overview_100(self):
        """Plot 1: Performance Overview - Latency Comparison"""
        print("Creating Plot 1: Performance Overview...")
        
        target_db_size = 100_000_000  # 100M records
        
        # Collect data for best case (0% selectivity) and worst case (100% selectivity)
        worst_case_data = []
        
        for config, df in self.comprehensive_data.items():
                 
            # Worst case (100% selectivity)
            worst_subset = df[(df['dbSize'] == target_db_size) & (df['realDataPercentage'] == 100)]
            if len(worst_subset) > 0:
                row = worst_subset.iloc[0]
                bloom_size = re.search(r'Bloom(\d+)M', config).group(1) if re.search(r'Bloom(\d+)M', config) else "1"
                ratio = re.search(r'Ratio(\d+)', config).group(1) if re.search(r'Ratio(\d+)', config) else "?"
                
                worst_case_data.append({
                    'Configuration': f"{bloom_size}M-R{ratio}",
                    'Bloom_Size': int(bloom_size),
                    'Ratio': int(ratio) if ratio.isdigit() else 999,
                    'Multi_Time': row['avgMultiTime'],
                    'Single_Time': row['avgSingleTime']
                })
        
       
        worst_df = pd.DataFrame(worst_case_data).sort_values(['Bloom_Size', 'Ratio'])
        
        # Create subplot
        fig, ax2 = plt.subplots(figsize=(8, 6))
        
         
        # Worst case plot
        x2 = np.arange(len(worst_df))
        width = 0.35
        
        bars3 = ax2.bar(x2 - width/2, worst_df['Multi_Time']/1000, width, 
                       label='Multi-Column', color=COLORS['multi'], alpha=0.8)
        bars4 = ax2.bar(x2 + width/2, worst_df['Single_Time']/1000, width, 
                       label='Single-Column', color=COLORS['single'], alpha=0.8)
        
        ax2.set_xlabel('Configuration (Bloom Size - Ratio)', fontweight='bold')
        ax2.set_ylabel('Query Latency (ms)', fontweight='bold')
        ax2.set_title('Worst Case Performance\n(100% Selectivity - All Matches)', fontweight='bold', pad=20)
        ax2.set_xticks(x2)
        ax2.set_xticklabels(worst_df['Configuration'], rotation=45, ha='right')
        ax2.legend(frameon=True, fancybox=True, shadow=True)
        ax2.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar in bars3:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{height:.0f}', ha='center', va='bottom', fontweight='bold', fontsize=10)
        
        plt.tight_layout()
        plt.savefig(self.results_dir / "plot1_performance_overview_100.png", dpi=300, bbox_inches='tight')
        plt.close()


    def plot_2_branching_factor(self, dbSize):
        """Plot 2: Branching factor  Overview - Latency Comparison"""
        print("Creating Plot 2: Performance Overview...")
        
        target_db_size = dbSize
        dbSizeEdit = dbSize//1000000  
        
        # Collect data for best case (0% selectivity) and worst case (100% selectivity)
        data = []
        
        for config, df in self.comprehensive_data.items():
                 
            # 50% selectivity
            subset = df[(df['dbSize'] == target_db_size) & (df['realDataPercentage'] == 50)]
            if len(subset) > 0:
                row = subset.iloc[0]
                bloom_size = re.search(r'Bloom(\d+)M', config).group(1) if re.search(r'Bloom(\d+)M', config) else "1"
                ratio = re.search(r'Ratio(\d+)', config).group(1) if re.search(r'Ratio(\d+)', config) else "?"
                
                data.append({
                    'Configuration': f"{bloom_size}M-R{ratio}",
                    'Bloom_Size': int(bloom_size),
                    'Ratio': int(ratio) if ratio.isdigit() else 999,
                    'Multi_Time': row['avgMultiTime'],
                    'Single_Time': row['avgSingleTime']
                })
        
       
        worst_df = pd.DataFrame(data).sort_values(['Bloom_Size', 'Ratio'])
        
        # Create subplot
        fig, ax2 = plt.subplots(figsize=(8, 6))
        
         
        # Worst case plot
        x2 = np.arange(len(worst_df))
        width = 0.35
        
        bars3 = ax2.bar(x2 - width/2, worst_df['Multi_Time'], width, 
                       label='Multi-Column Hierarchical', color=COLORS['multi'], alpha=0.8)
        bars4 = ax2.bar(x2 + width/2, worst_df['Single_Time'], width, 
                       label='Single-Column Hierarchical', color=COLORS['single'], alpha=0.8)
        
        ax2.set_xlabel('Configuration (Bloom Size - Ratio)', fontweight='bold')
        ax2.set_ylabel('Query Latency (μs)', fontweight='bold')
        ax2.set_title('Database size: ' + str(dbSizeEdit) + 'M Records', fontweight='bold', pad=20)
        ax2.set_xticks(x2)
        ax2.set_xticklabels(worst_df['Configuration'], rotation=45, ha='right')
        ax2.legend(frameon=True, fancybox=True, shadow=True)
        ax2.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar in bars3:
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                    f'{height:.0f}', ha='center', va='bottom', fontweight='bold', fontsize=10)
        
        plt.tight_layout()
        filename = "plot2_branching_factor_" + str(dbSize) + ".png"
        plt.savefig(self.results_dir / filename, dpi=300, bbox_inches='tight')
        plt.close()

def main():
    generator = ThesisPlotGenerator()
    
    # Load all experimental data
    generator.load_data()
    
    if not generator.comprehensive_data:
        print("No data loaded. Check data files.")
        return
    
    print("="*80)
    
    # Generate all plots
    generator.plot_1_performance_overview_0()
    generator.plot_1_performance_overview_100()

    #generator.plot_2_branching_factor(10000000)
    #generator.plot_2_branching_factor(50000000)
    #generator.plot_2_branching_factor(100000000)
    #generator.plot_2_branching_factor(500000000)
    
    print(f"\nPlots generated successfully!")
    print(f"Results saved in: {generator.results_dir}")

if __name__ == "__main__":
    main() 