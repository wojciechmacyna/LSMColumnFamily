
import re
import html
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


SCRIPT_DIR = Path(__file__).resolve().parent

CSV_FILE = SCRIPT_DIR.parent / "csv" / "real_data_comparison.csv"
OUTPUT_DIR = SCRIPT_DIR

OUTPUT_DIR.mkdir(exist_ok=True)


# --------------------------------------------------
# 1. Load and clean CSV
# --------------------------------------------------

df = pd.read_csv(CSV_FILE)

# Remove HTML entities and formatting artifacts from column names.
df.columns = [
    html.unescape(re.sub(r"\*\*", "", col)).strip()
    for col in df.columns
]

# Convert all columns to numeric values.
for col in df.columns:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# Average repeated measurements for each percentage.
df = (
    df.groupby(["dbSize", "realDataPercentage"], as_index=False)
      .mean(numeric_only=True)
      .sort_values("realDataPercentage")
)

x = df["realDataPercentage"]


# --------------------------------------------------
# 2. Plot helper
# --------------------------------------------------

def plot_group(title, columns, labels, filename):
    fig, ax = plt.subplots(figsize=(10, 6))

    for column, label in zip(columns, labels):
        ax.plot(
            x,
            df[column],
            marker="o",
            linewidth=2,
            markersize=6,
            label=label
        )

    ax.set_title(title, fontsize=14)
    ax.set_xlabel("Real data percentage (%)")
    ax.set_ylabel("Execution time (µs)")

    ax.set_yscale("log")
    ax.grid(True, which="both", linestyle="--", alpha=0.5)
    ax.legend()
    fig.tight_layout()

    fig.savefig(
        OUTPUT_DIR / filename,
        dpi=300,
        bbox_inches="tight"
    )
    plt.show()
    plt.close(fig)


# --------------------------------------------------
# 3. REAL DATA queries
# --------------------------------------------------

plot_group(
    "Real Data - Query Performance",
    [
        "avgRealDataMultiTime",
        "avgRealDataSingleTime",
        "avgRealHashIndexTime",
        "avgRealHashSingleIndexTime",
        "avgRealHashSingleHierarchyIndexTime",
        "avgRealDataHierarchicalHashMultiTime",
    ],
    [
        "Multi",
        "Single",
        "Hash Index",
        "Hash Single Index",
        "Hash Single Hierarchy",
        "Hierarchical Hash Multi",
    ],
    "real_data_performance.png"
)


# --------------------------------------------------
# 4. FALSE DATA queries
# --------------------------------------------------

plot_group(
    "False Data - Query Performance",
    [
        "avgFalseDataMultiTime",
        "avgFalseDataSingleTime",
        "avgFalseHashIndexTime",
        "avgFalseHashSingleIndexTime",
        "avgFalseHashSingleHierarchyIndexTime",
        "avgFalseDataHierarchicalHashMultiTime",
    ],
    [
        "Multi",
        "Single",
        "Hash Index",
        "Hash Single Index",
        "Hash Single Hierarchy",
        "Hierarchical Hash Multi",
    ],
    "false_data_performance.png"
)


# --------------------------------------------------
# 5. General hierarchical queries
# --------------------------------------------------

plot_group(
    "Hierarchical Queries - Query Performance",
    [
        "avgHierarchicalMultiTime",
        "avgHierarchicalSingleTime",
        "avgHashIndexTime",
        "avgHashSingleIndexTime",
        "avgHashSingleHierarchyIndexTime",
        "avgHierarchicalHashMultiTime",
    ],
    [
        "Hierarchical Multi",
        "Hierarchical Single",
        "Hash Index",
        "Hash Single Index",
        "Hash Single Hierarchy",
        "Hierarchical Hash Multi",
    ],
    "hierarchical_performance.png"
)


# --------------------------------------------------
# 6. Direct comparison: Multi vs HashMulti
# --------------------------------------------------

plot_group(
    "Multi vs Hierarchical Hash Multi - Real Data",
    [
        "avgRealDataMultiTime",
        "avgRealDataHierarchicalHashMultiTime",
    ],
    [
        "Multi",
        "Hierarchical Hash Multi",
    ],
    "real_multi_vs_hash_multi.png"
)

plot_group(
    "Multi vs Hierarchical Hash Multi - False Data",
    [
        "avgFalseDataMultiTime",
        "avgFalseDataHierarchicalHashMultiTime",
    ],
    [
        "Multi",
        "Hierarchical Hash Multi",
    ],
    "false_multi_vs_hash_multi.png"
)


# --------------------------------------------------
# 7. Speedup calculation
# --------------------------------------------------

df["Real_HashMulti_Speedup"] = (
    df["avgRealDataMultiTime"]
    / df["avgRealDataHierarchicalHashMultiTime"]
)

df["False_HashMulti_Speedup"] = (
    df["avgFalseDataMultiTime"]
    / df["avgFalseDataHierarchicalHashMultiTime"]
)

fig, ax = plt.subplots(figsize=(10, 6))

ax.plot(
    x,
    df["Real_HashMulti_Speedup"],
    marker="o",
    linewidth=2,
    label="Real Data"
)

ax.plot(
    x,
    df["False_HashMulti_Speedup"],
    marker="s",
    linewidth=2,
    label="False Data"
)

ax.axhline(1.0, linestyle="--", linewidth=1)

ax.set_title("Hierarchical Hash Multi Speedup over Multi")
ax.set_xlabel("Real data percentage (%)")
ax.set_ylabel("Speedup (Multi time / HashMulti time)")
ax.grid(True, linestyle="--", alpha=0.5)
ax.legend()

fig.tight_layout()
fig.savefig(
    OUTPUT_DIR / "hash_multi_speedup.png",
    dpi=300,
    bbox_inches="tight"
)
plt.show()
plt.close(fig)


# --------------------------------------------------
# 8. Save aggregated data
# --------------------------------------------------

df.to_csv(
    OUTPUT_DIR / "aggregated_results.csv",
    index=False
)

print(f"Plots saved in: {OUTPUT_DIR.resolve()}")
print(df[[
    "realDataPercentage",
    "Real_HashMulti_Speedup",
    "False_HashMulti_Speedup"
]].to_string(index=False))
