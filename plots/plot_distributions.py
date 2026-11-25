
import re
import html
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# --------------------------------------------------
# Configuration
# --------------------------------------------------

SCRIPT_DIR = Path(__file__).resolve().parent
CSV_FILE = SCRIPT_DIR.parent / "csv" / "expJ1.csv"
OUTPUT_DIR = SCRIPT_DIR / "distribution_plots"

OUTPUT_DIR.mkdir(exist_ok=True)

DISTRIBUTIONS = ["Mixed", "Normal", "Zipfian"]

# All execution times are in microseconds.
Y_LABEL = "Execution time (µs)"


# --------------------------------------------------
# Load and clean CSV
# --------------------------------------------------

df = pd.read_csv(CSV_FILE)

df.columns = [
    html.unescape(re.sub(r"\*\*", "", col)).strip()
    for col in df.columns
]

# Remove possible duplicated header rows embedded in the CSV.
df = df[df["dbSize"].astype(str) != "dbSize"].copy()

# Convert numeric columns.
numeric_columns = [
    "dbSize", "exponent", "zipfRatio", "cardinality",
    "realDataPercentage"
]

for col in df.columns:
    if col not in ["distribution"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df = df.dropna(subset=["dbSize", "realDataPercentage"])

# Average repeated measurements for the same configuration.
group_columns = [
    "dbSize", "distribution", "exponent",
    "zipfRatio", "cardinality", "realDataPercentage"
]

df = (
    df.groupby(group_columns, as_index=False)
      .mean(numeric_only=True)
      .sort_values("realDataPercentage")
)


# --------------------------------------------------
# Plot helper
# --------------------------------------------------

def plot_lines(data, columns, labels, title, filename,
               log_scale=True):

    fig, ax = plt.subplots(figsize=(10, 6))

    for column, label in zip(columns, labels):
        ax.plot(
            data["realDataPercentage"],
            data[column],
            marker="o",
            linewidth=2,
            markersize=6,
            label=label
        )

    ax.set_title(title, fontsize=14)
    ax.set_xlabel("Real data percentage (%)")
    ax.set_ylabel(Y_LABEL)

    if log_scale:
        ax.set_yscale("log")

    ax.grid(True, which="both", linestyle="--", alpha=0.5)
    ax.legend(fontsize=9)
    fig.tight_layout()

    fig.savefig(OUTPUT_DIR / filename, dpi=300)
    plt.close(fig)


# --------------------------------------------------
# Column definitions
# --------------------------------------------------

real_columns = [
    "avgRealMultiTime",
    "avgRealSingleTime",
    "avgRealHashTime",
    "avgRealHashSingleTime",
    "avgRealHashSingleHierarchyTime",
    "avgRealHashMultiHierarchyIndexTime",
]

real_labels = [
    "Multi",
    "Single",
    "Hash",
    "Hash Single",
    "Hash Single Hierarchy",
    "Hash Multi Hierarchy",
]

false_columns = [
    "avgFalseMultiTime",
    "avgFalseSingleTime",
    "avgFalseHashTime",
    "avgFalseHashSingleTime",
    "avgFalseHashSingleHierarchyTime",
    "avgFalseHashMultiHierarchyIndexTime",
]

false_labels = [
    "Multi",
    "Single",
    "Hash",
    "Hash Single",
    "Hash Single Hierarchy",
    "Hash Multi Hierarchy",
]

hierarchical_columns = [
    "avgHierarchicalMultiTime",
    "avgHierarchicalSingleTime",
    "avgHashIndexTime",
    "avgHashSingleIndexTime",
    "avgHashSingleHierarchyIndexTime",
    "avgHashMultiHierarchyIndexTime",
]

hierarchical_labels = [
    "Hierarchical Multi",
    "Hierarchical Single",
    "Hash Index",
    "Hash Single Index",
    "Hash Single Hierarchy",
    "Hash Multi Hierarchy",
]


# --------------------------------------------------
# Generate plots for each distribution
# --------------------------------------------------

for distribution in DISTRIBUTIONS:

    data = df[df["distribution"] == distribution].copy()

    if data.empty:
        print(f"Skipping {distribution}: no data")
        continue

    # Create a separate directory for each distribution.
    dist_dir = OUTPUT_DIR / distribution.lower()
    dist_dir.mkdir(exist_ok=True)

    print(f"Generating plots for {distribution}...")

    # REAL DATA
    plot_lines(
        data,
        real_columns,
        real_labels,
        f"{distribution} - Real Data Queries",
        f"{distribution.lower()}_real.png"
    )

    # FALSE DATA
    plot_lines(
        data,
        false_columns,
        false_labels,
        f"{distribution} - False Data Queries",
        f"{distribution.lower()}_false.png"
    )

    # HIERARCHICAL
    plot_lines(
        data,
        hierarchical_columns,
        hierarchical_labels,
        f"{distribution} - Hierarchical Queries",
        f"{distribution.lower()}_hierarchical.png"
    )

    # Multi vs HashMulti - REAL
    plot_lines(
        data,
        [
            "avgRealMultiTime",
            "avgRealHashMultiHierarchyIndexTime",
        ],
        [
            "Multi",
            "Hash Multi Hierarchy",
        ],
        f"{distribution} - Multi vs HashMulti (Real Data)",
        f"{distribution.lower()}_multi_vs_hashmulti_real.png"
    )

    # Multi vs HashMulti - FALSE
    plot_lines(
        data,
        [
            "avgFalseMultiTime",
            "avgFalseHashMultiHierarchyIndexTime",
        ],
        [
            "Multi",
            "Hash Multi Hierarchy",
        ],
        f"{distribution} - Multi vs HashMulti (False Data)",
        f"{distribution.lower()}_multi_vs_hashmulti_false.png"
    )

    # SPEEDUP
    speedup = data.copy()

    speedup["RealSpeedup"] = (
        speedup["avgRealMultiTime"]
        / speedup["avgRealHashMultiHierarchyIndexTime"]
    )

    speedup["FalseSpeedup"] = (
        speedup["avgFalseMultiTime"]
        / speedup["avgFalseHashMultiHierarchyIndexTime"]
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(
        speedup["realDataPercentage"],
        speedup["RealSpeedup"],
        marker="o",
        linewidth=2,
        label="Real Data"
    )

    ax.plot(
        speedup["realDataPercentage"],
        speedup["FalseSpeedup"],
        marker="s",
        linewidth=2,
        label="False Data"
    )

    ax.axhline(1.0, linestyle="--", linewidth=1)

    ax.set_title(f"{distribution} - HashMulti Speedup over Multi")
    ax.set_xlabel("Real data percentage (%)")
    ax.set_ylabel("Speedup (Multi / HashMulti)")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend()

    fig.tight_layout()
    fig.savefig(
        dist_dir / f"{distribution.lower()}_speedup.png",
        dpi=300
    )
    plt.close(fig)

    # Move the other generated plots to the distribution directory.
    for suffix in [
        "real", "false", "hierarchical",
        "multi_vs_hashmulti_real",
        "multi_vs_hashmulti_false",
    ]:
        source = OUTPUT_DIR / f"{distribution.lower()}_{suffix}.png"
        if source.exists():
            source.rename(dist_dir / source.name)


# --------------------------------------------------
# Combined speedup comparison
# --------------------------------------------------

fig, ax = plt.subplots(figsize=(10, 6))

for distribution in DISTRIBUTIONS:

    data = df[df["distribution"] == distribution].copy()

    if data.empty:
        continue

    data["Speedup"] = (
        data["avgRealMultiTime"]
        / data["avgRealHashMultiHierarchyIndexTime"]
    )

    ax.plot(
        data["realDataPercentage"],
        data["Speedup"],
        marker="o",
        linewidth=2,
        label=distribution
    )

ax.axhline(1.0, linestyle="--", linewidth=1)
ax.set_title("HashMulti Speedup over Multi - Distribution Comparison")
ax.set_xlabel("Real data percentage (%)")
ax.set_ylabel("Speedup (Multi / HashMulti)")
ax.grid(True, linestyle="--", alpha=0.5)
ax.legend()

fig.tight_layout()
fig.savefig(OUTPUT_DIR / "all_distributions_speedup.png", dpi=300)
plt.close(fig)


# --------------------------------------------------
# Export aggregated data and speedup table
# --------------------------------------------------

df["RealSpeedup"] = (
    df["avgRealMultiTime"]
    / df["avgRealHashMultiHierarchyIndexTime"]
)

df["FalseSpeedup"] = (
    df["avgFalseMultiTime"]
    / df["avgFalseHashMultiHierarchyIndexTime"]
)

df.to_csv(OUTPUT_DIR / "aggregated_results.csv", index=False)

print("\nSpeedup results:")
print(
    df[[
        "distribution",
        "realDataPercentage",
        "RealSpeedup",
        "FalseSpeedup"
    ]].to_string(index=False)
)

print(f"\nPlots saved in: {OUTPUT_DIR.resolve()}")
