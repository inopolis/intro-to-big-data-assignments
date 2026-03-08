# performance_chart.py
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ── Results ────────────────────────────────────────────────────────────────
results = {
    "PostgreSQL": {"Q1": 2.355,  "Q2": 13.486, "Q3": 4.869,   "Q4": 5.536},
    "Citus":      {"Q1": 37.139, "Q2": 47.192, "Q3": 179.245, "Q4": 105.648},
    "ScyllaDB":   {"Q1": 20.979, "Q2": 6.727,  "Q3": 27.136,  "Q4": 42.178},
    "MongoDB":    {"Q1": 347.164,"Q2": 79.208,  "Q3": 32.108,  "Q4": 8.722},
}

queries  = ["Q1", "Q2", "Q3", "Q4"]
dbs      = ["PostgreSQL", "Citus", "ScyllaDB", "MongoDB"]
colors   = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]

q_labels = {
    "Q1": "Q1: Top 10 Countries\nby Check-ins",
    "Q2": "Q2: POIs Visited\nby Friends",
    "Q3": "Q3: Attractive Venues\nby Country",
    "Q4": "Q4: Venues by\nCustom Category",
}

# ── Figure 1: Grouped bar chart (all queries) ──────────────────────────────
fig, ax = plt.subplots(figsize=(14, 7))

x      = np.arange(len(queries))
width  = 0.18
offset = np.linspace(-1.5, 1.5, len(dbs)) * width

for i, (db, color) in enumerate(zip(dbs, colors)):
    vals = [results[db][q] for q in queries]
    bars = ax.bar(x + offset[i], vals, width, label=db, color=color, alpha=0.88)
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f"{val:.1f}s", ha="center", va="bottom", fontsize=7.5, rotation=45)

ax.set_xticks(x)
ax.set_xticklabels([q_labels[q] for q in queries], fontsize=10)
ax.set_ylabel("Average Query Time (seconds)", fontsize=11)
ax.set_title("Database Performance Comparison — All Queries\n(average of 3 runs)", fontsize=13, fontweight="bold")
ax.legend(title="Database", fontsize=10)
ax.set_ylim(0, max(results["MongoDB"]["Q1"], results["Citus"]["Q3"]) * 1.15)
ax.yaxis.grid(True, linestyle="--", alpha=0.5)
ax.set_axisbelow(True)
plt.tight_layout()
plt.savefig("chart_all_queries.png", dpi=150)
plt.show()
print("Saved: chart_all_queries.png")

# ── Figure 2: Per-query subplots ───────────────────────────────────────────
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
axes = axes.flatten()

for idx, q in enumerate(queries):
    ax = axes[idx]
    vals = [results[db][q] for db in dbs]
    bars = ax.bar(dbs, vals, color=colors, alpha=0.88, width=0.5)
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(vals)*0.01,
                f"{val:.2f}s", ha="center", va="bottom", fontsize=9, fontweight="bold")
    ax.set_title(q_labels[q], fontsize=11, fontweight="bold")
    ax.set_ylabel("Avg Time (seconds)", fontsize=9)
    ax.set_ylim(0, max(vals) * 1.2)
    ax.yaxis.grid(True, linestyle="--", alpha=0.5)
    ax.set_axisbelow(True)

fig.suptitle("Per-Query Performance Breakdown\n(average of 3 runs, lower is better)",
             fontsize=13, fontweight="bold", y=1.01)
plt.tight_layout()
plt.savefig("chart_per_query.png", dpi=150, bbox_inches="tight")
plt.show()
print("Saved: chart_per_query.png")

# ── Figure 3: Log scale overview ──────────────────────────────────────────
fig, ax = plt.subplots(figsize=(14, 6))

for i, (db, color) in enumerate(zip(dbs, colors)):
    vals = [results[db][q] for q in queries]
    bars = ax.bar(x + offset[i], vals, width, label=db, color=color, alpha=0.88)
    for bar, val in zip(bars, vals):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.05,
                f"{val:.1f}", ha="center", va="bottom", fontsize=7, rotation=45)

ax.set_yscale("log")
ax.set_xticks(x)
ax.set_xticklabels([q_labels[q] for q in queries], fontsize=10)
ax.set_ylabel("Average Query Time (seconds, log scale)", fontsize=11)
ax.set_title("Database Performance Comparison — Log Scale\n(average of 3 runs, lower is better)",
             fontsize=13, fontweight="bold")
ax.legend(title="Database", fontsize=10)
ax.yaxis.grid(True, linestyle="--", alpha=0.5)
ax.set_axisbelow(True)
plt.tight_layout()
plt.savefig("chart_log_scale.png", dpi=150)
plt.show()
print("Saved: chart_log_scale.png")

# ── Print summary table ────────────────────────────────────────────────────
print("\n" + "="*70)
print(f"{'Database':<12} {'Q1':>10} {'Q2':>10} {'Q3':>10} {'Q4':>10}")
print("="*70)
for db in dbs:
    row = results[db]
    print(f"{db:<12} {row['Q1']:>9.3f}s {row['Q2']:>9.3f}s {row['Q3']:>9.3f}s {row['Q4']:>9.3f}s")
print("="*70)