#!/usr/bin/env python3
"""
Generate three PNG plots from MapReduce outputs:
  1. hist_avg.png           – histogram of average ratings
  2. top20_counts.png       – top-20 most-rated movies
  3. scatter_avg_vs_cnt.png – avg rating vs #ratings (log-scale)
Saved into ./output/
"""

import pathlib, pandas as pd, matplotlib.pyplot as plt

HERE   = pathlib.Path(__file__).resolve().parent
OUTDIR = HERE / "output"
OUTDIR.mkdir(exist_ok=True)

avg   = pd.read_csv(HERE / "avg_ratings.tsv",   sep="\t",
                    names=["movieId", "avg"])
cnt   = pd.read_csv(HERE / "rating_counts.tsv", sep="\t",
                    names=["movieId", "cnt"])

# 1) histogram --------------------------------------------------------
plt.figure()
avg["avg"].plot.hist(bins=20, edgecolor="black")
plt.title("Distribution of Average Movie Ratings")
plt.xlabel("Average rating"); plt.ylabel("Number of movies")
plt.tight_layout(); plt.savefig(OUTDIR / "hist_avg.png", dpi=150); plt.close()

# 2) top-20 counts ----------------------------------------------------
top = cnt.sort_values("cnt", ascending=False).head(20)
plt.figure()
top.plot.barh(x="movieId", y="cnt", legend=False)
plt.gca().invert_yaxis()
plt.title("Top-20 Movies by Rating Count")
plt.xlabel("Number of ratings"); plt.ylabel("Movie ID")
plt.tight_layout(); plt.savefig(OUTDIR / "top20_counts.png", dpi=150); plt.close()

# 3) scatter avg vs cnt ----------------------------------------------
df = pd.merge(avg, cnt, on="movieId")
plt.figure()
plt.scatter(df["cnt"], df["avg"], alpha=0.4, s=8)
plt.xscale("log")
plt.axhline(df["avg"].mean(), ls="--", lw=1, c="grey")
plt.title("Average Rating vs #Ratings (log-scale)")
plt.xlabel("Number of ratings (log10)"); plt.ylabel("Average rating")
plt.tight_layout(); plt.savefig(OUTDIR / "scatter_avg_vs_cnt.png", dpi=150); plt.close()

print("✓ Plots saved in", OUTDIR)
