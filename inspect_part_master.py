import pandas as pd

try:
    df = pd.read_csv("database")
except:
    df = pd.read_excel("part_master.xlsx")

print("\n========================")
print("COLUMNS:")
print("========================")
print(list(df.columns))

print("\n========================")
print("ROW COUNT:")
print("========================")
print(len(df))

print("\n========================")
print("NON-NULL COUNTS:")
print("========================")
print(df.notna().sum().sort_values(ascending=False))

print("\n========================")
print("SOURCE SYSTEM DISTRIBUTION:")
print("========================")
if "source_system" in df.columns:
    print(df["source_system"].value_counts())

print("\n========================")
print("SAMPLE ROWS GROUPED BY source_system:")
print("========================")
if "source_system" in df.columns:
    for src in df["source_system"].unique():
        print(f"\n--- {src} ---")
        print(df[df["source_system"] == src].head(3).T)
