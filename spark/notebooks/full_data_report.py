import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

class FullDataReport:

    @staticmethod
    def full_data_report(data):

        # =========================
        # SMART INPUT HANDLING
        # =========================

        # Case 1: DataFrame
        if isinstance(data, pd.DataFrame):
            df = data

        # Case 2: file path
        elif isinstance(data, (str, Path)):
            print(f"Loading data from: {data}")
            file_path = Path(data)

            if not file_path.exists():
                print(f"File does not exist: {data}")
                return

            df = pd.read_parquet(file_path)

        else:
            raise TypeError("Input must be a DataFrame or a valid file path")

        # =========================
        # ORIGINAL LOGIC (UNCHANGED)
        # =========================

        print(f"\nDataset: Shape: {df.shape}")
        print(f"Rows: {df.shape[0]:,} | Columns: {df.shape[1]}")

        print("\nColumn Information:")
        for col in df.columns:

            dtype = df[col].dtype
            non_null = df[col].notna().sum()
            non_null_pct = (non_null / len(df)) * 100
            null_count = df[col].isna().sum()
            null_pct = (null_count / len(df)) * 100

            print(
                f"{col:<30} | "
                f"Type: {str(dtype):<20} | "
                f"Non-Null: {non_null:>8} ({non_null_pct:>5.1f}%) | "
                f"Nulls: {null_count:>8} ({null_pct:>5.1f}%)"
            )

        print("\nFirst 5 rows:")
        display(df.head())

        print("\nNumerical Columns Summary:")
        print("=" * 70)
        display(df.describe().round(2))

        # Missing Values Analysis
        missing_data = pd.DataFrame({
            'Column': df.columns,
            'Missing_Count': df.isnull().sum(),
            'Missing_Percentage': (df.isnull().sum() / len(df) * 100).round(2)
        })

        missing_data = missing_data[
            missing_data['Missing_Count'] > 0
        ].sort_values('Missing_Count', ascending=False)

        if len(missing_data) > 0:
            print("\nMissing Values:")
            print("=" * 70)
            print(missing_data.to_string(index=False))
        else:
            print("\nNo missing values found!")

        # Numeric & Categorical
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()

        print(f"\nData Types:")
        print(f"Numeric Columns ({len(numeric_cols)}): {numeric_cols[:5]}")
        print(f"Categorical Columns ({len(categorical_cols)}): {categorical_cols}")

        # Duplicates
        duplicate_count = df.duplicated().sum()

        print(f"\nDuplicate Rows:")
        print(f"Total duplicates: {duplicate_count}")
        print(f"Duplicate percentage: {(duplicate_count/len(df)*100):.2f}%")

        print("\n" + "="*70)
        print("EXPLORATION SUMMARY")
        print("="*70)

        print(f"Dataset Size: {df.shape[0]:,} rows × {df.shape[1]} columns")
        print(f"Memory Usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        print(f"Numeric Features: {len(numeric_cols)}")
        print(f"Categorical Features: {len(categorical_cols)}")
        print(f"Missing Values: {df.isnull().sum().sum()}")
        print(f"Duplicate Rows: {duplicate_count}")
        # data quality
        print(f"Data Quality: {'High' if duplicate_count == 0 and df.isnull().sum().sum() == 0 else 'Needs Cleaning'}")
        print("="*70)
        print("\nExploration Complete! Ready for cleaning and transformation.")

        return df