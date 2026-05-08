import pandas as pd
import numpy as np

def clean_uber_data(df):
    df = df.copy()

    # 1. Remove duplicates
    df = df.drop_duplicates()

    # 2. Create trip_duration if possible
    if 'tpep_pickup_datetime' in df.columns and 'tpep_dropoff_datetime' in df.columns:
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

        df['trip_duration'] = (
            df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']
        ).dt.total_seconds() / 60  # minutes

    # 3. Remove invalid trips (negative/zero duration, fare, distance)
    if 'trip_duration' in df.columns:
        df = df[df['trip_duration'] > 0]

    if 'fare_amount' in df.columns:
        df = df[df['fare_amount'] > 0]

    if 'trip_distance' in df.columns:
        df = df[df['trip_distance'] > 0]

    # 4. Handle missing values (context-aware FAST)
    for col in df.columns:

        if df[col].isnull().sum() == 0:
            continue

        if df[col].dtype == 'object':
            df[col] = df[col].fillna(
                df[col].mode()[0] if not df[col].mode().empty else "Unknown"
            )

        elif np.issubdtype(df[col].dtype, np.number):
            if df[col].isnull().mean() < 0.2:
                df[col] = df[col].fillna(df[col].median())
            else:
                df[col] = df[col].fillna(0)

        else:
            df[col] = df[col].ffill()

    # 5. Validate trip duration (CAP at 99th percentile)
    if 'trip_duration' in df.columns:
        upper = df['trip_duration'].quantile(0.99)
        df['trip_duration'] = df['trip_duration'].clip(upper=upper)

    # 6. Validate fare & distance (CAP not filter)
    for col in ['fare_amount', 'trip_distance']:

        if col in df.columns:
            upper = df[col].quantile(0.99)
            df[col] = df[col].clip(upper=upper)

    return df