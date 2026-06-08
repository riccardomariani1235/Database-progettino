import pandas as pd

def clean_taxi_data(input_path: str, output_path: str) -> pd.DataFrame:
    """Legge, pulisce e salva il dataset dei taxi."""
    print(f"Leggendo il file originale: {input_path}...")
    df = pd.read_parquet(input_path)
    print(f"Righe originali: {len(df)}")

    # 1. Valori Nulli
    colonne_critiche = [
        'tpep_pickup_datetime', 'tpep_dropoff_datetime', 
        'PULocationID', 'DOLocationID', 'fare_amount', 
        'passenger_count', 'trip_distance'
    ]
    df_clean = df.dropna(subset=colonne_critiche).copy()

    # 2. Viaggi nel tempo
    df_clean['tpep_pickup_datetime'] = pd.to_datetime(df_clean['tpep_pickup_datetime'])
    df_clean['tpep_dropoff_datetime'] = pd.to_datetime(df_clean['tpep_dropoff_datetime'])
    
    data_inizio = pd.Timestamp('2026-01-01')
    data_fine = pd.Timestamp('2026-01-31 23:59:59')

    df_clean = df_clean[
        (df_clean['tpep_pickup_datetime'] >= data_inizio) & 
        (df_clean['tpep_pickup_datetime'] <= data_fine) &
        (df_clean['tpep_dropoff_datetime'] >= df_clean['tpep_pickup_datetime']) 
    ]

    # 3. Costi e Distanze impossibili
    df_clean = df_clean[(df_clean['fare_amount'] > 0) & (df_clean['trip_distance'] > 0)]

    # 4. Passeggeri Fantasma
    df_clean = df_clean[df_clean['passenger_count'] > 0]

    # Reset indice
    df_clean = df_clean.reset_index(drop=True)

    print(f"Righe dopo la pulizia: {len(df_clean)}")
    
    # 5. SALVATAGGIO DEL FILE PULITO
    print(f"Salvataggio del file pulito in: {output_path}...")
    df_clean.to_parquet(output_path)
    print("Salvataggio completato!\n")

    return df_clean
