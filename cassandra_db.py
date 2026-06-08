from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def connect_to_cassandra():
    # Se il tuo Cassandra è locale, l'indirizzo è '127.0.0.1'
    cluster = Cluster(['127.0.0.1']) 
    session = cluster.connect()
    return cluster, session

def setup_database(session):
    # 1. Creazione Keyspace (il contenitore delle tabelle)
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS taxi_stats 
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    session.set_keyspace('taxi_stats')
    
    # 2. Creazione Tabella (Modellata per una query specifica)
    # Esempio: "Voglio vedere le corse per ogni zona di partenza (PULocationID) ordinate per data"
    session.execute("""
        CREATE TABLE IF NOT EXISTS trips_by_pickup_zone (
            pu_location_id int,
            pickup_datetime timestamp,
            trip_distance float,
            fare_amount float,
            PRIMARY KEY (pu_location_id, pickup_datetime)
        ) WITH CLUSTERING ORDER BY (pickup_datetime DESC)
    """)
    print("Keyspace e tabelle pronti!")

def insert_data(session, df):
    # Qui inseriremo i dati dal DataFrame alla tabella
    # Nota: con 2.5 milioni di righe, l'inserimento riga per riga è lento.
    # Useremo i prepared statements per velocizzare.
    query = session.prepare("""
        INSERT INTO trips_by_pickup_zone (pu_location_id, pickup_datetime, trip_distance, fare_amount)
        VALUES (?, ?, ?, ?)
    """)
    
    print("Inizio inserimento dati in Cassandra (potrebbe volerci un po')...")
    for index, row in df.iterrows():
        session.execute(query, (
            int(row['PULocationID']), 
            row['tpep_pickup_datetime'], 
            float(row['trip_distance']), 
            float(row['fare_amount'])
        ))
    print("Inserimento completato!")