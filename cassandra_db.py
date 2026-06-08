"""
Parte Cassandra del lab NYC Yellow Taxi (approccio query-first).

Modella UNA tabella per ogni query che ci serve, perche' Cassandra non
supporta GROUP BY / JOIN cross-partizione: i riepiloghi (tabelle 2 e 3)
vengono quindi PRE-AGGREGATI in scrittura con pandas groupby.

Input atteso: il DataFrame gia' pulito da pulizia_dati.clean_taxi_data()
(NON viene rifatta nessuna pulizia qui). Le colonne usate sono quelle
reali del dataset pulito:
    tpep_pickup_datetime, tpep_dropoff_datetime,
    PULocationID, DOLocationID,
    trip_distance, fare_amount, tip_amount, passenger_count
"""

import sys
import time
import datetime

import pandas as pd

# --- Bootstrap del reactor (event loop) del driver ------------------------
# cassandra-driver auto-rileva solo gevent/eventlet/libev/asyncore. Su
# Python 3.12+ il modulo asyncore non esiste piu' e, se la C-extension
# libev non e' compilata, l'import del driver fallisce. In quel caso
# ripieghiamo su gevent (monkey-patch) e ritentiamo l'import. Negli
# ambienti dove il driver importa normalmente, questo ramo non scatta.
try:
    from cassandra.cluster import Cluster
except Exception:
    import gevent.monkey
    gevent.monkey.patch_all()
    # ripuliamo i moduli cassandra caricati a meta' prima di ritentare
    for _m in [m for m in sys.modules if m.startswith("cassandra")]:
        del sys.modules[_m]
    from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

KEYSPACE = "taxi_stats"


# ---------------------------------------------------------------------------
# CONNESSIONE
# ---------------------------------------------------------------------------
def connect_to_cassandra(hosts=("127.0.0.1",), retries=10, delay=6):
    """
    Si connette a Cassandra con retry: il container e' lento ad avviarsi,
    quindi riproviamo piu' volte prima di arrenderci.
    Ritorna (cluster, session).
    """
    cluster = Cluster(list(hosts))
    last_err = None
    for tentativo in range(1, retries + 1):
        try:
            session = cluster.connect()
            print(f"Connesso a Cassandra al tentativo {tentativo}.")
            return cluster, session
        except Exception as err:  # tipicamente NoHostAvailable finche' parte
            last_err = err
            print(f"Cassandra non pronta (tentativo {tentativo}/{retries}), "
                  f"riprovo tra {delay}s...")
            time.sleep(delay)
    # Esauriti i tentativi: liberiamo le risorse e propaghiamo l'errore
    cluster.shutdown()
    raise RuntimeError(f"Impossibile connettersi a Cassandra: {last_err}")


# ---------------------------------------------------------------------------
# SCHEMA (una tabella per query)
# ---------------------------------------------------------------------------
def create_schema(session, reset=False):
    """
    Crea il keyspace e le tre tabelle modellate sulle query.

    reset=True: elimina prima il keyspace esistente. Serve perche' le CREATE
    usano IF NOT EXISTS: se una tabella esiste gia' (es. con la vecchia
    PRIMARY KEY), la nuova definizione NON verrebbe applicata. Con reset
    partiamo da zero e la chiave aggiornata viene davvero creata.
    ATTENZIONE: reset cancella i dati esistenti.
    """
    if reset:
        session.execute("DROP KEYSPACE IF EXISTS taxi_stats")
        print("Keyspace 'taxi_stats' eliminato (reset schema).")

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS taxi_stats
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace(KEYSPACE)

    # QUERY 1: corse di una zona in un giorno, dalla piu' recente.
    # Partition key composta (pickup_location_id, pickup_date): il giorno fa
    # da "bucket" cosi' la partizione di una zona non cresce all'infinito.
    # Clustering su pickup_datetime DESC -> gia' ordinate dalla piu' recente.
    # dropoff_location_id come ultima clustering column rende uniche due corse
    # della stessa zona partite nello stesso secondo (altrimenti collidono
    # sulla PK e una sovrascrive l'altra, perdendo righe).
    session.execute("""
        CREATE TABLE IF NOT EXISTS trips_by_zone_day (
            pickup_location_id   int,
            pickup_date          date,
            pickup_datetime      timestamp,
            dropoff_location_id  int,
            trip_distance        float,
            fare_amount          float,
            tip_amount           float,
            passenger_count      int,
            trip_duration_min    float,
            PRIMARY KEY ((pickup_location_id, pickup_date), pickup_datetime, dropoff_location_id)
        ) WITH CLUSTERING ORDER BY (pickup_datetime DESC, dropoff_location_id ASC)
    """)

    # QUERY 2: riepilogo per zona (pre-aggregato in scrittura).
    # Una riga per zona -> partizione pickup_location_id.
    session.execute("""
        CREATE TABLE IF NOT EXISTS zone_summary (
            pickup_location_id  int,
            trips_count         bigint,
            avg_fare            float,
            avg_tip             float,
            avg_distance        float,
            avg_duration_min    float,
            PRIMARY KEY (pickup_location_id)
        )
    """)

    # QUERY 3: volume corse per ora del giorno (pre-aggregato in scrittura).
    # Partizione pickup_date, clustering su pickup_hour -> ore ordinate 0..23.
    session.execute("""
        CREATE TABLE IF NOT EXISTS hourly_summary (
            pickup_date   date,
            pickup_hour   int,
            trips_count   bigint,
            PRIMARY KEY (pickup_date, pickup_hour)
        ) WITH CLUSTERING ORDER BY (pickup_hour ASC)
    """)

    print("Keyspace e tabelle pronti!")


# ---------------------------------------------------------------------------
# PREPARAZIONE COLONNE DERIVATE
# ---------------------------------------------------------------------------
def _aggiungi_colonne_derivate(df):
    """
    Aggiunge le colonne derivate che servono al modello Cassandra,
    senza toccare i dati gia' puliti. Ritorna una copia del DataFrame.
        - pickup_date       (date)  bucket giornaliero
        - pickup_hour       (int)   ora del giorno 0..23
        - trip_duration_min (float) durata corsa in minuti
    """
    df = df.copy()
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["trip_duration_min"] = (
        (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"])
        .dt.total_seconds() / 60.0
    )
    return df


# ---------------------------------------------------------------------------
# CARICAMENTO DATI
# ---------------------------------------------------------------------------
def load_data(session, df, concurrency=16):
    """
    Carica il DataFrame pulito nelle tre tabelle.
    Usa prepared statement + execute_concurrent_with_args per andare veloce.
    """
    session.set_keyspace(KEYSPACE)
    # Timeout client piu' generoso: un nodo singolo e' lento sotto carico,
    # 60s evita che le richieste vadano in OperationTimedOut.
    session.default_timeout = 60.0
    df = _aggiungi_colonne_derivate(df)

    # ----- Tabella 1: trips_by_zone_day (riga per corsa) -----------------
    ps_trips = session.prepare("""
        INSERT INTO trips_by_zone_day (
            pickup_location_id, pickup_date, pickup_datetime,
            dropoff_location_id, trip_distance, fare_amount,
            tip_amount, passenger_count, trip_duration_min
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    # Costruiamo la lista di tuple di parametri convertendo i tipi
    # (Cassandra vuole int/float nativi, non numpy / pandas Timestamp).
    params_trips = [
        (
            int(r.PULocationID),
            r.pickup_date,                       # datetime.date
            r.tpep_pickup_datetime.to_pydatetime(),
            int(r.DOLocationID),
            float(r.trip_distance),
            float(r.fare_amount),
            float(r.tip_amount),
            int(r.passenger_count),
            float(r.trip_duration_min),
        )
        for r in df.itertuples(index=False)
    ]
    print(f"Inserimento {len(params_trips)} corse in trips_by_zone_day...")
    execute_concurrent_with_args(session, ps_trips, params_trips,
                                 concurrency=concurrency)

    # ----- Tabella 2: zone_summary (PRE-AGGREGATA con pandas) ------------
    zone = (
        df.groupby("PULocationID")
          .agg(
              trips_count=("fare_amount", "size"),
              avg_fare=("fare_amount", "mean"),
              avg_tip=("tip_amount", "mean"),
              avg_distance=("trip_distance", "mean"),
              avg_duration_min=("trip_duration_min", "mean"),
          )
          .reset_index()
    )
    ps_zone = session.prepare("""
        INSERT INTO zone_summary (
            pickup_location_id, trips_count,
            avg_fare, avg_tip, avg_distance, avg_duration_min
        ) VALUES (?, ?, ?, ?, ?, ?)
    """)
    params_zone = [
        (
            int(r.PULocationID),
            int(r.trips_count),
            float(r.avg_fare),
            float(r.avg_tip),
            float(r.avg_distance),
            float(r.avg_duration_min),
        )
        for r in zone.itertuples(index=False)
    ]
    print(f"Inserimento {len(params_zone)} righe in zone_summary...")
    execute_concurrent_with_args(session, ps_zone, params_zone,
                                 concurrency=concurrency)

    # ----- Tabella 3: hourly_summary (PRE-AGGREGATA con pandas) ----------
    hourly = (
        df.groupby(["pickup_date", "pickup_hour"])
          .size()
          .reset_index(name="trips_count")
    )
    ps_hourly = session.prepare("""
        INSERT INTO hourly_summary (pickup_date, pickup_hour, trips_count)
        VALUES (?, ?, ?)
    """)
    params_hourly = [
        (
            r.pickup_date,             # datetime.date
            int(r.pickup_hour),
            int(r.trips_count),
        )
        for r in hourly.itertuples(index=False)
    ]
    print(f"Inserimento {len(params_hourly)} righe in hourly_summary...")
    execute_concurrent_with_args(session, ps_hourly, params_hourly,
                                 concurrency=concurrency)

    print("Caricamento completato!")


# ---------------------------------------------------------------------------
# UTILITY
# ---------------------------------------------------------------------------
def _to_date(giorno):
    """Accetta una stringa 'YYYY-MM-DD' o un datetime.date e ritorna un date."""
    if isinstance(giorno, str):
        return datetime.date.fromisoformat(giorno)
    if isinstance(giorno, datetime.datetime):
        return giorno.date()
    return giorno


# ---------------------------------------------------------------------------
# QUERY 1 -> trips_by_zone_day
# ---------------------------------------------------------------------------
def query_trips_by_zone_day(session, pickup_location_id, giorno, limit=20):
    """
    Corse di una zona in un dato giorno, dalla piu' recente.
    Filtra sulla PARTITION KEY COMPLETA (location_id + data): niente ALLOW FILTERING.
    """
    session.set_keyspace(KEYSPACE)
    ps = session.prepare("""
        SELECT pickup_datetime, dropoff_location_id, trip_distance,
               fare_amount, tip_amount, passenger_count, trip_duration_min
        FROM trips_by_zone_day
        WHERE pickup_location_id = ? AND pickup_date = ?
        LIMIT ?
    """)
    return list(session.execute(
        ps, (int(pickup_location_id), _to_date(giorno), int(limit))
    ))


# ---------------------------------------------------------------------------
# QUERY 2 -> zone_summary
# ---------------------------------------------------------------------------
def query_zone_summary(session, pickup_location_id):
    """Riepilogo aggregato di una zona. Filtra sulla partition key (location_id)."""
    session.set_keyspace(KEYSPACE)
    ps = session.prepare("""
        SELECT pickup_location_id, trips_count, avg_fare, avg_tip,
               avg_distance, avg_duration_min
        FROM zone_summary
        WHERE pickup_location_id = ?
    """)
    return session.execute(ps, (int(pickup_location_id),)).one()


# ---------------------------------------------------------------------------
# QUERY 3 -> hourly_summary
# ---------------------------------------------------------------------------
def query_hourly_summary(session, giorno):
    """
    Volume corse per ora in un dato giorno (ore 0..23 ordinate).
    Filtra sulla partition key (pickup_date): niente ALLOW FILTERING.
    """
    session.set_keyspace(KEYSPACE)
    ps = session.prepare("""
        SELECT pickup_hour, trips_count
        FROM hourly_summary
        WHERE pickup_date = ?
    """)
    return list(session.execute(ps, (_to_date(giorno),)))


# ---------------------------------------------------------------------------
# ESEMPIO D'USO
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Carichiamo il DataFrame GIA' pulito (non rifacciamo la pulizia)
    CLEAN_FILE = "data/yellow_tripdata_2026-01_clean.parquet"
    print(f"Leggo il dataset pulito: {CLEAN_FILE}")
    df = pd.read_parquet(CLEAN_FILE)
    print(f"Righe totali nel dataset pulito: {len(df)}")

    # Su un nodo Cassandra singolo l'intero dataset manda in OperationTimedOut:
    # per l'esempio campioniamo 20.000 righe (seed fisso per riproducibilita').
    df = df.sample(n=20000, random_state=42)
    print(f"Campionate {len(df)} righe per il caricamento di esempio.")

    cluster, session = connect_to_cassandra()
    try:
        # reset=True: ricrea lo schema da zero cosi' la nuova PRIMARY KEY
        # di trips_by_zone_day viene applicata anche se la tabella esisteva.
        create_schema(session, reset=True)
        load_data(session, df)

        # Scegliamo una zona e un giorno realmente presenti nei dati
        zona = int(df["PULocationID"].mode().iloc[0])   # zona piu' frequente
        giorno = df["tpep_pickup_datetime"].dt.date.min()  # primo giorno
        print(f"\n=== Esempio: zona={zona}, giorno={giorno} ===")

        # --- Query 1 ---
        print("\n[1] Ultime corse della zona nel giorno (dalla piu' recente):")
        for riga in query_trips_by_zone_day(session, zona, giorno, limit=5):
            print(f"  {riga.pickup_datetime} | dest={riga.dropoff_location_id} "
                  f"| dist={riga.trip_distance:.2f} | fare={riga.fare_amount:.2f} "
                  f"| tip={riga.tip_amount:.2f} | durata={riga.trip_duration_min:.1f}min")

        # --- Query 2 ---
        print("\n[2] Riepilogo della zona:")
        s = query_zone_summary(session, zona)
        if s:
            print(f"  corse={s.trips_count} | fare medio={s.avg_fare:.2f} "
                  f"| tip media={s.avg_tip:.2f} | distanza media={s.avg_distance:.2f} "
                  f"| durata media={s.avg_duration_min:.1f}min")

        # --- Query 3 ---
        print("\n[3] Volume corse per ora nel giorno:")
        for riga in query_hourly_summary(session, giorno):
            print(f"  ore {riga.pickup_hour:02d} -> {riga.trips_count} corse")
    finally:
        cluster.shutdown()
