# main.py
from pulizia_dati import clean_taxi_data
import os

def main():
    # Creiamo la cartella data se non esiste per evitare errori
    if not os.path.exists("data"): # AGGIUNTO
        os.makedirs("data")        # AGGIUNTO

    input_file = "data/yellow_tripdata_2026-01.parquet"
    clean_file = "data/yellow_tripdata_2026-01_clean.parquet"

    # Prima di pulire, verifichiamo che il file originale esista davvero lì dentro
    if not os.path.exists(input_file): # AGGIUNTO
        print(f"ERRORE: Non trovo il file {input_file}!")
        print("Assicurati di aver spostato il file .parquet dentro la cartella 'data'.")
        return # Esce dal programma senza crashare

    if not os.path.exists(clean_file):
        print("Il file pulito non esiste. Avvio la procedura di pulizia...")
        df = clean_taxi_data(input_file, clean_file)
    else:
        print("File pulito trovato! Salto la pulizia.")

if __name__ == "__main__":
    main()