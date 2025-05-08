from superset_wfs_dialect import base

def main():
    conn = base.connect(base_url="https://geoportal.stadt-koeln.de/arcgis/services/basiskarten/adressen_stadtteil/MapServer/WFSServer")
    cursor = conn.cursor()

    try:
        cursor.execute("SELECT * FROM adressen_stadtteil:Raderthal LIMIT 5")
        data = cursor.fetchall()
        print(f"{len(data)} Features geladen")
        if data:
            print("Beispiel:", data[0])
        print("\nSpaltenbeschreibung:")
        for desc in cursor.description:
            print(desc)
    except Exception as e:
        print("Fehler:", e)

if __name__ == "__main__":
    main()
