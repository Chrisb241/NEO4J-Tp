import pandas as pd
from neo4j import GraphDatabase

URI = "bolt://52.203.239.119:7687"
AUTH = ("neo4j","rowers-appearances-stairs")

driver = GraphDatabase.driver(URI, auth=AUTH)

# lire dataset

df = pd.read_csv("tgvmax.csv", sep=";")

df = df[[
    "DATE",
    "TRAIN_NO",
    "Origine",
    "Origine IATA",
    "Destination",
    "Destination IATA",
    "Heure_depart",
    "Heure_arrivee"
]]

df = df.sort_values(["DATE","TRAIN_NO","Heure_depart"])

# gares uniques

gares = pd.concat([
    df[["Origine IATA","Origine"]].rename(columns={"Origine IATA":"iata","Origine":"nom"}),
    df[["Destination IATA","Destination"]].rename(columns={"Destination IATA":"iata","Destination":"nom"})
]).drop_duplicates()


# trajets regroupés

trajets = df.groupby(["DATE","TRAIN_NO"]).agg(
    origine_iata=("Origine IATA","first"),
    destination_iata=("Destination IATA","last"),
    heure_depart=("Heure_depart","first"),
    heure_arrivee=("Heure_arrivee","last")
).reset_index()

trajets.columns = [
    "date",
    "train_no",
    "origine_iata",
    "destination_iata",
    "heure_depart",
    "heure_arrivee"
]


print("Gares:", len(gares))
print("Trajets:", len(trajets))
# import Neo4j

with driver.session() as session:

    # vider base
    session.run("MATCH (n) DETACH DELETE n")

    # index pour accélérer
    session.run("""
    CREATE INDEX gare_iata IF NOT EXISTS
    FOR (g:Gare)
    ON (g.iata)
    """)

    # import gares
    session.run("""
    UNWIND $rows AS row
    MERGE (g:Gare {iata:row.iata})
    SET g.nom=row.nom
    """, rows=gares.to_dict("records"))


    # import trajets
    session.run("""
    UNWIND $rows AS row
    MATCH (o:Gare {iata:row.origine_iata})
    MATCH (d:Gare {iata:row.destination_iata})

    CREATE (o)-[:TRAJET {
        train_no:row.train_no,
        date:row.date,
        heure_depart:row.heure_depart,
        heure_arrivee:row.heure_arrivee
    }]->(d)
    """, rows=trajets.to_dict("records"))


print("Import terminé")

# recherche trajets

def rechercher_trajet(depart, arrivee):

    query = """
    MATCH (a:Gare {nom:$depart})-[t:TRAJET]->(b:Gare {nom:$arrivee})
    RETURN t.train_no, t.date, t.heure_depart, t.heure_arrivee
    ORDER BY t.date, t.heure_depart
    """

    with driver.session() as session:
        result = session.run(query, depart=depart, arrivee=arrivee)

        print("Trains disponibles :")

        for r in result:
            print(
                r["date"],
                r["train_no"],
                r["heure_depart"],
                "→",
                r["heure_arrivee"]
            )

driver.close()
