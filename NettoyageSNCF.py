import pandas as pd
# Lecture du dataset
df = pd.read_csv("tgvmax.csv", sep=";")
# Colonnes utiles
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
# Tri des trajets
df = df.sort_values(["DATE", "TRAIN_NO", "Heure_depart"])
# Création des gares
gares = pd.concat([
    df[["Origine IATA","Origine"]].rename(
        columns={"Origine IATA":"iata","Origine":"nom"}
    ),
    df[["Destination IATA","Destination"]].rename(
        columns={"Destination IATA":"iata","Destination":"nom"}
    )
]).drop_duplicates()
gares.to_csv("gares_nodes.csv", index=False)
print("Nombre de gares :", len(gares))
# Création des segments
# 1 segment = un arrêt -> arrêt suivant
segments = df[[
    "TRAIN_NO",
    "DATE",
    "Origine IATA",
    "Destination IATA",
    "Heure_depart",
    "Heure_arrivee"
]].drop_duplicates()

segments.columns = [
    "train_no",
    "date",
    "origine",
    "destination",
    "heure_depart",
    "heure_arrivee"
]

segments.to_csv("segments_relations.csv", index=False)

print("Nombre de segments :", len(segments))


trajets = df.groupby(["TRAIN_NO","DATE"]).agg(
    origine=("Origine","first"),
    destination=("Destination","last"),
    depart=("Heure_depart","first"),
    arrivee=("Heure_arrivee","last")
).reset_index()

trajets.columns = [
    "train_no",
    "date",
    "origine",
    "destination",
    "heure_depart",
    "heure_arrivee"
]

trajets.to_csv("trajets_complets.csv", index=False)

print("Nombre de trajets :", len(trajets))


print("Fichiers générés :")
print("- gares_nodes.csv")
print("- segments_relations.csv")
print("- trajets_complets.csv")