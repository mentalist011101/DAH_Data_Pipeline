import os.path
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
ENRICHED_DATA_DIR = os.path.join(DATA_DIR, "enriched_data")
CLEAN_DATA_DIR = os.path.join(DATA_DIR, "clean_data")

def stock_journalier(date: datetime):
    input_products_path = os.path.join(f"{CLEAN_DATA_DIR}/products/{date.year}/{date.month}", f"{date.day}.csv")
    input_orders_path = os.path.join(f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
    output_path = os.path.join(f"{ENRICHED_DATA_DIR}/stock_journalier/{date.year}/{date.month}", f"{date.day}.csv")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(input_products_path):
        raise FileNotFoundError(f"Le fichier {input_products_path} n'existe pas.")
    if not os.path.exists(input_orders_path):
        raise FileNotFoundError(f"Le fichier {input_orders_path} n'existe pas.")

    products_data = pd.read_csv(input_products_path)
    orders_data = pd.read_csv(input_orders_path)

    # On calcule le total commandé par produit
    commande_total = orders_data.groupby('product_id')['quantity'].sum().reset_index()

    #petit renommage
    commande_total.rename(columns={'quantity': 'total_commandé'}, inplace=True)

    #fusion avec les produits dans une colonne total_commandé
    stock_data = pd.merge(products_data, commande_total, on='product_id', how='left')

    # calcul du stock_journalier
    stock_data["stock_journalier"] = stock_data["stock"] - stock_data["total_commandé"].fillna(0)

    # sauvegarde du fichier enrichi
    stock_data.to_csv(output_path, index=False)
    print(f"Fichier de stock journalier sauvegardé dans : {output_path}")

def suivie_nouveau_client(date: datetime):
    input_clients_path = os.path.join(f"{CLEAN_DATA_DIR}/clients/{date.year}/{date.month}", f"{date.day}.csv")
    input_orders_path = os.path.join(f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
    output_path = os.path.join(f"{ENRICHED_DATA_DIR}/suivie_nouveau_client/{date.year}/{date.month}", f"{date.day}.csv")
    input_clients_preview_path = os.path.join(f"{CLEAN_DATA_DIR}/clients/{date.year}/{date.month}", f"{date.day-1}.csv")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(input_clients_path):
        raise FileNotFoundError(f"Le fichier {input_clients_path} n'existe pas.")
    if not os.path.exists(input_orders_path):
        raise FileNotFoundError(f"Le fichier {input_orders_path} n'existe pas.")

    clients_data = pd.read_csv(input_clients_path)
    clients_preview_data = pd.read_csv(input_clients_preview_path) 
    orders_data = pd.read_csv(input_orders_path)

    # on compare avec les clients de la veille pour ne garder que les nouveaux clients
    nouveaux_clients = clients_data[~clients_data['customer_id'].isin(clients_preview_data['customer_id'])]
    print(f"Nombre de nouveaux clients : {nouveaux_clients.shape[0]}")

    # On calcule le total dépensé par nouveau client
    depense_total = orders_data[orders_data['customer_id'].isin(nouveaux_clients['customer_id'])].groupby('customer_id')['price'].sum().reset_index()

    #petit renommage
    depense_total.rename(columns={'price': 'total_dépensé'}, inplace=True)

    #fusion avec les clients dans une colonne total_dépensé
    suivie_client_data = pd.merge(nouveaux_clients, depense_total, on='customer_id', how='left')

    # gestion des valeurs manquantes pour les clients sans commande
    suivie_client_data['total_dépensé'] = suivie_client_data['total_dépensé'].fillna(0)

    # sauvegarde du fichier enrichi
    suivie_client_data.to_csv(output_path, index=False)
    print(f"Fichier de suivie nouveau client sauvegardé dans : {output_path}")

def chiffre_affaire_mensuel(date: datetime):
    # on recupere tout les fichiers de chaque jours du mois
    list_input_orders_path = []
    for day in range(1,32):
        path = os.path.join(f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}", f"{day}.csv")
        if os.path.exists(path):
            list_input_orders_path.append(path)

    if not list_input_orders_path:
            raise FileNotFoundError(f"Aucun fichier de commande trouvé pour le mois {date.month} de l'année {date.year}.")

    month_orders_data = pd.DataFrame()
    for fichier in list_input_orders_path:
        data = pd.read_csv(fichier)
        month_orders_data = pd.concat([month_orders_data, data], ignore_index=True)

    print('Données des commandes du mois :')
    print(month_orders_data.shape[0])
    #input_orders_path = os.path.join(f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
    output_path = os.path.join(f"{ENRICHED_DATA_DIR}/chiffre_affaire_mensuel/{date.year}/{date.month}", f"{date.month}.csv")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # On calcule le chiffre d'affaire mensuel
    print("calcul du chiffre d'affaire mensuel...")
    month_orders_data['order_date'] = pd.to_datetime(month_orders_data['order_date'])
    month_orders_data['month'] = month_orders_data['order_date'].dt.to_period('M')
    chiffre_affaire = month_orders_data.groupby('month')['price'].sum().reset_index()

    #petit renommage
    chiffre_affaire.rename(columns={'month': 'mois', 'price': 'chiffre_affaire'}, inplace=True)

    # sauvegarde du fichier enrichi
    chiffre_affaire.to_csv(output_path, index=False)
    print(f"Fichier de chiffre d'affaire mensuel sauvegardé dans : {output_path}")

if __name__=="__main__":
    #suivie_nouveau_client(datetime.strptime("2024-05-10", "%Y-%m-%d"))
    #stock_journalier(datetime.strptime("2024-05-10", "%Y-%m-%d"))
    chiffre_affaire_mensuel(datetime.strptime("2024-05", "%Y-%m"))