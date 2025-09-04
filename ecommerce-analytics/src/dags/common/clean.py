import os.path
from datetime import datetime
import pandas as pd

DATA_DIR = "data"
CLEAN_DATA_DIR = os.path.join(DATA_DIR, "clean_data")

RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")

def clean_clients(date: datetime):
    input_path = os.path.join(f"{RAW_DATA_DIR}/clients/{date.year}/{date.month}", f"{date.day}.csv")
    output_path = os.path.join(f"{CLEAN_DATA_DIR}/clients/{date.year}/{date.month}", f"{date.day}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier {input_path} n'existe pas.")

    input_data = pd.read_csv(input_path) # le fichier a traiter
    print(input_data.head())
    # standart nom colonnes
    input_data.columns = input_data.columns.str.lower()

    # suppression des doublons
    print("Suppression des doublons...")
    clean_data = input_data.drop_duplicates()

    # gestion des valeurs manquantes
    print("Gestion des valeurs manquantes...")
    clean_data = clean_data.dropna(subset=['customer_id', 'email']) 

    # On remplit les valeurs manquantes des autres colonnes avec 'Inconnu'
    clean_data['first_name'] = clean_data['firstname'].fillna('Inconnu')
    clean_data['last_name'] = clean_data['lastname'].fillna('Inconnu')

    #On arange les types
    clean_data['customer_id'] = clean_data['customer_id'].astype(int)
    clean_data["date"] = pd.to_datetime(clean_data["date"])
    clean_data["first_name"] = clean_data["first_name"].astype(str)
    clean_data["last_name"] = clean_data["last_name"].astype(str)
    clean_data["email"] = clean_data["email"].astype(str)


    # sauvegarde du fichier nettoyé
    clean_data.to_csv(output_path, index=False)

def clean_products(date: datetime):
    input_path = os.path.join(f"{RAW_DATA_DIR}/products/{date.year}/{date.month}", f"{date.day}.csv")
    output_path = os.path.join(f"{CLEAN_DATA_DIR}/products/{date.year}/{date.month}", f"{date.day}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier {input_path} n'existe pas.")

    input_data = pd.read_csv(input_path) # le fichier a traiter
    print(input_data.head())

    #standart nom colonnes
    input_data.columns = input_data.columns.str.lower()

    # suppression des doublons
    print("Suppression des doublons...")
    clean_data = input_data.drop_duplicates()

    # gestion des valeurs manquantes
    print("Gestion des valeurs manquantes...")
    clean_data = clean_data.dropna(subset=['product_id'])

    # On remplit les valeurs manquantes des autres colonnes avec 'Inconnu'
    clean_data['product_name'] = clean_data['product_name'].fillna('Inconnu')

    #On arange les types
    clean_data['stock'] = clean_data['stock'].astype(int)
    clean_data["date"] = pd.to_datetime(clean_data["date"])
    clean_data['product_id'] = clean_data['product_id'].astype(int)
    clean_data["product_name"] = clean_data["product_name"].astype(str)
    
    #On supprime les produits avec un stock négatif ou nul
    clean_data = clean_data[clean_data['stock'] > 0]

    # sauvegarde du fichier nettoyé
    clean_data.to_csv(output_path, index=False)


def clean_orders(date: datetime):
    input_path = os.path.join(f"{RAW_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
    output_path = os.path.join(f"{CLEAN_DATA_DIR}/orders/{date.year}/{date.month}", f"{date.day}.csv")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Le fichier {input_path} n'existe pas.")

    input_data = pd.read_csv(input_path) # le fichier a traiter
    print(input_data.head())

    #standart nom colonnes
    input_data.columns = input_data.columns.str.lower()

    # suppression des doublons
    print("Suppression des doublons...")
    clean_data = input_data.drop_duplicates()

    # gestion des valeurs manquantes
    print("Gestion des valeurs manquantes...")
    clean_data = clean_data.dropna(subset=['order_id', 'customer_id', 'product_id', 'quantity', 'order_date','price'])

    # On remplit les valeurs manquantes des autres colonnes avec 'Inconnu'
    clean_data['customer_name'] = clean_data['customer_name'].fillna('Inconnu')
    clean_data['product_name'] = clean_data['product_name'].fillna('Inconnu')

    # On supprime les commandes avec une quantité négative ou nulle
    clean_data = clean_data[clean_data['quantity'] > 0]

    # On supprime les commandes avec un prix négatif
    clean_data = clean_data[clean_data['price'] >= 0]

    #On arange les types
    clean_data['order_date'] = pd.to_datetime(clean_data['order_date'])
    clean_data['quantity'] = clean_data['quantity'].astype(int)
    clean_data['price'] = clean_data['price'].astype(float)
    clean_data['order_id'] = clean_data['order_id'].astype(int)
    clean_data['customer_id'] = clean_data['customer_id'].astype(int)
    clean_data['product_id'] = clean_data['product_id'].astype(int)
    clean_data["customer_name"] = clean_data["customer_name"].astype(str)
    clean_data["product_name"] = clean_data["product_name"].astype(str)

    # sauvegarde du fichier nettoyé
    clean_data.to_csv(output_path, index=False)

if __name__=="__main__":
    clean_clients(datetime.strptime("2024-05-10", "%Y-%m-%d"))
    #clean_products(datetime.strptime("2024-05-10", "%Y-%m-%d"))
    #clean_orders(datetime.strptime("2024-05-10", "%Y-%m-%d"))