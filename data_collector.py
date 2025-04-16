import pandas as pd
import numpy as np
import yfinance as yf
import os
import sqlite3
from datetime import datetime, timedelta

class DataCollector:
    """
    Module de récupération et traitement des données financières.
    """
    def __init__(self, tickers=None, start_date=None, end_date=None):
        if start_date is None:
            start_date = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
            
        if end_date is None:
            end_date = datetime.today().strftime("%Y-%m-%d")
            
        if tickers is None:
            with sqlite3.connect("fund_management.db") as conn:
                tickers = [row[0] for row in conn.execute("SELECT DISTINCT ticker FROM Products").fetchall()]
        
        self.tickers = tickers
        self.start_date = start_date
        self.end_date = end_date
        self.data = {}

    def fetch_data(self):
        """
        Télécharge les données de marché pour les tickers spécifiés.
        """
        with sqlite3.connect("fund_management.db") as conn:
            for ticker in self.tickers:
                try:
                    print(f"Téléchargement des données pour {ticker}...")
                    
                    # Télécharge les données avec yfinance
                    stock = yf.Ticker(ticker)
                    data = stock.history(start=self.start_date, end=self.end_date)
                    
                    if not data.empty:
                        # S'assure que l'index est au format datetime
                        data.index = pd.to_datetime(data.index)
                        
                        # Calcule les rendements et la volatilité
                        data['Returns'] = data['Close'].pct_change()
                        data['Volatility'] = data['Returns'].rolling(window=20).std() * np.sqrt(252)
                        
                        # Réinitialise l'index pour avoir Date comme colonne
                        data = data.reset_index()
                        data['Date'] = data['Date'].dt.strftime('%Y-%m-%d')
                        
                        # Sauvegarde dans une table spécifique au ticker
                        table_name = f"{ticker.replace('-', '_')}_data"
                        data.to_sql(table_name, conn, if_exists="replace", index=False)
                        
                        # Met à jour la table Returns
                        product_id = conn.execute(
                            "SELECT id FROM Products WHERE ticker = ?", 
                            (ticker,)
                        ).fetchone()[0]
                        
                        # Supprime les anciennes données
                        conn.execute("DELETE FROM Returns WHERE product_id = ?", (product_id,))
                        
                        # Insère les nouvelles données
                        returns_data = []
                        for _, row in data.iterrows():
                            if pd.notna(row['Returns']) and pd.notna(row['Volatility']):
                                returns_data.append((
                                    product_id,
                                    row['Date'],
                                    float(row['Returns']),
                                    float(row['Volatility'])
                                ))
                        
                        if returns_data:
                            conn.executemany(
                                """INSERT INTO Returns 
                                   (product_id, date, return, volatilite) 
                                   VALUES (?, ?, ?, ?)""",
                                returns_data
                            )
                        
                        conn.commit()
                        print(f"Données sauvegardées pour {ticker}")
                        self.data[ticker] = data
                    else:
                        print(f"Aucune donnée disponible pour {ticker}")

                except Exception as e:
                    print(f"Erreur pour {ticker}: {str(e)}")
                    continue

    def get_data(self):
        """ Retourne les données brutes téléchargées. """
        return self.data

    def ensure_all_data_available(self):
        """
        Vérifie que toutes les données nécessaires sont disponibles.
        """
        missing_data = []
        with sqlite3.connect("fund_management.db") as conn:
            for ticker in self.tickers:
                table_name = f"{ticker.replace('-', '_')}_data"
                exists = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?", 
                    (table_name,)
                ).fetchone()
                if not exists:
                    missing_data.append(ticker)
        
        if missing_data:
            print(f"Données manquantes pour: {', '.join(missing_data)}")
            return False
        return True

class MarketDataHandler:
    """
    Classe permettant de gérer l'enregistrement des données.
    """
    def __init__(self, data, output_dir="market_data"):
        self.data = data
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def save_to_csv(self):
        """
        Sauvegarde les données nettoyées sous forme de fichiers CSV.
        """
        for ticker, df in self.data.items():
            file_path = os.path.join(self.output_dir, f"{ticker}.csv")
            df.to_csv(file_path, index=False)
            print(f"Données sauvegardées sous {file_path}")

if __name__ == "__main__":
    # Initialise la base de données si elle n'existe pas
    if not os.path.exists("fund_management.db"):
        from database_manager import DatabaseManager
        db_manager = DatabaseManager()
        db_manager.connect()
        db_manager.create_tables()
        db_manager.populate_initial_data()
        db_manager.close()
    
    # Récupère tous les tickers depuis la base de données
    with sqlite3.connect("fund_management.db") as conn:
        tickers = [row[0] for row in conn.execute("SELECT DISTINCT ticker FROM Products").fetchall()]
    
    # Créer une instance et télécharger les données
    collector = DataCollector(tickers=tickers)
    collector.fetch_data()
    
    # Vérifie que toutes les données sont disponibles
    if collector.ensure_all_data_available():
        print("Toutes les données sont disponibles")
    else:
        print("Certaines données sont manquantes")