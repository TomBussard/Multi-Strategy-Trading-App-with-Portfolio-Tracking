import sqlite3
import os
from contextlib import contextmanager

class DatabaseManager:
    """
    Gestionnaire de base de données pour la gestion du fonds.
    """
    def __init__(self, db_name="fund_management.db"):
        self.db_name = db_name
        self.conn = None
        self.cursor = None

    @contextmanager
    def get_connection(self):
        """Context manager pour la gestion des connexions"""
        conn = sqlite3.connect(self.db_name)
        try:
            yield conn
        finally:
            conn.close()

    def connect(self):
        """Établit la connexion avec la base de données."""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()

    def close_all_connections(self):
        """Ferme toutes les connexions à la base de données"""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

        try:
            with self.get_connection() as temp_conn:
                temp_cursor = temp_conn.cursor()
                temp_cursor.execute("PRAGMA optimize")
                temp_cursor.execute("PRAGMA wal_checkpoint(FULL)")
        except Exception as e:
            print(f"Erreur lors de la fermeture des connexions : {str(e)}")

    def create_tables(self):
        """Création des tables nécessaires pour le fonds."""
        queries = [
            """CREATE TABLE IF NOT EXISTS Clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nom TEXT NOT NULL,
                profil_risque TEXT CHECK(profil_risque IN ('Low risk', 'Low turnover', 'High yield equity')) NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS Products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nom TEXT NOT NULL,
                type TEXT CHECK(type IN ('Stock', 'Bond', 'ETF')) NOT NULL,
                ticker TEXT UNIQUE NOT NULL,
                volatilite_historique REAL,
                rendement_historique REAL
            )""",
            """CREATE TABLE IF NOT EXISTS Portfolios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                nom TEXT NOT NULL,
                volatilite_cible REAL DEFAULT 0.10,
                max_trades_mensuel INTEGER DEFAULT 2,
                FOREIGN KEY (client_id) REFERENCES Clients(id)
            )""",
            """CREATE TABLE IF NOT EXISTS Portfolios_Holdings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portefeuille_id INTEGER NOT NULL,
                produit_id INTEGER NOT NULL,
                quantite INTEGER NOT NULL,
                volatilite_courante REAL,
                rendement_mensuel REAL,
                FOREIGN KEY (portefeuille_id) REFERENCES Portfolios(id),
                FOREIGN KEY (produit_id) REFERENCES Products(id)
            )""",
            """CREATE TABLE IF NOT EXISTS Returns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER,
                date TEXT NOT NULL,
                return REAL NOT NULL,
                volatilite REAL,
                FOREIGN KEY (product_id) REFERENCES Products(id)
            )""",
            """CREATE TABLE IF NOT EXISTS Deals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portefeuille_id INTEGER,
                produit_id INTEGER,
                type_operation TEXT CHECK(type_operation IN ('achat', 'vente')),
                quantite INTEGER NOT NULL,
                date TEXT NOT NULL,
                volatilite_periode REAL,
                rendement_periode REAL,
                FOREIGN KEY (portefeuille_id) REFERENCES Portfolios(id),
                FOREIGN KEY (produit_id) REFERENCES Products(id)
            )""",
            """CREATE TABLE IF NOT EXISTS Allocations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                actif TEXT NOT NULL,
                poids_cible REAL DEFAULT 0.0,
                FOREIGN KEY (client_id) REFERENCES Clients(id)
            )""",
            """CREATE TABLE IF NOT EXISTS Portfolio_Stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                portfolio_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                volatilite_realisee REAL,
                rendement_realise REAL,
                nombre_trades_mois INTEGER,
                FOREIGN KEY (portfolio_id) REFERENCES Portfolios(id)
            )"""
        ]

        for query in queries:
            self.cursor.execute(query)

        self.conn.commit()
        print("Tables créées avec succès.")

    def clear_all_tables(self):
        """Vide toutes les tables sans supprimer la structure"""
        tables = [
            "Portfolio_Stats",
            "Deals",
            "Portfolios_Holdings",
            "Returns",
            "Allocations",
            "Portfolios",
            "Products",
            "Clients"
        ]

        try:
            self.cursor.execute("PRAGMA foreign_keys = OFF")
            
            for table in tables:
                self.cursor.execute(f"DELETE FROM {table}")
                self.cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table}'")
            
            self.cursor.execute("PRAGMA foreign_keys = ON")
            self.conn.commit()
            print("Toutes les tables ont été vidées.")
            return True
        except Exception as e:
            print(f"Erreur lors du nettoyage des tables : {str(e)}")
            return False

    def reset_database(self):
        """Réinitialise complètement la base de données."""
        try:
            # S'assure que nous avons une connexion valide
            if not self.conn:
                self.connect()
            
            # Vide toutes les tables au lieu de supprimer le fichier
            if not self.clear_all_tables():
                return False
            
            # Recréer les tables
            self.create_tables()
            
            print("Base de données réinitialisée.")
            return True
            
        except Exception as e:
            print(f"Erreur lors de la réinitialisation : {str(e)}")
            return False

    def populate_initial_data(self):
        """Remplit la base de données avec les données initiales."""
        try:
            clients_data = [
                ("Client 1", "Low risk"),
                ("Client 2", "Low turnover"),
                ("Client 3", "High yield equity")
            ]

            products_data = [
                # Actions
                ("Apple", "Stock", "AAPL"),
                ("Google", "Stock", "GOOGL"),
                ("Microsoft", "Stock", "MSFT"),
                ("Tesla", "Stock", "TSLA"),
                ("Nvidia", "Stock", "NVDA"),
                ("Meta Platforms", "Stock", "META"),
                ("Amazon", "Stock", "AMZN"),
                ("Johnson & Johnson", "Stock", "JNJ"),
                ("Visa", "Stock", "V"),
                ("Coca-Cola", "Stock", "KO"),
                ("Procter & Gamble", "Stock", "PG"),
                ("Pfizer", "Stock", "PFE"),
                ("Berkshire Hathaway", "Stock", "BRK-B"),
                ("Walmart", "Stock", "WMT"),
                ("McDonald’s", "Stock", "MCD"),
                ("JPMorgan Chase", "Stock", "JPM"),
                ("Chevron", "Stock", "CVX"),
                ("PepsiCo", "Stock", "PEP"),
                ("Intel", "Stock", "INTC"),
                ("Netflix", "Stock", "NFLX"),
                
                # ETFs
                ("S&P 500 ETF", "ETF", "SPY"),
                ("NASDAQ-100 ETF", "ETF", "QQQ"),
                ("Russell 2000 ETF", "ETF", "IWM"),
                ("MSCI World ETF", "ETF", "URTH"),
                ("Technology Select Sector ETF", "ETF", "XLK"),
                ("Healthcare Select Sector ETF", "ETF", "XLV"),
                ("Financial Select Sector ETF", "ETF", "XLF"),
                ("Consumer Discretionary ETF", "ETF", "XLY"),
                ("Energy Select Sector ETF", "ETF", "XLE"),
                ("Vanguard Total World ETF", "ETF", "VT"),
                
                # Obligations
                ("Treasury Bond ETF", "Bond", "TLT"),
                ("Corporate Bond ETF", "Bond", "LQD"),
                ("iShares TIPS Bond ETF", "Bond", "TIP"),
                ("Aggregate Bond ETF", "Bond", "AGG"),
                ("High Yield Bond ETF", "Bond", "HYG"),
                ("Municipal Bond ETF", "Bond", "MUB"),
                ("Short-Term Treasury ETF", "Bond", "SHY"),
                ("Intermediate Treasury ETF", "Bond", "IEF"),
                ("UltraShort Treasury ETF", "Bond", "TBT"),
                ("Vanguard Total Bond Market ETF", "Bond", "BND")
            ]


            # Ajoute les clients
            self.cursor.executemany(
                "INSERT INTO Clients (nom, profil_risque) VALUES (?, ?)", 
                clients_data
            )

            # Récupère les IDs des clients et créer les portefeuilles
            self.cursor.execute("SELECT id, nom, profil_risque FROM Clients")
            clients = self.cursor.fetchall()

            for client_id, client_nom, profil in clients:
                volatilite_cible = 0.10 if profil == "Low risk" else None
                max_trades = 2 if profil == "Low turnover" else None
                
                self.cursor.execute(
                    """INSERT INTO Portfolios 
                       (client_id, nom, volatilite_cible, max_trades_mensuel) 
                       VALUES (?, ?, ?, ?)""",
                    (client_id, f"Portefeuille de {client_nom}", volatilite_cible, max_trades)
                )

            # Ajoute les produits
            self.cursor.executemany(
                "INSERT INTO Products (nom, type, ticker) VALUES (?, ?, ?)", 
                products_data
            )

            # Attribution des actifs selon le profil
            self.cursor.execute("SELECT id, profil_risque FROM Clients")
            for client_id, profil in self.cursor.fetchall():
                if profil == "Low risk":
                    assets = ["TLT", "LQD", "TIP", "AGG", "SPY", "BND"]
                elif profil == "Low turnover":
                    assets = ["AAPL", "MSFT", "JNJ", "V", "KO", "PG", "SPY", "VT"]
                else:  # High yield equity
                    assets = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA", "META", "AMZN", "NFLX"]

                for asset in assets:
                    self.cursor.execute(
                        "INSERT INTO Allocations (client_id, actif) VALUES (?, ?)", 
                        (client_id, asset)
                    )

            self.conn.commit()
            print("Données initiales ajoutées avec succès.")
            
        except Exception as e:
            print(f"Erreur lors de l'insertion des données : {str(e)}")
            self.conn.rollback()

    def close(self):
        """Ferme la connexion à la base de données."""
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None

if __name__ == "__main__":
    db_manager = DatabaseManager()
    db_manager.connect()
    if db_manager.reset_database():
        db_manager.populate_initial_data()
    db_manager.close()
    print("Base de données réinitialisée avec succès.")