import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from strategies import InvestmentStrategies
import numpy as np

DB_NAME = "fund_management.db"

class BaseUpdater:
    def __init__(self):
        self.conn = sqlite3.connect(DB_NAME)
        self.cursor = self.conn.cursor()
        self.strategies = InvestmentStrategies(DB_NAME)

    def store_deals(self):
        """Applique les stratégies et stocke les transactions"""
        print("Génération des transactions...")
        
        # Récupère tous les portefeuilles existants une seule fois
        portfolios = self.cursor.execute("""
            SELECT DISTINCT p.id, c.profil_risque 
            FROM Portfolios p
            JOIN Clients c ON p.client_id = c.id
        """).fetchall()

        # Pour chaque lundi des 4 dernières semaines
        today = datetime.today()
        for i in range(4):
            date = today - timedelta(days=i*7)
            while date.weekday() != 0:
                date = date - timedelta(days=1)
            
            print(f"Exécution des stratégies pour {date.strftime('%Y-%m-%d')}")
            
            # Pour chaque portfolio existant
            for portfolio_id, strategy in portfolios:
                print(f"Application de la stratégie {strategy} pour portfolio {portfolio_id}")
                
                # Génère et stocke les décisions
                decisions = self.strategies.apply_strategy(
                    strategy, 
                    portfolio_id, 
                    date.strftime('%Y-%m-%d')
                )
                
                if decisions:
                    self.strategies.store_deals(decisions, date.strftime('%Y-%m-%d'))
                    print(f"{len(decisions)} transactions générées pour portfolio {portfolio_id}")

        print("Génération des transactions terminée")

    def update_portfolios(self):
        """Met à jour les positions des portefeuilles"""
        print("Mise à jour des positions des portefeuilles...")
        
        # Récupère toutes les transactions
        self.cursor.execute("""
            SELECT 
                d.portefeuille_id, d.produit_id, d.type_operation, 
                d.quantite, pr.ticker
            FROM Deals d
            JOIN Products pr ON d.produit_id = pr.id
            ORDER BY d.date
        """)
        deals = self.cursor.fetchall()

        if not deals:
            print("Aucune transaction trouvée")
            return

        print(f"Traitement de {len(deals)} transactions...")

        # Garde une trace des positions courantes
        current_positions = {}
        for deal in deals:
            portefeuille_id, produit_id, type_operation, quantite, ticker = deal
            
            if portefeuille_id not in current_positions:
                current_positions[portefeuille_id] = {}
            
            if ticker not in current_positions[portefeuille_id]:
                current_positions[portefeuille_id][ticker] = {
                    'quantity': 0,
                    'produit_id': produit_id
                }

            # Met à jour la position
            if type_operation == "achat":
                current_positions[portefeuille_id][ticker]['quantity'] += quantite
            else:  # vente
                current_positions[portefeuille_id][ticker]['quantity'] -= quantite

        # Met à jour la base de données
        for portefeuille_id, positions in current_positions.items():
            # Supprime toutes les positions actuelles
            self.cursor.execute("""
                DELETE FROM Portfolios_Holdings 
                WHERE portefeuille_id = ?
            """, (portefeuille_id,))
            
            # Insère les nouvelles positions
            for ticker, details in positions.items():
                if details['quantity'] > 0:
                    self.cursor.execute("""
                        INSERT INTO Portfolios_Holdings (
                            portefeuille_id, produit_id, quantite
                        ) VALUES (?, ?, ?)
                    """, (portefeuille_id, details['produit_id'], details['quantity']))
                    print(f"✅ Position mise à jour: Portfolio {portefeuille_id} | {ticker} | {details['quantity']}")

        self.conn.commit()
        print("\n✅ Positions des portefeuilles mises à jour")

    def close(self):
        """Ferme la connexion à la base de données"""
        if hasattr(self, 'strategies'):
            self.strategies.db_manager.close()
        if hasattr(self, 'conn'):
            self.conn.close()
            self.conn = None
            self.cursor = None

if __name__ == "__main__":
    updater = BaseUpdater()
    updater.store_deals()
    updater.update_portfolios()
    updater.close()
    print("Mise à jour terminée avec succès")