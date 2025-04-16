import sqlite3
import os
import subprocess
import sys
from data_collector import DataCollector
from base_update import BaseUpdater
from datetime import datetime, timedelta

DB_NAME = "fund_management.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def fetch_market_data():
    with get_connection() as conn:
        tickers = [row["ticker"] for row in conn.execute("SELECT ticker FROM Products").fetchall()]

    if not tickers:
        print("Aucun actif trouvé dans la base.")
        return

    collector = DataCollector(tickers)
    collector.fetch_data()
    print("Données de marché récupérées.")

def execute_strategies():
    updater = BaseUpdater()
    updater.store_deals()
    updater.update_portfolios()
    print("Stratégies appliquées.")

def update_portfolio_stats():
    with get_connection() as conn:
        portfolios = conn.execute("""
            SELECT p.id FROM Portfolios p
        """).fetchall()

        for portfolio in portfolios:
            portfolio_id = portfolio[0]
            stats = conn.execute("""
                WITH MonthlyStats AS (
                    SELECT 
                        strftime('%Y-%m', date) as month,
                        AVG(volatilite_periode),
                        SUM(CASE WHEN type_operation = 'achat' THEN quantite ELSE -quantite END),
                        COUNT(*),
                        SUM(rendement_periode)
                    FROM Deals
                    WHERE portefeuille_id = ?
                    GROUP BY strftime('%Y-%m', date)
                )
                SELECT * FROM MonthlyStats ORDER BY month DESC LIMIT 1
            """, (portfolio_id,)).fetchone()

            if stats:
                conn.execute("""
                    INSERT INTO Portfolio_Stats (
                        portfolio_id, date, volatilite_realisee, 
                        rendement_realise, nombre_trades_mois
                    ) VALUES (?, ?, ?, ?, ?)
                """, (
                    portfolio_id,
                    datetime.now().strftime("%Y-%m-%d"),
                    stats[1], stats[4], stats[3]
                ))

        conn.commit()
    print("Statistiques mises à jour.")

def setup_database_if_needed():
    if not os.path.exists(DB_NAME):
        from database_manager import DatabaseManager
        db_manager = DatabaseManager()
        db_manager.connect()
        db_manager.create_tables()
        db_manager.populate_initial_data()
        db_manager.close()
        print("✅ Base de données initialisée.")

def main():
    print("Initialisation du projet...")
    setup_database_if_needed()
    fetch_market_data()
    execute_strategies()
    update_portfolio_stats()

    print("Lancement de l'application Streamlit...")
    subprocess.run([sys.executable, "run_app.py"])

if __name__ == "__main__":
    main()
