import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
from database_manager import DatabaseManager

class InvestmentStrategies:
    def __init__(self, db_name="fund_management.db"):
        self.db_manager = DatabaseManager(db_name)
        self.db_manager.connect()
        self.volatility_window = 252
        self.max_monthly_trades = 2
        self.target_volatility = 0.10

    def get_products_by_strategy(self, strategy, portfolio_id):
        """Récupère uniquement les actifs alloués au client"""
        with self.db_manager.conn:
            allocations = self.db_manager.conn.execute("""
                SELECT a.actif, p.type 
                FROM Allocations a
                JOIN Portfolios po ON a.client_id = po.client_id
                JOIN Products p ON a.actif = p.ticker
                WHERE po.id = ?
            """, (portfolio_id,)).fetchall()
            
            if not allocations:
                print(f"Aucune allocation trouvée pour portfolio {portfolio_id}")
                return []
            
            filtered_tickers = []
            for ticker, asset_type in allocations:
                if strategy == "Low risk":
                    filtered_tickers.append(ticker)
                elif strategy == "Low turnover" and asset_type in ["Stock", "ETF"]:
                    filtered_tickers.append(ticker)
                elif strategy == "High yield equity" and asset_type == "Stock":
                    filtered_tickers.append(ticker)
            
            print(f"Produits trouvés pour portfolio {portfolio_id}: {filtered_tickers}")
            return filtered_tickers

    def calculate_portfolio_volatility(self, returns_df):
        """Calcule la volatilité du portefeuille"""
        if returns_df.empty:
            return 0
        
        cov_matrix = returns_df.cov() * np.sqrt(252)
        n_assets = len(returns_df.columns)
        if n_assets == 0:
            return 0
            
        weights = np.array([1/n_assets] * n_assets)
        portfolio_variance = np.dot(weights.T, np.dot(cov_matrix, weights))
        return np.sqrt(portfolio_variance)

    def calculate_returns(self, prices):
        """Calcule les rendements logarithmiques"""
        return np.log(prices / prices.shift(1))

    def get_market_data(self, tickers, date):
        """Récupère et prépare les données de marché jusqu'à une date spécifique"""
        conn = self.db_manager.conn
        market_data = {}

        for ticker in tickers:
            try:
                table_name = f"{ticker.replace('-', '_')}_data"
                df = pd.read_sql_query(f"""
                    SELECT * FROM "{table_name}"
                    WHERE Date <= ?
                    ORDER BY Date DESC
                    LIMIT 252
                """, conn, params=(date,))
                
                if df.empty:
                    print(f"Aucune donnée trouvée pour {ticker} à la date {date}")
                    continue

                if "Close" not in df.columns and "Adj_Close" in df.columns:
                    df["Close"] = df["Adj_Close"]
                
                if "Close" not in df.columns:
                    print(f"Pas de colonne Close pour {ticker}")
                    continue

                df['Returns'] = self.calculate_returns(df['Close'])
                df['Volatility'] = df['Returns'].rolling(window=self.volatility_window).std() * np.sqrt(252)
                
                market_data[ticker] = df

            except Exception as e:
                print(f"Erreur lors du chargement des données pour {ticker}: {e}")

        return market_data

    def get_current_position(self, portfolio_id, ticker, date):
        """Récupère la position actuelle pour un actif à une date donnée"""
        query = """
            WITH PositionSum AS (
                SELECT 
                    SUM(CASE 
                        WHEN type_operation = 'achat' THEN quantite 
                        WHEN type_operation = 'vente' THEN -quantite 
                    END) as position
                FROM Deals d
                JOIN Products p ON d.produit_id = p.id
                WHERE d.portefeuille_id = ? 
                AND p.ticker = ?
                AND d.date <= ?
            )
            SELECT COALESCE(position, 0) as position FROM PositionSum
        """
        result = self.db_manager.conn.execute(query, (portfolio_id, ticker, date)).fetchone()
        return result[0] if result else 0

    def apply_low_risk_strategy(self, data, portfolio_id, date):
        """Stratégie visant une volatilité annualisée de 10%"""
        decisions = []
        
        # Calcule la volatilité du portefeuille actuel
        portfolio_returns = pd.DataFrame()
        for ticker, df in data.items():
            if 'Returns' not in df.columns or df.empty:
                continue
            portfolio_returns[ticker] = df['Returns']
        
        portfolio_vol = self.calculate_portfolio_volatility(portfolio_returns)
        
        for ticker, df in data.items():
            if 'Returns' not in df.columns or df.empty:
                continue

            # Utilise des périodes plus longues
            returns_10d = df['Returns'].head(10).mean()
            returns_30d = df['Returns'].head(30).mean()
            volatility = df['Returns'].head(30).std() * np.sqrt(252)
            current_position = self.get_current_position(portfolio_id, ticker, date)
            
            # Vérifie le type d'actif
            asset_type = self.db_manager.conn.execute(
                "SELECT type FROM Products WHERE ticker = ?", 
                (ticker,)
            ).fetchone()[0]
            
            if current_position > 0:
                if (
                    (returns_30d < -0.02 and asset_type == 'Stock') or  # -2% sur 30 jours pour les actions
                    (returns_30d < -0.01 and asset_type == 'Bond') or   # -1% sur 30 jours pour les obligations
                    volatility > self.target_volatility * 1.5           # 50% au-dessus de la cible
                ):
                    quantity = min(current_position, max(5, int(current_position * 0.25)))  # Vendre max 25%
                    decisions.append((portfolio_id, ticker, "vente", quantity))
            else:
                if (
                    returns_10d > 0 and 
                    returns_30d > 0 and 
                    volatility < self.target_volatility * 1.2
                ):
                    # Position size basée sur la volatilité
                    position_size = int(10000 / (volatility * 100))  # Plus la vol est basse, plus la position est grande
                    position_size = max(5, min(20, position_size))  # Borner entre 5 et 20
                    decisions.append((portfolio_id, ticker, "achat", position_size))

        return decisions

    def apply_low_turnover_strategy(self, data, portfolio_id, date):
        """Stratégie limitée à 2 trades par mois (1 trade tous les 2 lundis)"""
        decisions = []
        
        # Converti la date en datetime pour manipulation
        current_date = pd.to_datetime(date)
        start_of_month = current_date.replace(day=1)
        
        # Détermine si c'est un lundi "actif" (1er ou 3ème lundi du mois)
        week_number = (current_date.day - 1) // 7 + 1
        is_trading_week = week_number in [1, 3]  # Seulement 1er et 3ème lundi
        
        if not is_trading_week:
            return decisions
        
        # Vérifie si on a déjà fait un trade cette semaine
        trades_this_week = self.db_manager.conn.execute("""
            SELECT COUNT(*) FROM Deals 
            WHERE portefeuille_id = ? 
            AND date = ?
        """, (portfolio_id, date)).fetchone()[0]
        
        if trades_this_week > 0:
            return decisions

        # Continue avec la logique de trading uniquement si c'est un lundi "actif"
        for ticker, df in data.items():
            if 'Returns' not in df.columns or df.empty:
                continue

            returns_5d = df['Returns'].head(5).mean()
            returns_20d = df['Returns'].head(20).mean()
            current_position = self.get_current_position(portfolio_id, ticker, date)

            if current_position > 0:
                if returns_5d < returns_20d * 0.90: 
                    quantity = min(current_position, random.randint(15, 25))
                    decisions.append((portfolio_id, ticker, "vente", quantity))
                    break  # Un seul trade par lundi actif
            else:
                if returns_5d > returns_20d * 1.10: 
                    decisions.append((portfolio_id, ticker, "achat", random.randint(15, 25)))
                    break  # Un seul trade par lundi actif

        return decisions


    def apply_high_yield_strategy(self, data, portfolio_id, date):
        """Stratégie actions agressive avec gestion du risque"""
        decisions = []
        max_position_size = 25  # Taille maximale de position

        for ticker, df in data.items():
            if 'Returns' not in df.columns or df.empty:
                continue

            returns_5d = df['Returns'].head(5).mean()
            returns_10d = df['Returns'].head(10).mean()
            returns_30d = df['Returns'].head(30).mean()
            volatility = df['Returns'].head(30).std() * np.sqrt(252)
            current_position = self.get_current_position(portfolio_id, ticker, date)

            if current_position > 0:
                # Stop loss à -7% ou tendance baissière
                if (
                    returns_30d < -0.07 or  # Stop loss
                    (returns_5d < 0 and returns_10d < 0 and returns_30d < 0)  # Tendance baissière confirmée
                ):
                    quantity = min(current_position, max(5, int(current_position * 0.75)))
                    decisions.append((portfolio_id, ticker, "vente", quantity))
            else:
                # Achat sur momentum positif
                if (
                    returns_5d > 0 and 
                    returns_10d > 0 and 
                    returns_30d > 0 and  # Tendance haussière confirmée
                    volatility < 0.40    # Volatilité maximale de 40%
                ):
                    # Position size inversement proportionnelle à la volatilité
                    position_size = int(15000 / (volatility * 100))
                    position_size = max(5, min(max_position_size, position_size))
                    decisions.append((portfolio_id, ticker, "achat", position_size))

        return decisions

    def get_monthly_trades_count(self, portfolio_id, month):
        """Compte le nombre de trades pour un portefeuille dans un mois donné"""
        query = """
            SELECT COUNT(*) FROM Deals 
            WHERE portefeuille_id = ? 
            AND strftime('%Y-%m', date) = ?
        """
        count = self.db_manager.conn.execute(query, (portfolio_id, month)).fetchone()[0]
        return count

    def apply_strategy(self, strategy, portfolio_id, date):
        """Applique la stratégie appropriée selon le profil"""
        tickers = self.get_products_by_strategy(strategy, portfolio_id)
        if not tickers:
            print(f"Aucun actif trouvé pour {strategy}")
            return []

        market_data = self.get_market_data(tickers, date)
        if not market_data:
            print(f"Aucune donnée de marché pour {strategy} à la date {date}")
            return []

        if strategy == "Low risk":
            return self.apply_low_risk_strategy(market_data, portfolio_id, date)
        elif strategy == "Low turnover":
            return self.apply_low_turnover_strategy(market_data, portfolio_id, date)
        elif strategy == "High yield equity":
            return self.apply_high_yield_strategy(market_data, portfolio_id, date)
        
        return []

    def execute_strategies(self, date=None):
        """Exécute les stratégies pour tous les portefeuilles"""
        if date is None:
            date = datetime.today()
            while date.weekday() != 0:
                date = date - timedelta(days=1)
            date = date.strftime('%Y-%m-%d')

        print(f"Exécution des stratégies pour {date}")
        
        query = """
            SELECT p.id, c.profil_risque
            FROM Portfolios p
            JOIN Clients c ON p.client_id = c.id
        """
        
        with self.db_manager.conn:
            portfolios = self.db_manager.conn.execute(query).fetchall()

        if not portfolios:
            print("Aucun portefeuille trouvé.")
            return

        print(f"Portefeuilles trouvés : {portfolios}")
        
        all_decisions = []
        for portfolio_id, profil_risque in portfolios:
            decisions = self.apply_strategy(profil_risque, portfolio_id, date)
            if decisions:
                all_decisions.extend(decisions)

        print(f"Décisions générées pour {date}: {all_decisions}")
        self.store_deals(all_decisions, date)

    def store_deals(self, deals, date):
        """Enregistre les transactions dans la base de données"""
        if not deals:
            print(f"Aucune transaction pour {date}")
            return

        with self.db_manager.conn:
            for portfolio_id, ticker, type_operation, quantite in deals:
                try:
                    produit_id = self.db_manager.conn.execute(
                        "SELECT id FROM Products WHERE ticker = ?",
                        (ticker,)
                    ).fetchone()

                    if not produit_id:
                        print(f"❌ Produit {ticker} introuvable")
                        continue

                    existing = self.db_manager.conn.execute("""
                        SELECT COUNT(*) FROM Deals 
                        WHERE portefeuille_id = ? 
                        AND produit_id = ? 
                        AND date = ?
                        AND type_operation = ?
                    """, (portfolio_id, produit_id[0], date, type_operation)).fetchone()[0]

                    if not existing:
                        df = pd.read_sql_query(f"""
                            SELECT return, volatilite 
                            FROM Returns 
                            WHERE product_id = ? 
                            AND date <= ?
                            ORDER BY date DESC 
                            LIMIT 20
                        """, self.db_manager.conn, params=(produit_id[0], date))

                        volatilite_periode = df['volatilite'].mean() if not df.empty else None
                        rendement_periode = df['return'].mean() if not df.empty else None

                        self.db_manager.conn.execute("""
                            INSERT INTO Deals (
                                portefeuille_id, produit_id, type_operation, 
                                quantite, date, volatilite_periode, rendement_periode
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            portfolio_id, produit_id[0], type_operation, 
                            quantite, date, volatilite_periode, rendement_periode
                        ))

                        print(f"Transaction ajoutée : {portfolio_id} | {ticker} | {type_operation} {quantite}")

                except Exception as e:
                    print(f"Erreur lors de l'insertion : {e}")

            self.db_manager.conn.commit()

if __name__ == "__main__":
    strategies = InvestmentStrategies()
    today = datetime.today()
    for i in range(4):
        date = today - timedelta(days=i*7)
        while date.weekday() != 0:
            date = date - timedelta(days=1)
        strategies.execute_strategies(date.strftime('%Y-%m-%d'))