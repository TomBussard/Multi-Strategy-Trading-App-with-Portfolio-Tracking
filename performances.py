import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, timedelta

class PortfolioAnalyzer:
    """Classe pour analyser les performances des portefeuilles"""
    
    def __init__(self, db_name="fund_management.db"):
        self.db_name = db_name
        self.risk_free_rate = 0.02  # Taux sans risque annuel (2%)

    def get_mondays_between_dates(self, start_date, end_date):
        """Retourne la liste des lundis entre deux dates"""
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        
        # Ajuste au lundi suivant
        while start.weekday() != 0:
            start += timedelta(days=1)
        
        dates = pd.date_range(start=start, end=end, freq='W-MON')
        return [d.strftime('%Y-%m-%d') for d in dates]

    def get_portfolio_positions(self, portfolio_id, date):
        """Récupère les positions du portefeuille à une date donnée"""
        with sqlite3.connect(self.db_name) as conn:
            query = """
                WITH DailyPositions AS (
                    SELECT 
                        d.produit_id,
                        pr.ticker,
                        pr.type,
                        d.date,
                        d.type_operation,
                        d.quantite,
                        SUM(CASE 
                            WHEN d.type_operation = 'achat' THEN d.quantite 
                            ELSE -d.quantite 
                        END) OVER (
                            PARTITION BY d.produit_id 
                            ORDER BY d.date
                        ) as running_position
                    FROM Deals d
                    JOIN Products pr ON d.produit_id = pr.id
                    WHERE d.portefeuille_id = ? AND d.date <= ?
                    ORDER BY d.date
                )
                SELECT 
                    ticker,
                    type,
                    running_position as position
                FROM DailyPositions
                WHERE date <= ?
                GROUP BY ticker
                HAVING running_position > 0
            """
            return pd.read_sql_query(query, conn, params=(portfolio_id, date, date))

    def get_asset_price_and_return(self, ticker, date):
        """Récupère le prix et le rendement d'un actif à une date donnée"""
        with sqlite3.connect(self.db_name) as conn:
            query = f"""
                SELECT 
                    Close as price,
                    Returns as daily_return
                FROM {ticker}_data 
                WHERE Date <= ?
                ORDER BY Date DESC
                LIMIT 1
            """
            result = pd.read_sql_query(query, conn, params=(date,))
            if result.empty:
                return None, None
            return float(result['price'].iloc[0]), float(result['daily_return'].iloc[0]) if not pd.isna(result['daily_return'].iloc[0]) else 0

    def get_portfolio_value_and_return(self, portfolio_id, date):
        """Calcule la valeur et le rendement du portefeuille à une date donnée"""
        positions = self.get_portfolio_positions(portfolio_id, date)
        if positions.empty:
            return 0, 0
        
        total_value = 0
        weighted_return = 0
        
        for _, position in positions.iterrows():
            price, daily_return = self.get_asset_price_and_return(position['ticker'], date)
            if price is not None:
                position_value = price * position['position']
                total_value += position_value
                weighted_return += position_value * daily_return if daily_return is not None else 0
        
        portfolio_return = weighted_return / total_value if total_value > 0 else 0
        return total_value, portfolio_return

    def get_weekly_returns(self, portfolio_id, start_date, end_date):
        """Calcule les rendements hebdomadaires du portefeuille"""
        mondays = self.get_mondays_between_dates(start_date, end_date)
        weekly_returns = []
        previous_value = None
        
        for monday in mondays:
            value, daily_return = self.get_portfolio_value_and_return(portfolio_id, monday)
            
            if previous_value is not None and previous_value > 0:
                weekly_return = (value / previous_value) - 1
                
                # Limite les rendements extrêmes à ±10% par semaine
                weekly_return = np.clip(weekly_return, -0.10, 0.10)
                
                weekly_returns.append({
                    'date': monday,
                    'return': weekly_return,
                    'value': value
                })
            
            previous_value = value if value > 0 else previous_value
        
        return pd.DataFrame(weekly_returns)

    def calculate_performance_metrics(self, portfolio_id, start_date, end_date):
        """Calcule toutes les métriques de performance"""
        weekly_returns = self.get_weekly_returns(portfolio_id, start_date, end_date)
        
        if weekly_returns.empty or len(weekly_returns) < 2:
            return {
                'rendement_total': 0,
                'rendement_annualise': 0,
                'volatilite_annualisee': 0,
                'ratio_sharpe': 0,
                'drawdown_max': 0,
                'valeur_finale': 0,
                'nb_semaines_positives': 0,
                'weekly_returns': pd.DataFrame()
            }

        # Calcul du rendement total (méthode géométrique)
        rendement_total = (1 + weekly_returns['return']).prod() - 1
        
        # Nombre de semaines dans la période
        nb_weeks = len(weekly_returns)
        
        # Rendement annualisé
        rendement_annualise = (1 + rendement_total) ** (52 / nb_weeks) - 1
        
        # Volatilité annualisée 
        volatilite_hebdo = weekly_returns['return'].std()
        volatilite_annualisee = volatilite_hebdo * np.sqrt(52) if not pd.isna(volatilite_hebdo) else 0
        
        # Ratio de Sharpe
        excess_return = rendement_annualise - self.risk_free_rate
        ratio_sharpe = excess_return / volatilite_annualisee if volatilite_annualisee != 0 else 0
        
        # Drawdown maximum
        cumulative_returns = (1 + weekly_returns['return']).cumprod()
        rolling_max = cumulative_returns.expanding().max()
        drawdowns = cumulative_returns / rolling_max - 1
        drawdown_max = drawdowns.min()
        
        # Statistiques supplémentaires
        nb_semaines_positives = (weekly_returns['return'] > 0).sum()
        valeur_finale = weekly_returns['value'].iloc[-1] if not weekly_returns['value'].empty else 0
        
        return {
            'rendement_total': rendement_total,
            'rendement_annualise': rendement_annualise,
            'volatilite_annualisee': volatilite_annualisee,
            'ratio_sharpe': ratio_sharpe,
            'drawdown_max': drawdown_max,
            'valeur_finale': valeur_finale,
            'nb_semaines_positives': nb_semaines_positives,
            'weekly_returns': weekly_returns
        }

    def get_portfolio_composition(self, portfolio_id, date=None):
        """Récupère la composition actuelle du portefeuille avec les valeurs de marché"""
        if date is None:
            date = datetime.today().strftime('%Y-%m-%d')
        
        positions = self.get_portfolio_positions(portfolio_id, date)
        if positions.empty:
            return pd.DataFrame()
        
        result = []
        total_value = 0
        
        for _, position in positions.iterrows():
            price, _ = self.get_asset_price_and_return(position['ticker'], date)
            if price is not None:
                market_value = price * position['position']
                total_value += market_value
                
                result.append({
                    'ticker': position['ticker'],
                    'type': position['type'],
                    'quantite': position['position'],
                    'prix': price,
                    'valeur_marche': market_value
                })
        
        composition = pd.DataFrame(result)
        if not composition.empty and total_value > 0:
            composition['poids'] = (composition['valeur_marche'] / total_value * 100)
        
        return composition

if __name__ == "__main__":
    analyzer = PortfolioAnalyzer()
    portfolio_id = 1
    start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
    end_date = datetime.today().strftime('%Y-%m-%d')
    
    metrics = analyzer.calculate_performance_metrics(portfolio_id, start_date, end_date)
    
    print("Métriques de performance:")
    print(f"Rendement total: {metrics['rendement_total']:.2%}")
    print(f"Rendement annualisé: {metrics['rendement_annualise']:.2%}")
    print(f"Volatilité annualisée: {metrics['volatilite_annualisee']:.2%}")
    print(f"Ratio de Sharpe: {metrics['ratio_sharpe']:.2f}")
    print(f"Drawdown maximum: {metrics['drawdown_max']:.2%}")
    print(f"Valeur finale: ${metrics['valeur_finale']:,.2f}")
    print(f"Nombre de semaines positives: {metrics['nb_semaines_positives']}")
    
    composition = analyzer.get_portfolio_composition(portfolio_id)
    if not composition.empty:
        print("Composition du portefeuille:")
        print(composition)
