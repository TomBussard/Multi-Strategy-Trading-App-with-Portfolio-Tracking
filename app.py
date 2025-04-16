import streamlit as st
from data_collector import DataCollector
import pandas as pd
import sqlite3
from database_manager import DatabaseManager
import os
from strategies import InvestmentStrategies
from base_update import BaseUpdater
from performances import PortfolioAnalyzer
from datetime import datetime, timedelta
import numpy as np

# Configuration de la page Streamlit
st.set_page_config(page_title="Gestion de Fonds Multi-Stratégies", layout="wide")


# Connexion à la base de données SQLite
DB_NAME = "fund_management.db"

def get_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def reset_and_populate_database():
    db_manager = DatabaseManager()
    db_manager.connect()
    
    print("Réinitialisation de la base...")
    if db_manager.reset_database():
        print("Insertion des données initiales...")
        db_manager.populate_initial_data()
        db_manager.close()
        print("Base de données mise à jour.")
        return True
    else:
        db_manager.close()
        print("Échec de la réinitialisation de la base de données.")
        return False

def generate_transactions_for_client(client_id, start_date=None, end_date=None):
    """Génère les transactions pour un client sur une période donnée"""
    if start_date is None:
        start_date = datetime.today() - timedelta(days=30)
    if end_date is None:
        end_date = datetime.today()

    # Initialise les stratégies
    strategies = InvestmentStrategies()
    
    # Récupère le portfolio du client
    with get_connection() as conn:
        portfolio = conn.execute("""
            SELECT p.id, c.profil_risque
            FROM Portfolios p
            JOIN Clients c ON p.client_id = c.id
            WHERE c.id = ?
        """, (client_id,)).fetchone()
        
        if not portfolio:
            return
        
        portfolio_id, strategy = portfolio
        
        # Supprime les anciennes transactions sur la période
        conn.execute("""
            DELETE FROM Deals 
            WHERE portefeuille_id = ? 
            AND date BETWEEN ? AND ?
        """, (
            portfolio_id,
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))
        conn.commit()

    # Récupère les données de marché pour toute la période une seule fois
    collector = DataCollector(
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )
    collector.fetch_data()
    
    # Pour chaque lundi dans la période
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() == 0:  # Si c'est un lundi
            # Génère et stocke les décisions
            decisions = strategies.apply_strategy(
                strategy, 
                portfolio_id, 
                current_date.strftime('%Y-%m-%d')
            )
            
            if decisions:
                strategies.store_deals(decisions, current_date.strftime('%Y-%m-%d'))
                print(f"Transactions générées pour {current_date.strftime('%Y-%m-%d')}")
        
        current_date += timedelta(days=1)

    # Met à jour les positions du portefeuille
    updater = BaseUpdater()
    updater.update_portfolios()
    updater.close()

# Interface Utilisateur
st.title("📊 Gestion de Fonds Multi-Stratégies")

st.markdown("""
Bienvenue dans l'application de **gestion de fonds multi-stratégies**.  
Cette interface permet de simuler l'évolution d'un portefeuille financier basé sur différents profils de risque, avec des stratégies d'investissement automatiques.

### Profils & stratégies disponibles :
- **Low risk** : stratégie conservatrice visant une faible volatilité (ex. obligations, ETF).
- **Low turnover** : stratégie modérée avec peu de transactions mensuelles.
- **High yield equity** : stratégie dynamique axée sur les actions à fort rendement.

Pour chaque profil, une **sélection d'actifs est automatiquement proposée**, mais vous pouvez librement **ajouter, retirer ou modifier** les actifs dans le menu dédié.

📅 Vous pouvez aussi **choisir la période d'analyse** via les sélecteurs de dates.

✅ Une fois vos choix faits, **cliquez sur “💾 Sauvegarder les modifications”** pour lancer les calculs, générer les transactions et mettre à jour les performances du portefeuille.

""")

# Gestion des Clients dans la sidebar
st.sidebar.header("👤 Gestion des Clients")

# Récupère les clients existants
with get_connection() as conn:
    clients = conn.execute("SELECT * FROM Clients").fetchall()

client_options = {client["nom"]: client["id"] for client in clients}

# Sélection d'un client existant
if client_options:
    selected_client = st.sidebar.selectbox("Sélectionner un client", list(client_options.keys()))
    
    with get_connection() as conn:
        client_profile = conn.execute(
            "SELECT profil_risque FROM Clients WHERE id = ?",
            (client_options[selected_client],)
        ).fetchone()["profil_risque"]
        
        st.sidebar.info(f"Profil de risque : {client_profile}")

# Ajouter un nouveau client
new_client_name = st.sidebar.text_input("Nom du nouveau client")
risk_options = ["Low risk", "Low turnover", "High yield equity"]
new_client_risk = st.sidebar.selectbox("Sélectionner le profil de risque", risk_options)

if st.sidebar.button("➕ Ajouter Client"):
    if new_client_name:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO Clients (nom, profil_risque) VALUES (?, ?)", 
                (new_client_name, new_client_risk)
            )
            client_id = cursor.lastrowid
            
            volatilite_cible = 0.10 if new_client_risk == "Low risk" else None
            max_trades = 2 if new_client_risk == "Low turnover" else None
            
            cursor.execute(
                """INSERT INTO Portfolios (client_id, nom, volatilite_cible, max_trades_mensuel) 
                   VALUES (?, ?, ?, ?)""",
                (client_id, f"Portefeuille de {new_client_name}", volatilite_cible, max_trades)
            )
            
            if new_client_risk == "Low risk":
                assets = ["TLT", "LQD", "TIP", "AGG", "SPY", "BND"]
            elif new_client_risk == "Low turnover":
                assets = ["AAPL", "MSFT", "JNJ", "V", "KO", "PG", "SPY", "VT"]
            else:  # High yield equity
                assets = ["AAPL", "GOOGL", "MSFT", "TSLA", "NVDA", "META", "AMZN", "NFLX"]

            
            for asset in assets:
                cursor.execute(
                    "INSERT INTO Allocations (client_id, actif) VALUES (?, ?)", 
                    (client_id, asset)
                )
            
            conn.commit()
            st.success(f"✅ Client '{new_client_name}' ajouté avec succès.")
            st.rerun()

# Boutons de contrôle dans la sidebar
if st.sidebar.button("🔄 Régénérer les transactions"):
    if selected_client:
        updater = BaseUpdater()
        updater.store_deals()
        updater.update_portfolios()
        updater.close()
        st.sidebar.success("✅ Transactions régénérées avec succès !")
        st.rerun()

if st.sidebar.button("🗑️ Réinitialiser la base de données"):
    if reset_and_populate_database():
        st.sidebar.success("✅ Base de données réinitialisée avec succès !")
    else:
        st.sidebar.error("❌ Erreur lors de la réinitialisation de la base de données.")
    st.rerun()

# Sélecteur de benchmark pour le calcul du beta
st.sidebar.subheader("📈 Benchmark de référence")
benchmark_ticker = st.sidebar.selectbox(
    "Choisissez un benchmark pour le calcul du bêta :",
    ["SPY", "QQQ", "VT", "URTH", "XLK", "XLF", "IWM"]
)

# Affichage du contenu principal
if selected_client:
    st.header(f"📊 Portfolio de {selected_client}")

    # Sélection de la période
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "Date de début", 
            value=datetime(2015, 1, 1), 
            min_value=datetime(2010, 1, 1),
            max_value=datetime.today()
    )
    with col2:
        end_date = st.date_input(
            "Date de fin", 
            value=datetime.today(),
            min_value=datetime(2010, 1, 1),
            max_value=datetime.today()
        )

    # Récupère l'ID du portefeuille
    with get_connection() as conn:
        portfolio_id = conn.execute(
            "SELECT id FROM Portfolios WHERE client_id = ?",
            (client_options[selected_client],)
        ).fetchone()["id"]

    # Gestion des actifs
    st.header("🔄 Gestion des Actifs")

    # Récupérer les actifs actuels du client
    with get_connection() as conn:
        assets_data = conn.execute("""
            SELECT a.actif, p.type
            FROM Allocations a
            JOIN Products p ON a.actif = p.ticker
            WHERE a.client_id = ?
        """, (client_options[selected_client],)).fetchall()

    # Organise les actifs par classe
    asset_classes = {
        "Stock": [],
        "Bond": [],
        "ETF": []
    }

    for asset in assets_data:
        asset_classes[asset[1]].append(asset[0])

    # Interface de sélection des actifs
    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("📈 Actions")
        available_stocks = [
        "AAPL", "GOOGL", "MSFT", "TSLA", "NVDA", "META", "AMZN", "JNJ", "V", "KO",
        "PG", "PFE", "BRK-B", "WMT", "MCD", "JPM", "CVX", "PEP", "INTC", "NFLX"]

        selected_stocks = st.multiselect(
            "Sélectionner les actions",
            available_stocks,
            default=asset_classes["Stock"]
        )

    with col2:
        st.subheader("💰 Obligations")
        available_bonds = [
         "TLT", "LQD", "TIP", "AGG", "HYG", "MUB", "SHY", "IEF", "TBT", "BND"]
        selected_bonds = st.multiselect(
            "Sélectionner les obligations",
            available_bonds,
            default=asset_classes["Bond"]
        )

    with col3:
        st.subheader("📊 ETFs")
        available_etfs = [
        "SPY", "QQQ", "IWM", "URTH", "XLK", "XLV", "XLF", "XLY", "XLE", "VT"]
        selected_etfs = st.multiselect(
            "Sélectionner les ETFs",
            available_etfs,
            default=asset_classes["ETF"]
        )

    # Sauvegarder les modifications
    if st.button("💾 Sauvegarder les modifications"):
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM Allocations WHERE client_id = ?", 
                (client_options[selected_client],)
            )
            
            all_selected = selected_stocks + selected_bonds + selected_etfs
            for asset in all_selected:
                cursor.execute(
                    "INSERT INTO Allocations (client_id, actif) VALUES (?, ?)",
                    (client_options[selected_client], asset)
                )
            
            conn.commit()

            # Regénère les transactions pour la période sélectionnée
            generate_transactions_for_client(
                client_options[selected_client],
                start_date,
                end_date
            )
            
            st.success("✅ Modifications sauvegardées avec succès!")
            st.rerun()

    # Affichage des données de marché
    selected_assets = selected_stocks + selected_bonds + selected_etfs
    if selected_assets:
        st.header("📊 Données de Marché")
        
        collector = DataCollector(
            tickers=selected_assets,
            start_date=start_date.strftime('%Y-%m-%d'),
            end_date=end_date.strftime('%Y-%m-%d')
        )
        collector.fetch_data()
        data = collector.get_data()

        if data:
            # Création du DataFrame combiné
            combined_df = pd.DataFrame()
            for ticker, df in data.items():
                if df.index.name == "Date" or "Date" not in df.columns:
                    df = df.reset_index()

                if "Close" not in df.columns:
                    continue

                close_df = df[["Date", "Close"]].copy()
                close_df = close_df.rename(columns={"Close": ticker})

                if combined_df.empty:
                    combined_df = close_df
                else:
                    combined_df = pd.merge(combined_df, close_df, on="Date", how="outer")

            if not combined_df.empty:
                combined_df = combined_df.sort_values("Date").set_index("Date").ffill().bfill()
                
                st.subheader("📈 Prix de clôture")
                st.dataframe(combined_df)

                # Performance normalisée
                def normalize_base100(df):
                    return df.div(df.iloc[0]).mul(100)
                
                st.subheader("📊 Performance normalisée (Base 100)")
                normalized_df = normalize_base100(combined_df)
                st.dataframe(normalized_df)
                st.line_chart(normalized_df)

    # Description de la stratégie choisie
    st.subheader("Stratégie d'investissement appliquée")

    if client_profile == "Low risk":
        st.markdown("""
        Cette stratégie vise une **volatilité annualisée cible de 10%**.  
        Elle privilégie les obligations et les ETF défensifs.  
        Les signaux d'achat sont déclenchés lorsque les rendements récents sont positifs et la volatilité modérée.  
        Des ventes peuvent être réalisées si un actif devient trop volatil ou affiche une tendance baissière.
        """)
    elif client_profile == "Low turnover":
        st.markdown("""
        Cette stratégie cherche à **limiter les mouvements à 2 transactions par mois**.  
        Elle favorise des positions stables, en actions et ETF, avec une logique de rotation lente.  
        Des signaux sont déclenchés uniquement le **1er et 3ème lundi du mois**, selon l’évolution des rendements sur 5 et 20 jours.
        """)
    elif client_profile == "High yield equity":
        st.markdown("""
        Cette stratégie est orientée vers la **recherche de rendement élevé via des actions à forte volatilité**.  
        Elle repose sur l’analyse du momentum à court, moyen et long terme.  
        Les achats sont déclenchés si les tendances sont haussières et la volatilité maîtrisée.  
        Un stop-loss est activé si la tendance s'inverse ou si les pertes dépassent -7% sur 30 jours.
        """)


    # Historique des transactions
    st.header("📝 Historique des Transactions")
    with get_connection() as conn:
        transactions = pd.read_sql_query("""
            SELECT 
                d.date as "Date",
                pr.ticker as "Actif",
                pr.type as "Type d'actif",
                d.type_operation as "Opération",
                d.quantite as "Quantité"
            FROM Deals d
            JOIN Products pr ON d.produit_id = pr.id
            JOIN Portfolios p ON d.portefeuille_id = p.id
            WHERE p.client_id = ? 
            AND d.date BETWEEN ? AND ?
            ORDER BY d.date DESC
        """, conn, params=(
            client_options[selected_client],
            start_date.strftime('%Y-%m-%d'),
            end_date.strftime('%Y-%m-%d')
        ))

        if not transactions.empty:
            st.dataframe(transactions)
        else:
            st.info("Aucune transaction sur la période sélectionnée.")

    # Composition actuelle du portefeuille
    st.subheader("📊 Composition du Portefeuille")
    analyzer = PortfolioAnalyzer()
    composition = analyzer.get_portfolio_composition(portfolio_id)
    if not composition.empty:
        composition['poids'] = composition['poids'].map("{:.2f}%".format)
        composition['valeur_marche'] = composition['valeur_marche'].map("${:,.2f}".format)
        composition['prix'] = composition['prix'].map("${:.2f}".format)
        st.dataframe(composition)

    # Performances du portefeuille
    st.header("📈 Performances du Portefeuille")

    # Calcule et affiche les métriques de performance
    metrics = analyzer.calculate_performance_metrics(
        portfolio_id,
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    # Graphique des rendements hebdomadaires
    st.subheader("📈 Évolution des rendements")
    weekly_returns = metrics['weekly_returns']
    if not weekly_returns.empty:
        st.line_chart(weekly_returns.set_index('date')['return'])

    # Affiche les métriques principales
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric(
            "Rendement Total",
            f"{metrics['rendement_total']:.2%}",
            delta=f"{metrics['rendement_annualise']:.2%} (annualisé)"
        )
    with col2:
        st.metric("Volatilité Annualisée", f"{metrics['volatilite_annualisee']:.2%}")
    with col3:
        st.metric("Ratio de Sharpe", f"{metrics['ratio_sharpe']:.2f}")
    with col4:
        st.metric("Drawdown Maximum", f"{metrics['drawdown_max']:.2%}")
    with col5:
        def get_weekly_returns(ticker, dates):
            start = min(dates)
            end = max(dates)
            with sqlite3.connect("fund_management.db") as conn:
                table = f"{ticker}_data"
                df = pd.read_sql_query(f"""
                    SELECT Date, Returns FROM {table}
                    WHERE Date BETWEEN ? AND ?
                    ORDER BY Date
                """, conn, params=(start, end))
                df = df.dropna()
                df['Date'] = pd.to_datetime(df['Date'])
                return df.set_index('Date')['Returns']

        if not weekly_returns.empty:
            benchmark_returns = get_weekly_returns(benchmark_ticker, list(weekly_returns['date']))
            portfolio_returns = weekly_returns.set_index('date')['return']

            portfolio_returns.index = pd.to_datetime(portfolio_returns.index).date
            benchmark_returns.index = pd.to_datetime(benchmark_returns.index).date

            common_index = benchmark_returns.index.intersection(portfolio_returns.index)

            if len(common_index) > 0:
                x = benchmark_returns.loc[common_index]
                y = portfolio_returns.loc[common_index]
                beta = np.cov(y, x)[0, 1] / np.var(x)
                alpha = (y.mean() - beta * x.mean()) * 52  # Annualisation
                st.metric(f"Bêta ({benchmark_ticker})", f"{beta:.2f}")
                st.metric(f"Alpha annuel ({benchmark_ticker})", f"{alpha:.2%}")
            else:
                st.metric(f"Bêta ({benchmark_ticker})", "N/A")
                st.metric(f"Alpha annuel ({benchmark_ticker})", "N/A")


    # Graphique d’évolution des rendements cumulés
    st.subheader("💹 Évolution des rendements cumulés du portefeuille (Base 100)")
    if not weekly_returns.empty:
        evolution_df = weekly_returns.copy()
        evolution_df['rendement_cumule'] = (1 + evolution_df['return']).cumprod() * 100
        st.line_chart(evolution_df.set_index('date')['rendement_cumule'])
        
    # Courbe de drawdown
    st.subheader("📉 Drawdown dans le temps")
    if not weekly_returns.empty:
        cumulative = (1 + weekly_returns['return']).cumprod()
        peak = cumulative.cummax()
        drawdown = (cumulative - peak) / peak
        drawdown_df = pd.DataFrame({
            'date': weekly_returns['date'],
            'drawdown': drawdown
        }).set_index('date')
        st.line_chart(drawdown_df)
