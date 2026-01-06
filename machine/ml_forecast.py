import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pandas_ta as ta
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, precision_score

# --- CONFIGURAÇÕES ---
DB_USER = "user_invest"
DB_PASSWORD = "senha_forte"
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'

# Definição do objetivo: Prever se a ação vai subir X% nos próximos N dias
DIAS_FUTURO = 30  # Olhar 1 mês a frente
ALVO_RETORNO = 0.05 # 5% de retorno (Alvo agressivo para 1 mês)

def carregar_dados():
    print("Carregando dados do Banco...")
    db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(db_url)
    
    # Pegamos tudo ordenado
    query = 'SELECT * FROM companys ORDER BY ticker, data ASC'
    df = pd.read_sql(query, engine)
    return df

def criar_indicadores(df):
    print("Criando indicadores técnicos (Feature Engineering)...")
    
    # O pandas-ta precisa que o índice seja Datetime se formos usar certas funções,
    # mas aqui vamos aplicar por grupo (Ticker)
    
    df_list = []
    
    for ticker, grupo in df.groupby('ticker'):
        grupo = grupo.copy()
        
        # Evita warnings de processamento em cópias
        grupo.set_index('data', inplace=True)
        
        # 1. RSI (Índice de Força Relativa) - Detecta se está "barato" ou "caro"
        grupo['RSI'] = ta.rsi(grupo['Fechamento'], length=14)
        
        # 2. Médias Móveis (Tendência)
        grupo['SMA_20'] = ta.sma(grupo['Fechamento'], length=20)
        grupo['SMA_50'] = ta.sma(grupo['Fechamento'], length=50)
        
        # Distância do preço para a média (Normalização essencial para IA)
        grupo['Dist_SMA20'] = grupo['Fechamento'] / grupo['SMA_20'] - 1
        
        # 3. Volatilidade (Risco)
        grupo['Volatilidade'] = group_std = grupo['Fechamento'].pct_change().rolling(20).std()
        
        # --- CRIAÇÃO DO ALVO (TARGET) ---
        # Criamos uma coluna com o preço daqui a 30 dias (shift negativo traz o futuro para o presente da linha)
        grupo['Preco_Futuro'] = grupo['Fechamento'].shift(-DIAS_FUTURO)
        
        # O Alvo é 1 se (Preco_Futuro / Preco_Atual) - 1 > 0.05, senão 0
        grupo['Retorno_Futuro'] = (grupo['Preco_Futuro'] / grupo['Fechamento']) - 1
        grupo['Target'] = (grupo['Retorno_Futuro'] > ALVO_RETORNO).astype(int)
        
        # Removemos os últimos 30 dias (pois não têm futuro conhecido para treino) e os primeiros (sem média móvel)
        grupo.dropna(inplace=True)
        
        df_list.append(grupo)
        
    return pd.concat(df_list)

def treinar_modelo(df_completo):
    # Features que a IA vai olhar para tomar decisão
    features = ['RSI', 'Dist_SMA20', 'Volatilidade']
    
    print(f"Treinando modelo com {len(df_completo)} registros...")
    
    # Separação (Nunca faça shuffle em Time Series, mas aqui simplificaremos pegando os dados antigos para treino e recentes para teste)
    # Vamos usar os ultimos 20% dos dados para teste
    corte = int(len(df_completo) * 0.8)
    
    X = df_completo[features]
    y = df_completo['Target']
    
    X_train, X_test = X.iloc[:corte], X.iloc[corte:]
    y_train, y_test = y.iloc[:corte], y.iloc[corte:]
    
    # Modelo: Random Forest (Floresta Aleatória)
    # n_estimators=100 (100 árvores de decisão)
    # min_samples_leaf=5 (Evita decorar dados muito específicos/overfitting)
    model = RandomForestClassifier(n_estimators=100, min_samples_leaf=10, random_state=42)
    model.fit(X_train, y_train)
    
    # Avaliação
    preds = model.predict(X_test)
    precisao = precision_score(y_test, preds)
    
    print("\n--- RESULTADOS DO MODELO ---")
    print(f"Precisão (Quando a IA diz COMPRA, ela acerta?): {precisao:.2%}")
    print("Relatório Completo:")
    print(classification_report(y_test, preds))
    
    return model, features

def prever_agora(df_original, model, features):
    print("\n--- PREVISÕES PARA HOJE (LONG PRAZO) ---")
    print(f"Buscando ações com alta probabilidade de subir {ALVO_RETORNO*100}% em {DIAS_FUTURO} dias.")
    
    # Pega apenas a última linha de cada ticker (o dia mais recente disponível)
    ultimos_dados = df_original.groupby('ticker').tail(1).copy()
    
    # Recalcula indicadores (precisamos garantir que estão lá)
    # Nota: No fluxo real, ideal seria recalcular tudo antes, mas aqui vamos reusar o df processado se possível
    # Assumindo que df_original já saiu da função criar_indicadores
    
    X_hoje = ultimos_dados[features]
    
    # A IA prevê a probabilidade (não só 0 ou 1)
    probs = model.predict_proba(X_hoje)[:, 1] # Pega a chance de ser classe 1 (Subir)
    
    ultimos_dados['Probabilidade_Alta'] = probs
    
    # Filtra e ordena
    top_picks = ultimos_dados[ultimos_dados['Probabilidade_Alta'] > 0.40].sort_values('Probabilidade_Alta', ascending=False)
    
    if top_picks.empty:
        print("Nenhuma ação atingiu o limiar de confiança de 60% hoje.")
    else:
        print(top_picks[['ticker', 'Fechamento', 'RSI', 'Probabilidade_Alta']])

# --- EXECUÇÃO ---
if __name__ == "__main__":
    df_raw = carregar_dados()
    df_proc = criar_indicadores(df_raw)
    
    # Treina o modelo com o passado
    modelo, features_usadas = treinar_modelo(df_proc)
    
    # Usa o modelo para ver quem está bom HOJE
    prever_agora(df_proc, modelo, features_usadas)