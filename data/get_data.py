from sqlalchemy import create_engine, text
from datetime import date, timedelta
import yfinance as yf
import pandas as pd
import os
import time
import glob
import sqlalchemy.types 

# Credenciais para a conexão postgresql
DB_USER = "user_invest"
DB_PASSWORD = "senha_forte"
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'
TABLE_NAME = 'companys'

DATA_DIR = './data/bronze'
os.makedirs(DATA_DIR, exist_ok=True) 

# Definição da lista completa de tickers por setor (Formato YFinance)
# No início do seu script
FULL_TICKERS_LIST = [
    # --- AÇÕES "VACAS LEITEIRAS" (Dividendos Altos/Perenes) ---
    "BBAS3.SA", "SANB11.SA", "ITSA4.SA", # Bancos
    "TAEE11.SA", "CPLE6.SA", "EGIE3.SA", # Energia (Transmissão é rei na aposentadoria)
    "CSMG3.SA", "SAPR11.SA",             # Saneamento
    "VALE3.SA", "PETR4.SA",              # Commodities (Para boost de carteira, cuidado com ciclos)
    
    # --- FUNDOS IMOBILIÁRIOS (O Salário Mensal) ---
    # Logística (Galpões)
    "HGLG11.SA", "BTLG11.SA", 
    # Papel (Dívida imobiliária - pagam muito, protegem da inflação)
    "KNIP11.SA", "MXRF11.SA", "CPTS11.SA",
    # Shopping
    "VISC11.SA", "XPML11.SA",
    
    # Benchmarks (Para comparar se você está ganhando do mercado)
    "^BVSP", # Ibovespa
    "FIX.SA" # IFIX (Índice de Fundos Imobiliários)
]

def get_actions_data(data_inicio, data_fim, tickers: list):
    """
    Baixa dados e salva com cabeçalho PADRONIZADO (Nomeando explicitamente).
    """
    print(f"\nIniciando download BLINDADO para {len(tickers)} ativos...")

    for ticker in tickers:
        try:
            print(f"  > Baixando {ticker}...")
            time.sleep(1.5)  # Semaforo

            # Baixa dados brutos
            df = yf.download(ticker, start=data_inicio, end=data_fim, progress=False, actions=True)

            if df.empty:
                print(f"    X Vazio: {ticker}")
                continue

            # --- CORREÇÃO DE ESTRUTURA (Multi-Index) ---
            # Se o yfinance retornar colunas duplas (ex: Price | Ticker), achatamos para simples.
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)

            # --- MAPA DE RENOMEAÇÃO SEGURO (Dicionário) ---
            # Só renomeia se encontrar a chave exata. Não depende da ordem.
            mapa_colunas = {
                'Open': 'Abertura',
                'High': 'Maxima',
                'Low': 'Minima',
                'Close': 'Fechamento',
                'Adj Close': 'Fechamento_Ajustado',
                'Volume': 'Volume',
                'Dividends': 'Dividendos',
                'Stock Splits': 'Desdobramentos'
            }
            df.rename(columns=mapa_colunas, inplace=True)

            # Garante que colunas faltantes existam (Preenche com 0)
            # Ex: Se não teve Splits no periodo, o Yahoo pode nem mandar a coluna.
            colunas_finais_desejadas = [
                'Abertura', 'Maxima', 'Minima', 'Fechamento', 
                'Fechamento_Ajustado', 'Volume', 'Dividendos', 'Desdobramentos'
            ]
            
            for col in colunas_finais_desejadas:
                if col not in df.columns:
                    df[col] = 0.0

            # ---ORDENAÇÃO FINAL ---
            # Força a ordem das colunas para o CSV ser sempre igual
            df = df[colunas_finais_desejadas]

            # Salva o arquivo
            name = ticker.replace("^", "").replace(".SA", "")
            yearStart = data_inicio[2:4]
            yearEnd = date.today().strftime('%y-%m-%d')
            nome_arquivo = f'data{yearStart}To{yearEnd}_{name}.csv'
            caminho_arquivo = os.path.join(DATA_DIR, nome_arquivo)
            
            # ATENÇÃO: Salvamos COM cabeçalho (header=True) para leitura fácil depois
            df.to_csv(caminho_arquivo, header=True)
            print(f"    -> Sucesso: {ticker}")

        except Exception as e:
            print(f"Erro em {ticker}: {e}")
            continue

def process_csv_to_db(caminho_arquivo: str) -> pd.DataFrame:
    """
    Lê o CSV confiando no cabeçalho (Header).
    """
    try:
        # Extração do Ticker do nome do arquivo (Mais seguro que ler de dentro)
        # Ex: data23To25_PETR4.csv -> PETR4
        nome_arquivo = os.path.basename(caminho_arquivo)
        # Pega o que está depois do último '_' e tira o '.csv'
        ticker_bruto = nome_arquivo.split('_')[-1].replace('.csv', '')
        # Reaplica o sufixo .SA se necessário para padronizar no banco
        ticker = ticker_bruto + ".SA" if not ticker_bruto.endswith(".SA") and not ticker_bruto.startswith("^") else ticker_bruto

        # --- LEITURA DIRETA ---
        # Não usamos skiprows fixo, deixamos o pandas achar o header
        df = pd.read_csv(caminho_arquivo, index_col=0)
        
        # Normaliza nomes (caso tenha vindo 'Date' no index)
        df.index.name = 'data'
        df.reset_index(inplace=True)
        
        # Verifica se as colunas essenciais estão lá
        cols_obrigatorias = ['Abertura', 'Fechamento', 'Dividendos', 'Desdobramentos']
        if not all(col in df.columns for col in cols_obrigatorias):
            print(f"⚠️  Arquivo {nome_arquivo} parece incompleto ou formato antigo.")
            return pd.DataFrame()

        # Adiciona coluna Ticker
        df['ticker'] = ticker

        # Seleção Final (Garante ordem para o Banco de Dados)
        df = df[[
            'data', 'ticker', 
            'Abertura', 'Maxima', 'Minima', 'Fechamento', 
            'Fechamento_Ajustado', 'Volume', 
            'Dividendos', 'Desdobramentos'
        ]]
        
        # Tratamento final de Nulos
        df['Dividendos'] = df['Dividendos'].fillna(0)
        df['Desdobramentos'] = df['Desdobramentos'].fillna(0)
        
        return df
        
    except Exception as e:
        print(f"Erro ao processar {caminho_arquivo}: {e}")
        return pd.DataFrame()

def upload_all_data():
    # URL de Conexão com o PostgreSQL
    db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(db_url)
    print("\nConexão com PostgreSQL estabelecida.")

    try:
        # 1. Cria a tabela se ela não existir (Fechamento_Ajustado AGORA ACEITA NULL)
        print("Verificando e criando a tabela 'companys' se necessário...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS companys (
            data DATE,
            ticker VARCHAR(20),
            "Abertura" FLOAT,
            "Maxima" FLOAT,
            "Minima" FLOAT,
            "Fechamento" FLOAT,
            "Fechamento_Ajustado" FLOAT NULL,
            "Volume" BIGINT,
            "Dividendos" FLOAT,
            "Desdobramentos" FLOAT,
            PRIMARY KEY (data, ticker)
        );
        """
        with engine.connect() as connection:
            connection.execute(text(create_table_sql))
            connection.commit()
        print("Tabela 'companys' pronta para receber dados.")

        all_dfs = []
        tickers_missing_adj_close = []
        
        # 2. Percorre todos os arquivos CSV
        for filename in os.listdir(DATA_DIR):
            if filename.endswith('.csv') and filename.startswith('data'):
                full_path = os.path.join(DATA_DIR, filename)
                print(f"Processando arquivo: {filename}")
                
                df = process_csv_to_db(full_path)
                
                if not df.empty:
                    # Contagem de tickers sem Adj Close (se toda a coluna for NaN)
                    if df['Fechamento_Ajustado'].isnull().all():
                        tickers_missing_adj_close.append(df['ticker'].iloc[0])

                    all_dfs.append(df)
                else:
                    print(f"Arquivo {filename} ignorado devido a erro de processamento.")

        if not all_dfs:
            print("Nenhum dado válido para upload encontrado.")
            return

        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"\nTodos os dados processados. Total de {len(final_df)} linhas.")

        # 3. Upload para o Banco de Dados (agora com 7 colunas de dados)
        print(f"Fazendo upload para a tabela '{TABLE_NAME}'...")
        
        final_df.to_sql(
            TABLE_NAME,
            engine,
            if_exists='replace', 
            index=False,
            method='multi',
            dtype={
                'data': sqlalchemy.types.Date,
                'ticker': sqlalchemy.types.VARCHAR(20),
                'Fechamento_Ajustado': sqlalchemy.types.Float  # Tipo agora deve ser Float
            }
        )
        print("Upload concluído com sucesso!")
        
        # 4. Relatório de Tickers sem Fechamento Ajustado
        if tickers_missing_adj_close:
            print("\n--- Relatório de Coleta ---")
            print("Tickers onde a coluna 'Fechamento_Ajustado' está completamente vazia (pode ser delistada ou erro de API):")
            print(tickers_missing_adj_close)

    except Exception as e:
        print(f"ERRO CRÍTICO DURANTE O UPLOAD/CRIAÇÃO DE CHAVE: {e}")

# --- Execução Principal do Script ---
if __name__ == "__main__":
    
    # 1. Define o período completo, do início desejado até HOJE + 1 dia
    data_inicio_completa = '2015-01-01'
    data_fim_completa = (date.today() + timedelta(days=1)).isoformat() 
    
    print(f"Iniciando coleta do {data_inicio_completa} até {date.today().isoformat()} (data exclusiva: {data_fim_completa})")

    # --- Passo 1: Limpar Arquivos CSV Antigos ---
    print("\n--- Limpeza de Arquivos CSV Antigos ---")
    files = glob.glob(os.path.join(DATA_DIR, 'data*.csv'))
    for f in files:
        os.remove(f)
    print(f"Arquivos CSV antigos em {DATA_DIR} removidos. Próxima coleta será limpa.")

    # 2. Baixe TODOS os dados
    get_actions_data(data_inicio=data_inicio_completa, data_fim='2019-01-01', tickers=FULL_TICKERS_LIST)
    
    # 3. Chame a função de upload para processar os novos arquivos
    upload_all_data()

    get_actions_data(data_inicio='2019-01-02', data_fim='2023-01-01',tickers=FULL_TICKERS_LIST)

    upload_all_data()


    get_actions_data(data_inicio='2023-01-02', data_fim=data_fim_completa, tickers=FULL_TICKERS_LIST)

    upload_all_data()