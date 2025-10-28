import yfinance as yf
import pandas as pd
import os
from sqlalchemy import create_engine, text
import sqlalchemy.types 
from datetime import date, timedelta
import glob

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
FULL_TICKERS_LIST = [
    # Setor de Saúde
    "QUAL3.SA", "DASA3.SA", "HAPV3.SA", "FLRY3.SA", "ODPV3.SA", "RDOR3.SA",
    "MATD3.SA", "GNDI3.SA", "PFRM3.SA", "RADL3.SA", "HYPE3.SA", "BLAU3.SA",
    "ONCO3.SA", "AALR3.SA",
    # Setor de Infraestrutura
    "CCRO3.SA", "ECOR3.SA", "SBSP3.SA", "RAIL3.SA", "RENT3.SA",
    # Setor de Energia
    "ELET3.SA", "ENGI11.SA", "EQTL3.SA", "EGIE3.SA", "CMIG4.SA", "CPLE6.SA",
    "NEOE3.SA", "CPFE3.SA", "TAEE11.SA", "ENEV3.SA", "ISAE4.SA", "AURE3.SA",
    "AESB3.SA", "LIGT3.SA",
    # Setor de Alimentos
    "BRFS3.SA", "BEEF3.SA", "MRFG3.SA", "JBSS3.SA", "ABEV3.SA",
    "CAML3.SA", "MDIA3.SA", "SMTO3.SA",
    # Ibovespa (Índice de referência)
    "^BVSP"
]

def get_actions_data(data_inicio, data_fim, tickers:list):
    """
    Baixa os dados históricos e salva como CSV.
    """
    print(f"\nIniciando download para {len(tickers)} tickers...")
    
    for ticker in tickers:
        try:
            print(f"  > Tentando download para {ticker} de {data_inicio} até {data_fim}")
            
            # Download dos dados
            # auto_adjust=False é importante para manter a coluna 'Adj Close'
            df = yf.download(ticker, start=data_inicio, end=data_fim, progress=False, auto_adjust=False)

            if df.empty:
                print(f"Aviso: Nenhum dado encontrado para o ticker '{ticker}'. O arquivo não será salvo.")
                continue

            # Renomear as colunas ANTES de salvar no CSV (7 COLUNAS DE DADOS)
            df.rename(columns={
                'Open': 'Abertura',
                'High': 'Maxima',
                'Low': 'Minima',
                'Close': 'Fechamento',
                'Volume': 'Volume',
                'Adj Close': 'Fechamento_Ajustado'
            }, inplace=True)
            
            print(f"  > Dados para '{ticker}' baixados com sucesso! Última data: {df.index.max().strftime('%Y-%m-%d')}")
            
            # Lógica para nomear o arquivo
            name = ticker.replace("^","").replace(".SA","")
            yearStart = data_inicio[2:4]
            yearEnd = date.today().strftime('%y-%m-%d') 
            
            nome_arquivo = f'data{yearStart}To{yearEnd}_{name}.csv'
            caminho_arquivo = os.path.join(DATA_DIR, nome_arquivo)
    
            df.to_csv(caminho_arquivo)
        
        except Exception as e:
            print(f"Erro ao baixar dados para o ticker '{ticker}': {e}")
            continue

def process_csv_to_db(caminho_arquivo:str) -> pd.DataFrame:
    """
    Processa um arquivo CSV, extrai o ticker, trata as colunas e 
    retorna um DataFrame limpo para o upload, incluindo a coluna Adj Close.
    """
    try:
        # 1. Leitura inicial para extração do ticker.
        header_info = pd.read_csv(caminho_arquivo, header=None, nrows=2)
        ticker_row = header_info.iloc[1].tolist()
        ticker = [item for item in ticker_row if item != "Ticker" and pd.notna(item)][0]

        # 2. Leitura dos dados reais (pulando o cabeçalho do yfinance)
        df = pd.read_csv(caminho_arquivo, skiprows=3, index_col=0)
        
        # *** ALINHAMENTO CORRETO: 7 colunas no CSV do YFinance ***
        # A ordem no CSV é: Abertura, Maxima, Minima, Fechamento, Fechamento_Ajustado, Volume (se for o CSV que você enviou)
        # Se for o padrão YF com auto_adjust=False: Open, High, Low, Close, Adj Close, Volume
        # Vamos assumir a ordem: Open, High, Low, Close, Adj Close, Volume (traduzida)
        
        # O CSV do Yfinance é salvo com as colunas na ordem original (Open, High, Low, Close, Volume, Adj Close)
        # MAS, como você renomeou e salvou, a ordem no seu CSV é:
        df.columns = ['Abertura', 'Maxima', 'Minima', 'Fechamento', 'Volume', 'Fechamento_Ajustado']
        
        # 3. Preparação para o DB
        df.reset_index(inplace=True)

        # Renomear a coluna de índice (Date) para 'data'
        novo_nome = {df.columns[0]: 'data'}
        df.rename(columns=novo_nome, inplace=True)
        
        # Adiciona a coluna Ticker
        df['ticker'] = ticker

        # 4. Seleciona e reordena as colunas para o DB (Incluindo Fechamento_Ajustado)
        df = df[['data', 'ticker', 'Abertura', 'Maxima', 'Minima', 'Fechamento', 'Fechamento_Ajustado', 'Volume']]
        return df
        
    except Exception as e:
        print(f"Erro ao processar ticker de {caminho_arquivo}: {e}")
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
            "Fechamento_Ajustado" FLOAT NULL, -- *** MUDANÇA CRÍTICA AQUI ***
            "Volume" BIGINT,
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
    get_actions_data(data_inicio=data_inicio_completa, data_fim=data_fim_completa, tickers=FULL_TICKERS_LIST)
    
    # 3. Chame a função de upload para processar os novos arquivos
    upload_all_data()