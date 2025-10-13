import yfinance as yf
import pandas as pd
import os
from sqlalchemy import create_engine, text
import sqlalchemy.types 



#Credenciais para a conexão postgresql
DB_USER = "user_invest"
DB_PASSWORD = "senha_forte"
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'postgres'
TABLE_NAME = 'companys'

DATA_DIR = './data/bronze'
FILE_PATTERN = 'data*.csv'

    #    """Conecta ao DB, processa e sobe todos os CSVs para a tabela única."""

def get_actions_data(data_inicio, data_fim, tickers:list=["PETR4.SA", "ELET3.SA", "EGIE3.SA", "CPFE3.SA", "CMIG4.SA", "ENEV3.SA",
    "JBSS3.SA", "BRFS3.SA", "MRFG3.SA", "BEEF3.SA", "SMTO3.SA", "ABEV3.SA",
    "CCRO3.SA", "ECOR3.SA", "SBSP3.SA", "CSMG3.SA", "WEGE3.SA", "^BVSP"]):
    """
    Baixa os dados históricos e retorna um DataFrame.
    """
    print(f"Baixando dados para os tickers: {tickers}...")
    for ticker in tickers:
        try:
            df = yf.download(ticker, start=data_inicio, end=data_fim)

            # --- Adição da verificação de DataFrame vazio ---
            if df.empty:
                print(f"Aviso: Nenhum dado encontrado para o ticker '{ticker}'. O arquivo não será salvo.")
                continue  # Pula para o próximo ticker na lista
            # -----------------------------------------------

            # Renomear as colunas para português
            df.rename(columns={
                'Open': 'Abertura',
                'High': 'Maxima',
                'Low': 'Minima',
                'Close': 'Fechamento',
                'Volume': 'Volume',
                'Adj Close': 'Fechamento_Ajustado'
            }, inplace=True)
            
            # Não é necessário resetar o índice se o YFinance retornar o Date como índice
            # df.reset_index(drop=True)  # Esta linha não estava fazendo o que você esperava
            
            print(f"\nDados para '{ticker}' baixados com sucesso!")
            
            name = ticker.replace("^","").replace(".SA","")
            yearStart = data_inicio[2:4]
            yearEnd = data_fim[2:4]
            nome_arquivo = f'data{yearStart}To{yearEnd}_{name}.csv'
            caminho_arquivo = os.path.join(f'./data/bronze', nome_arquivo)
    
            df.to_csv(caminho_arquivo)
        
        except Exception as e:
            print(f"\nErro ao baixar dados para o ticker '{ticker}': {e}")
            continue # Continua o loop para os outros tickers

def process_csv_to_db(caminho_arquivo:str) -> pd.DataFrame:
    #Tratamento do cabeçalho do csv.
    #criacao da coluna "ticker"
    # criacao da coluna "ticker"
    # 1. Leitura inicial para extração do ticker.
    try:
        header_info = pd.read_csv(caminho_arquivo, header=None, nrows=2)
        # O ticker está na segunda linha.
        ticker_row = header_info.iloc[1].tolist()
        
        # CORREÇÃO DO ERRO 'bool' object is not subscriptable
        ticker = [item for item in ticker_row if item != "Ticker" and pd.notna(item)][0]

    except Exception as e:
        # Se ocorrer um erro durante o processamento do cabeçalho
        print(f"Erro ao extrair ticker de {caminho_arquivo}: {e}")
        return pd.DataFrame() # retorna dataframe vazio
    
    # 2. Leitura dos dados reais
    df = pd.read_csv(caminho_arquivo,skiprows=3,index_col=0)
    # df.columns = ['Abertura','Máxima','Mínima','Fechamento','Fechamento_Ajustado',"Volume"]

    # Linha CORRIGIDA (5 elementos na ordem do CSV):
    df.columns = ['Fechamento', 'Maxima', 'Minima', 'Abertura', 'Volume']
    # 4. Preparação para o DB: resetar o índice e adicionar a coluna Ticker
    df.reset_index(inplace=True)

    # --- CORREÇÃO MAIS SEGURA: Renomear a primeira coluna pela POSIÇÃO ---
    # A primeira coluna (índice 0) após o reset_index é SEMPRE a data.
    
    # Cria um dicionário com o nome da primeira coluna (df.columns[0]) e o novo nome ('data')
    novo_nome = {df.columns[0]: 'data'}
    
    # Renomeia a coluna usando esse dicionário
    df.rename(columns=novo_nome, inplace=True)
    # --------------------------------------------------------------------
    
    # Adiciona a coluna Ticker
    df['ticker'] = ticker

    # 5. Seleciona e reordena as colunas para o DB 
    df = df[['data', 'ticker', 'Abertura', 'Maxima', 'Minima', 'Fechamento', 'Volume']]
    return df

def upload_all_data():
    # URL de Conexão com o PostgreSQL
    db_url = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(db_url)
    print("Conexão com PostgreSQL estabelecida.")

    try:
        # Cria a tabela se ela não existir
        print("Verificando e criando a tabela 'companys' se necessário...")
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS companys (
            data DATE,
            ticker VARCHAR(20),
            "Abertura" FLOAT,
            "Maxima" FLOAT,
            "Minima" FLOAT,
            "Fechamento" FLOAT,
            "Volume" BIGINT,
            PRIMARY KEY (data, ticker)
        );
        """
        with engine.connect() as connection:
            connection.execute(text(create_table_sql))
            connection.commit()
        print("Tabela 'companys' pronta para receber dados.")

        all_dfs = []
        
        # --- NOVO CÓDIGO INSERIDO AQUI ---
        # Percorre todos os arquivos CSV na pasta
        for filename in os.listdir(DATA_DIR):
            if filename.endswith('.csv') and filename.startswith('data'):
                full_path = os.path.join(DATA_DIR, filename)
                print(f"Processando arquivo: {filename}")
                
                df = process_csv_to_db(full_path)
                
                if not df.empty:
                    all_dfs.append(df)
                else:
                    print(f"Arquivo {filename} ignorado devido a erro de processamento.")
        # --- FIM DO NOVO CÓDIGO ---

        if not all_dfs:
            print("Nenhum dado válido para upload encontrado.")
            return

        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"\nTodos os dados processados. Total de {len(final_df)} linhas.")

        # Upload para o Banco de Dados
        print(f"Fazendo upload para a tabela '{TABLE_NAME}'...")

        final_df.to_sql(
            TABLE_NAME,
            engine,
            if_exists='append',
            index=False,
            dtype={
                'data': sqlalchemy.types.Date,
                'ticker': sqlalchemy.types.VARCHAR(20)
            }
        )
        print("Upload concluído com sucesso!")

    except Exception as e:
        print(f"ERRO CRÍTICO DURANTE O UPLOAD/CRIAÇÃO DE CHAVE: {e}")

# --- Exemplo de uso do script ---
if __name__ == "__main__":
    # Defina o período de tempo para a coleta
    data_inicio = '2020-01-01'
    data_fim = '2024-12-31'
    
    # 1. Primeiro, baixe os dados para o primeiro período
    get_actions_data(data_inicio, data_fim)
    
    # 2. Depois, baixe os dados para o segundo período
    get_actions_data('2015-01-01','2019-12-31')
    
    # 3. Por fim, chame a função de upload para processar todos os arquivos criados
    upload_all_data()