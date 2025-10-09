import yfinance as yf
import pandas as pd
import os

def get_actions_data(data_inicio, data_fim, tickers:list=["PETR4.SA", "ELET3.SA", "EGIE3.SA", "CPFE3.SA", "CMIG4.SA", "ENEV3.SA",
    "JBSS3.SA", "BRFS3.SA", "MRFG3.SA", "BEEF3.SA", "SMTO3.SA", "ABEV3.SA",
    "CCRO3.SA", "ECOR3.SA", "SBSP3.SA", "CSMG3.SA", "WEGE3.SA", "^BVSP"]):
    """
    Baixa os dados históricos  e retorna um DataFrame.

    Args:
        data_inicio (str): Data de início no formato 'YYYY-MM-DD'.
        data_fim (str): Data de término no formato 'YYYY-MM-DD'.

    Returns:
        pd.DataFrame: DataFrame com os dados do Ibovespa.
    """
    print(f"Baixando dados do Ibovespa ({tickers})...")
    for ticker in tickers:
        try:
            # A chamada yf.download retorna um DataFrame
            df = yf.download(ticker, start=data_inicio, end=data_fim)

            # Renomear as colunas para português
            df.rename(columns={
                'Open': 'Abertura',
                'High': 'Maxima',
                'Low': 'Minima',
                'Close': 'Fechamento',
                'Volume': 'Volume',
                'Adj Close': 'Fechamento_Ajustado'
            }, inplace=True)
#            df_ibov = df_ibov.drop(df_ibov.index[[1,2]])
            df.reset_index(drop=True)
            #df_ibov.iloc
            print("\nDados baixados com sucesso!")
            name = ticker.replace("^","").replace(".SA","")
            yearStart = data_inicio[2:4]
            yearEnd = data_fim[2:4]
            nome_arquivo = f'data{yearStart}To{yearEnd}_{name}.csv'
            caminho_arquivo = os.path.join(f'./data/bronze', nome_arquivo)
    
    # Salva o CSV dentro do diretório bronze
            df.to_csv(caminho_arquivo)
        
        except Exception as e:
            print(f"\nErro ao baixar dados: {e}")
            return pd.DataFrame() # Retorna um DataFrame vazio em caso de erro
        

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


# --- Exemplo de uso do script ---
if __name__ == "__main__":
    # Defina o período de tempo para a coleta
    data_inicio = '2020-01-01'
    data_fim = '2024-12-31'
    get_actions_data(data_inicio, data_fim)
    get_actions_data('2015-01-01','2019-12-31')
    df = process_csv_to_db('./data/bronze/data15To19_BVSP.csv')
    print(df.head(10))
    # Exibir as primeiras 5 linhas do DataFrame