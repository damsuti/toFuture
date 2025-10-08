import yfinance as yf
import pandas as pd
import re

def get_actions_data(data_inicio, data_fim, tickers:tuple=('PETR4.SA','ELET3.SA', 'EGIE3.SA','BRFS3.SA', "^BVSP")):
    """
    Baixa os dados históricos do índice Ibovespa e retorna um DataFrame.

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
            
            nome_arquivo = 'data_BVSP.csv' if ticker == "^BVSP" else f'data_{re.sub(r"\.SA","",ticker)}.csv'
            df.to_csv(nome_arquivo)
        
        except Exception as e:
            print(f"\nErro ao baixar dados: {e}")
            return pd.DataFrame() # Retorna um DataFrame vazio em caso de erro

# --- Exemplo de uso do script ---
if __name__ == "__main__":
    # Defina o período de tempo para a coleta
    data_inicio = '2020-01-01'
    data_fim = '2024-10-01'
    
    df_ibovespa = get_actions_data(data_inicio, data_fim)
    
    # Exibir as primeiras 5 linhas do DataFrame