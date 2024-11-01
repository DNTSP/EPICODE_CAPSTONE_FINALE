import pandas as pd
import yfinance as yf
import requests
from datetime import datetime
import time
import os

# Path per il file dello script e la directory di output
SCRIPT_PATH = "/Users/emanueledevaleri/Desktop/CAPSTONE EPICODE"
SAVE_PATH = os.path.join(SCRIPT_PATH, "SP500_Data")

# Definizione date globali
START_DATE = "2020-01-01"
END_DATE = "2024-01-01"

# Crea la directory se non esiste
os.makedirs(SAVE_PATH, exist_ok=True)

class DataCollector:
   def get_sp500_companies(self) -> pd.DataFrame:
       try:
           url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
           html = requests.get(url).content
           df = pd.read_html(html)[0]
           print("Successfully retrieved S&P 500 companies list")
           return df
       except Exception as e:
           print(f"Error retrieving S&P 500 companies: {str(e)}")
           raise

   def get_stock_data(self, symbol: str) -> tuple:
       try:
           stock = yf.Ticker(symbol)
           data = stock.history(start=START_DATE, end=END_DATE)
           info = stock.info
           
           if symbol == '^GSPC':
               try:
                   vix = yf.Ticker('^VIX').history(start=START_DATE, end=END_DATE)['Close']
                   data['VIX'] = vix
               except:
                   data['VIX'] = None
           print(f"Successfully retrieved stock data for {symbol} from {START_DATE} to {END_DATE}")
           return data, info
       except Exception as e:
           print(f"Error retrieving stock data for {symbol}: {str(e)}")
           raise

   def get_detailed_company_info(self, symbol: str) -> dict:
       try:
           stock = yf.Ticker(symbol)
           info = stock.info
           return {
               'market_cap': info.get('marketCap'),
               'website': info.get('website'),
               'headquarters': f"{info.get('city', '')}, {info.get('state', '')}, {info.get('country', '')}".strip(', '),
               'founded_year': info.get('founded')
           }
       except:
           return {
               'market_cap': None,
               'website': None,
               'headquarters': None,
               'founded_year': None
           }

class DataTransformer:
   @staticmethod
   def prepare_company_data(df: pd.DataFrame) -> pd.DataFrame:
       df = df.copy()
       df.columns = [x.lower().replace(' ', '_') for x in df.columns]
       return df

   @staticmethod
   def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
       try:
           df = df.copy()
           
           # Moving Averages
           df['sma_50'] = df['Close'].rolling(window=50).mean()
           df['sma_200'] = df['Close'].rolling(window=200).mean()
           
           # Volatility
           df['volatility'] = df['Close'].pct_change().rolling(window=20).std() * (252 ** 0.5)
           
           return df
       except Exception as e:
           print(f"Error calculating technical indicators: {str(e)}")
           raise

def save_to_csv():
   collector = DataCollector()
   transformer = DataTransformer()
   
   print(f"I file verranno salvati in: {SAVE_PATH}")
   print(f"Periodo dati: da {START_DATE} a {END_DATE}")
   
   # 1. Market Sectors
   companies_df = collector.get_sp500_companies()
   companies_df = transformer.prepare_company_data(companies_df)
   sectors_data = companies_df.groupby('gics_sector').agg(
       number_of_companies=('symbol', 'count')
   ).reset_index()
   
   sectors = pd.DataFrame({
       'sector_name': sectors_data['gics_sector'],
       'sector_description': None,
       'weight_in_index': None,
       'number_of_companies': sectors_data['number_of_companies']
   })
   sectors.to_csv(os.path.join(SAVE_PATH, 'market_sectors.csv'), index=False)
   print("Saved market_sectors.csv")

   # 2. SP500 Companies
   companies_data = []
   print("Raccolta dati dettagliati delle aziende...")
   total_companies = len(companies_df['symbol'])
   
   for i, symbol in enumerate(companies_df['symbol'], 1):
       print(f"Processing company {i}/{total_companies}: {symbol}")
       company_info = collector.get_detailed_company_info(symbol)
       company_row = {
           'symbol': symbol,
           'company_name': companies_df[companies_df['symbol'] == symbol]['security'].iloc[0],
           'sector_name': companies_df[companies_df['symbol'] == symbol]['gics_sector'].iloc[0],
           'market_cap': company_info['market_cap'],
           'date_added': companies_df[companies_df['symbol'] == symbol]['date_added'].iloc[0],
           'is_active': 1,
           'headquarters': company_info['headquarters'],
           'founded_year': company_info['founded_year'],
           'website': company_info['website']
       }
       companies_data.append(company_row)
       time.sleep(0.5)

   companies = pd.DataFrame(companies_data)
   companies.to_csv(os.path.join(SAVE_PATH, 'sp500_companies.csv'), index=False)
   print("Saved sp500_companies.csv")

   # 3. SP500 Index
   print("Raccolta dati dell'indice S&P 500...")
   sp500_data, _ = collector.get_stock_data('^GSPC')
   # Verifica date
   sp500_data = sp500_data[(sp500_data.index >= START_DATE) & (sp500_data.index < END_DATE)]
   sp500_data = transformer.calculate_technical_indicators(sp500_data)
   sp500_data.reset_index(inplace=True)
   
   index_data = pd.DataFrame({
       'date': sp500_data['Date'],
       'open_price': sp500_data['Open'],
       'high_price': sp500_data['High'],
       'low_price': sp500_data['Low'],
       'close_price': sp500_data['Close'],
       'volume': sp500_data['Volume'],
       'vix_value': sp500_data.get('VIX', None)
   })
   index_data.to_csv(os.path.join(SAVE_PATH, 'sp500_index.csv'), index=False)
   print("Saved sp500_index.csv")

   # 4. Company Financials e Technical Indicators
   print("Raccolta dati finanziari delle aziende...")
   company_financials = []
   technical_data = []
   
   for i, symbol in enumerate(companies_df['symbol'], 1):
       print(f"Processing financial data for company {i}/{total_companies}: {symbol}")
       try:
           stock_data, stock_info = collector.get_stock_data(symbol)
           # Verifica date
           stock_data = stock_data[(stock_data.index >= START_DATE) & (stock_data.index < END_DATE)]
           stock_data = transformer.calculate_technical_indicators(stock_data)
           stock_data.reset_index(inplace=True)
           
           # Dati finanziari
           financials = pd.DataFrame({
               'symbol': symbol,
               'date': stock_data['Date'],
               'open_price': stock_data['Open'],
               'high_price': stock_data['High'],
               'low_price': stock_data['Low'],
               'close_price': stock_data['Close'],
               'volume': stock_data['Volume'],
               'market_cap': stock_info.get('marketCap')
           })
           company_financials.append(financials)
           
           # Indicatori tecnici
           technicals = pd.DataFrame({
               'symbol': symbol,
               'date': stock_data['Date'],
               'sma_50': stock_data['sma_50'],
               'sma_200': stock_data['sma_200'],
               'volatility': stock_data['volatility']
           })
           technical_data.append(technicals)
           
           time.sleep(0.5)
           
       except Exception as e:
           print(f"Errore nel processare {symbol}: {str(e)}")
           continue

   if company_financials:
       pd.concat(company_financials).to_csv(os.path.join(SAVE_PATH, 'company_financials.csv'), index=False)
       print("Saved company_financials.csv")
   
   if technical_data:
       pd.concat(technical_data).to_csv(os.path.join(SAVE_PATH, 'technical_indicators.csv'), index=False)
       print("Saved technical_indicators.csv")

   print("Data collection completed!")

if __name__ == "__main__":
   save_to_csv()