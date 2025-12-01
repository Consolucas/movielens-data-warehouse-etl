import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv # Importa a lib

# Carrega as senhas do arquivo .env
load_dotenv()

# --- 1. CONFIGURAÇÃO ---
# Pega os valores do arquivo oculto
user = os.getenv('DB_USER')
password = os.getenv('DB_PASS')
host = os.getenv('DB_HOST')
dbname = os.getenv('DB_NAME')

# Monta a string de conexão segura
db_string = f'postgresql+psycopg2://{user}:{password}@{host}:5432/{dbname}'

# Caminho dos arquivos 
pasta_projeto = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
pasta_dados = os.path.join(pasta_projeto, 'data')

print("--- INICIANDO ETL SIMPLIFICADO ---")

# --- 2. EXTRAÇÃO (Lendo os CSVs) ---
print("1. Lendo arquivos CSV...")
df_ratings = pd.read_csv(os.path.join(pasta_dados, 'ratings.csv'), encoding='latin-1')
df_movies = pd.read_csv(os.path.join(pasta_dados, 'movies.csv'), encoding='latin-1')

# --- 3. TRANSFORMAÇÃO (Arrumando os dados) ---
print("2. Transformando dados...")


# === PREPARANDO DIMENSÃO FILME ===
dim_filme = df_movies.copy()
# Extrai o ano do título (Ex: Pega "1995" de "Toy Story (1995)")
dim_filme['ano_lancamento'] = dim_filme['title'].str.extract(r'\((\d{4})\)$')
dim_filme['ano_lancamento'] = dim_filme['ano_lancamento'].fillna(0).astype(int)
# Remove o ano do texto do título
dim_filme['title'] = dim_filme['title'].str.replace(r'\s*\(\d{4}\)$', '', regex=True)
# Cria a chave numérica (1, 2, 3...)
dim_filme.insert(0, 'movie_key', range(1, 1 + len(dim_filme)))
# Renomeia colunas para bater com o Banco de Dados (Snake Case)
dim_filme = dim_filme.rename(columns={'movieId': 'movie_id'})
# Seleciona só o que vai para o banco
dim_filme = dim_filme[['movie_key', 'movie_id', 'title', 'ano_lancamento', 'genres']]

# === PREPARANDO DIMENSÃO USUÁRIO ===
# Pega usuários únicos e ordena
dim_usuario = df_ratings[['userId']].drop_duplicates().sort_values('userId')
dim_usuario.insert(0, 'user_key', range(1, 1 + len(dim_usuario)))
dim_usuario = dim_usuario.rename(columns={'userId': 'user_id'})


# === PREPARANDO DIMENSÃO TEMPO ===
# Converte timestamp para data real
df_ratings['data_hora'] = pd.to_datetime(df_ratings['timestamp'], unit='s')
df_ratings['data_completa'] = df_ratings['data_hora'].dt.date
# Pega datas únicas
dim_tempo = pd.DataFrame(df_ratings['data_completa'].unique(), columns=['data_completa'])
dim_tempo['data_completa'] = pd.to_datetime(dim_tempo['data_completa']) # Garante que é data
# Cria colunas de data (Ano, Mês, Dia)
dim_tempo['date_key'] = dim_tempo['data_completa'].dt.strftime('%Y%m%d').astype(int)
dim_tempo['ano'] = dim_tempo['data_completa'].dt.year
dim_tempo['mes'] = dim_tempo['data_completa'].dt.month
dim_tempo['dia_semana'] = dim_tempo['data_completa'].dt.day_name()
dim_tempo['trimestre'] = dim_tempo['data_completa'].dt.quarter
# Organiza colunas
dim_tempo = dim_tempo[['date_key', 'data_completa', 'ano', 'mes', 'dia_semana', 'trimestre']]


# === PREPARANDO A TABELA FATO (Os cruzamentos) ===
print("   - Criando Tabela Fato...")
# Cria uma chave de data temporária no ratings para poder cruzar
df_ratings['temp_date_key'] = df_ratings['data_hora'].dt.strftime('%Y%m%d').astype(int)
# Faz os cruzamentos (Merges) para buscar as chaves (Keys)
# 1. Pega user_key
df_fato = pd.merge(df_ratings, dim_usuario, left_on='userId', right_on='user_id', how='left')
# 2. Pega movie_key
df_fato = pd.merge(df_fato, dim_filme, left_on='movieId', right_on='movie_id', how='left')
# 3. Pega date_key
df_fato = pd.merge(df_fato, dim_tempo, left_on='temp_date_key', right_on='date_key', how='left')
# Seleciona colunas finais e renomeia
fato_avaliacoes = df_fato[['date_key', 'user_key', 'movie_key', 'rating']].copy()
fato_avaliacoes.columns = ['fk_tempo', 'fk_usuario', 'fk_filme', 'rating']
# Remove linhas que falharam no cruzamento (limpeza básica)
fato_avaliacoes = fato_avaliacoes.dropna()
# Cria ID da Fato
fato_avaliacoes.insert(0, 'rating_id', range(1, 1 + len(fato_avaliacoes)))


# --- 4. CARGA (Enviando pro Banco) ---
print("3. Carregando no Banco de Dados...")

try:
    engine = create_engine(db_string)
    
    # if_exists='append': Adiciona os dados na tabela que já criamos no SQL
    # index=False: Não envia o índice do Pandas (0, 1, 2...)
    dim_filme.to_sql('dim_filme', engine, if_exists='append', index=False)
    print("   - Filmes carregados.")
    
    dim_usuario.to_sql('dim_usuario', engine, if_exists='append', index=False)
    print("   - Usuários carregados.")
    
    dim_tempo.to_sql('dim_tempo', engine, if_exists='append', index=False)
    print("   - Tempo carregado.")
    
    # chunksize=1000: Envia de mil em mil linhas para não travar
    fato_avaliacoes.to_sql('fato_avaliacoes', engine, if_exists='append', index=False, chunksize=1000)
    print("   - Avaliações carregadas.")

    print("\n--- SUCESSO! ETL CONCLUÍDO ---")

except Exception as e:
    print(f"\nDEU ERRO: {e}")