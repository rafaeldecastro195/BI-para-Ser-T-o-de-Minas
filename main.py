from flask import Flask, jsonify
from flask_cors import CORS 
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine, text
from flask import request

app = Flask(__name__)
CORS(app)

def get_database_connections():
    conexao_original = mysql.connector.connect(
        host="sertaodeminas.mysql.database.azure.com",
        user="sertaodeminasADM",
        password="stdm#2024",
        database="sertaodeminasdb",
        port=3306
    )
    
    engine_dw = create_engine('mysql+mysqlconnector://sertaodeminasDW:stdm#2024@stdmdatawarehouse.mysql.database.azure.com:3306/stdmdatawarehouse')
    
    return conexao_original, engine_dw

def execute_etl():
    try:
        conexao_original, engine_dw = get_database_connections()
        
        # Primeiro carregamos as dimensões
        queries = {
            'familia': "SELECT recebe_auxilio, cesta as recebeu_cesta FROM visitassertaodeminas",
            'comunidade': "SELECT DISTINCT comunidade, latitude, longitude FROM visitassertaodeminas",
            'data': "SELECT DISTINCT created_at FROM visitassertaodeminas",
        }
        
        dataframes = {
            name: pd.read_sql(query, conexao_original) 
            for name, query in queries.items()
        }
        
        # Limpar tabelas
        with engine_dw.connect() as conn:
            conn.execute(text("DELETE FROM fato_doacao"))
            conn.execute(text("DELETE FROM dim_familia"))
            conn.execute(text("DELETE FROM dim_comunidade"))
            conn.execute(text("DELETE FROM dim_datacriacao"))
            conn.commit()
        
        # Carregar dimensões
        dataframes['familia'].to_sql('dim_familia', engine_dw, if_exists='append', index=False)
        dataframes['comunidade'].to_sql('dim_comunidade', engine_dw, if_exists='append', index=False)
        dataframes['data'].to_sql('dim_datacriacao', engine_dw, if_exists='append', index=False)
        
        # Agora vamos buscar os IDs gerados
        with engine_dw.connect() as conn:
            # Mapear IDs das dimensões
            df_comunidades = pd.read_sql(text("SELECT idComunidade_PK, comunidade FROM dim_comunidade"), conn)
            df_familia = pd.read_sql(text("SELECT idFamilia_PK FROM dim_familia ORDER BY idFamilia_PK"), conn)
            df_data = pd.read_sql(text("SELECT idDataCriacao_PK FROM dim_datacriacao ORDER BY idDataCriacao_PK"), conn)
            
            # Carregar dados para a fato
            query_fato = """SELECT 
                cesta_quantity, num_homens, num_mulheres, 
                sapatos as num_sapatos, kit_higiene as num_kit_higiene,
                leite as num_leite, brinquedos as num_brinquedos,
                biscoitos as num_biscoitos, racao as num_racao,
                roupas as num_roupas, comunidade
                FROM visitassertaodeminas"""
            
            df_fato = pd.read_sql(query_fato, conexao_original)
            
            # Mapear as chaves estrangeiras
            comunidade_mapping = dict(zip(df_comunidades['comunidade'], df_comunidades['idComunidade_PK']))
            df_fato['idComunidade_FK'] = df_fato['comunidade'].map(comunidade_mapping)
            df_fato['idFamilia_FK'] = df_familia['idFamilia_PK'].values
            df_fato['idDataCriacao_FK'] = df_data['idDataCriacao_PK'].values
            
            # Remover coluna auxiliar e carregar fato
            df_fato = df_fato.drop('comunidade', axis=1)
            df_fato.to_sql('fato_doacao', engine_dw, if_exists='append', index=False)
        
        return True
        
    except Exception as e:
        print(f"Erro durante o ETL: {str(e)}")
        return False
    finally:
        if 'conexao_original' in locals():
            conexao_original.close()
            
from flask import Flask, render_template, jsonify

app = Flask(__name__)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/telaanalises")
def tela_analises():
    return render_template("telaanalises.html")

if __name__ == "__main__":
    app.run(debug=True)
@app.route('/executar-etl', methods=['POST'])
def handle_etl():
    try:
        sucesso = execute_etl()
        return jsonify({'sucesso': sucesso})
    except Exception as e:
        return jsonify({'sucesso': False, 'erro': str(e)}), 500