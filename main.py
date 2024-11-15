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
        
        # Queries para extrair dados
        queries = {
            'familia': "SELECT recebe_auxilio, cesta as recebeu_cesta FROM visitassertaodeminas",
            'comunidade': "SELECT DISTINCT comunidade, latitude, longitude FROM visitassertaodeminas",
            'data': "SELECT DISTINCT created_at FROM visitassertaodeminas",
            'fato': """SELECT cesta_quantity, num_homens, num_mulheres, 
                    sapatos as num_sapatos, kit_higiene as num_kit_higiene,
                    leite as num_leite, brinquedos as num_brinquedos,
                    biscoitos as num_biscoitos, racao as num_racao,
                    roupas as num_roupas FROM visitassertaodeminas"""
        }
        
        # Extrair dados
        dataframes = {
            name: pd.read_sql(query, conexao_original) 
            for name, query in queries.items()
        }
        
        # Limpar tabelas existentes
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
        dataframes['fato'].to_sql('fato_doacao', engine_dw, if_exists='append', index=False)
        
        return True
        
    except Exception as e:
        print(f"Erro durante o ETL: {str(e)}")
        return False
    finally:
        if 'conexao_original' in locals():
            conexao_original.close()

@app.route('/executar-etl', methods=['POST'])
def handle_etl():
    try:
        sucesso = execute_etl()
        return jsonify({'sucesso': sucesso})
    except Exception as e:
        return jsonify({'sucesso': False, 'erro': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)