"""
Módulo Persistor - Lógica de Banco de Dados
==========================================
Funções auxiliares para configurar o TimescaleDB.

Este script é responsável por:
1. Conectar ao banco de dados PostgreSQL.
2. Garantir que a extensão TimescaleDB esteja ativa.
3. Criar as tabelas (Metadados e Hipertabela) conforme a
   arquitetura "larga" (wide) [cite: janiolos/smart_modular/janiolos-smart_modular-a7e13f1804a23c980ab6761ee459ee9366a439f6/ARQUITETURA.MD, Seção 4.2].
4. Criar os índices e políticas de retenção/compressão.
"""

import psycopg2
import os
import logging
import time

logger = logging.getLogger(__name__)


def get_db_connection():
    """
    Estabelece e retorna uma conexão com o banco de dados PostgreSQL.
    Lida com retentativas em caso de falha na inicialização.
    """
    conn = None
    retries = 10
    while retries > 0 and conn is None:
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT', 5432),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                dbname=os.getenv('POSTGRES_DB')
            )
        except psycopg2.OperationalError as e:
            logger.warning(f"Não foi possível conectar ao banco de dados: {e}. Tentando novamente em 5s...")
            retries -= 1
            time.sleep(5)

    if conn is None:
        logger.critical("Não foi possível conectar ao banco de dados após várias tentativas. Encerrando.")
        raise Exception("Falha na conexão com o banco de dados")

    return conn


def setup_database():
    """
    Garante que a extensão TimescaleDB esteja ativa e que
    as tabelas de metadados e hipertabelas existam.
    (Ref: ARQUITETURA.MD, Seção 4.3)
    """
    logger.info("Configurando o banco de dados...")
    conn = None
    try:
        conn = get_db_connection()
        # Usamos autocommit=True para comandos DDL (Data Definition Language)
        conn.autocommit = True

        with conn.cursor() as cursor:

            logger.info("1/5: Ativando extensão 'timescaledb'...")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")

            logger.info("2/5: Criando tabela de metadados 'pmu_devices' (Seção 4.1)...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pmu_devices (
                    pmu_id INTEGER PRIMARY KEY,
                    station_name VARCHAR(100),
                    description TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            logger.info("3/5: Criando hipertabela 'phasor_data' (Esquema Largo)...")
            # Este é o esquema "Largo" (Wide) [cite: janiolos/smart_modular/janiolos-smart_modular-a7e13f1804a23c980ab6761ee459ee9366a439f6/ARQUITETURA.MD, Seção 4.2]
            # É mais rápido para consultas e melhor para compressão.
            # Para o MVP, criamos colunas genéricas suficientes.
            # Se você tiver mais de 12 fasores, adicione mais colunas.
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS phasor_data (
                    time TIMESTAMPTZ NOT NULL,
                    pmu_id INTEGER NOT NULL REFERENCES pmu_devices(pmu_id),
                    stat_word BIGINT,
                    freq DOUBLE PRECISION,
                    rocof DOUBLE PRECISION,
                    phasor_0_mag DOUBLE PRECISION, phasor_0_ang_deg DOUBLE PRECISION,
                    phasor_1_mag DOUBLE PRECISION, phasor_1_ang_deg DOUBLE PRECISION,
                    phasor_2_mag DOUBLE PRECISION, phasor_2_ang_deg DOUBLE PRECISION,
                    phasor_3_mag DOUBLE PRECISION, phasor_3_ang_deg DOUBLE PRECISION,
                    phasor_4_mag DOUBLE PRECISION, phasor_4_ang_deg DOUBLE PRECISION,
                    phasor_5_mag DOUBLE PRECISION, phasor_5_ang_deg DOUBLE PRECISION,
                    phasor_6_mag DOUBLE PRECISION, phasor_6_ang_deg DOUBLE PRECISION,
                    phasor_7_mag DOUBLE PRECISION, phasor_7_ang_deg DOUBLE PRECISION,
                    phasor_8_mag DOUBLE PRECISION, phasor_8_ang_deg DOUBLE PRECISION,
                    phasor_9_mag DOUBLE PRECISION, phasor_9_ang_deg DOUBLE PRECISION,
                    phasor_10_mag DOUBLE PRECISION, phasor_10_ang_deg DOUBLE PRECISION,
                    phasor_11_mag DOUBLE PRECISION, phasor_11_ang_deg DOUBLE PRECISION,
                    analog_0 DOUBLE PRECISION, analog_1 DOUBLE PRECISION,
                    analog_2 DOUBLE PRECISION, analog_3 DOUBLE PRECISION,
                    analog_4 DOUBLE PRECISION, analog_5 DOUBLE PRECISION,
                    analog_6 DOUBLE PRECISION, analog_7 DOUBLE PRECISION,
                    digital_0 BIGINT, digital_1 BIGINT
                );
            """)

            logger.info("4/5: Convertendo 'phasor_data' para Hipertabela (Seção 4.3)...")
            try:
                cursor.execute("SELECT create_hypertable('phasor_data', 'time');")
                logger.info("Hipertabela 'phasor_data' criada com sucesso.")
            except psycopg2.errors.lookup("42P07"):  # duplicate_table
                logger.info("Hipertabela 'phasor_data' já existe.")
            except psycopg2.errors.IOError:  # table_not_empty
                logger.info("Hipertabela 'phasor_data' já existe e contém dados.")

            logger.info("5/5: Criando índices e definindo políticas (Seção 4.3)...")
            # Este índice é VITAL para performance de consulta
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_phasor_data_pmu_id_time ON phasor_data (pmu_id, time DESC);")

            # Políticas de gerenciamento de dados (opcionais, mas recomendadas)
            cursor.execute(
                "ALTER TABLE phasor_data SET (timescaledb.compress, timescaledb.compress_segmentby = 'pmu_id');",
                raise_exception=False)
            cursor.execute("SELECT add_compression_policy('phasor_data', INTERVAL '7 days');", raise_exception=False)
            cursor.execute("SELECT add_retention_policy('phasor_data', INTERVAL '2 years');", raise_exception=False)

        logger.info("Banco de dados configurado com sucesso.")

    except Exception as e:
        logger.error(f"Erro ao configurar o banco de dados: {e}")
    finally:
        if conn:
            conn.close()


def register_pmu(pmu_id: int):
    """
    Garante que um PMU_ID exista na tabela de metadados 'pmu_devices'.
    Isso é crucial para satisfazer a restrição de Chave Estrangeira
    (FOREIGN KEY) antes de inserir dados na 'phasor_data'.
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.autocommit = True
        with conn.cursor() as cursor:
            # "ON CONFLICT (pmu_id) DO NOTHING" é um comando "upsert" seguro.
            # Se o ID já existir, ele não faz nada.
            cursor.execute("""
                INSERT INTO pmu_devices (pmu_id, station_name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (pmu_id) DO NOTHING;
            """, (pmu_id, f"PMU {pmu_id}", f"Auto-registrado em {datetime.now()}"))
            if cursor.rowcount > 0:
                logger.info(f"Novo dispositivo PMU ID {pmu_id} registrado na tabela 'pmu_devices'.")

    except Exception as e:
        logger.error(f"Erro ao registrar PMU ID {pmu_id}: {e}")
    finally:
        if conn:
            conn.close()