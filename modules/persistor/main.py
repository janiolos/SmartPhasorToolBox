"""
Módulo Persistor - MVP
=====================
Serviço Python independente (microsserviço) responsável por:

1. Conectar-se ao Redis.
2. Descobrir streams de PMUs (ex: "pmu_data_stream:1").
3. Criar/Juntar-se a um grupo de consumidores (Consumer Group) para cada stream.
4. Ler dados em lotes (batches) dos streams (Ref: ARQUITETURA.MD, Seção 3.3).
5. Mapear e formatar os dados para o esquema "largo" (wide) do banco.
6. Usar `psycopg2.extras.execute_values` (bulk insert) para
   persistir os dados em massa no TimescaleDB (Ref: ARQUITETURA.MD, Seção 5.3).
"""
import redis
import os
import time
import logging
import signal
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from database import get_db_connection, setup_database, register_pmu

# --- Configuração ---
load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (Persistor): %(message)s')
logger = logging.getLogger(__name__)

# --- Constantes ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
STREAM_KEY_PATTERN = "pmu_data_stream:*"  # Padrão para todos os streams de PMU
GROUP_NAME = "persistor_group"
CONSUMER_NAME = "persistor_consumer_1"
BATCH_SIZE = 500  # Quantidade de mensagens para inserir no DB de uma vez
BLOCK_TIMEOUT_MS = 2000  # Tempo que o Redis espera por novas mensagens (2s)

# --- Estado Global ---
running = True  # Flag para permitir um desligamento gracioso
db_conn = None
data_buffer = []  # Buffer para o bulk insert: [(row1), (row2), ...]
acks_to_send = {}  # Dicionário para ACKs: {stream_name: [id1, id2, ...]}

# Colunas da tabela 'phasor_data'
# IMPORTANTE: A ORDEM DEVE CORRESPONDER EXATAMENTE AO 'database.py'
TABLE_COLUMNS = [
    'time', 'pmu_id', 'stat_word', 'freq', 'rocof',
    'phasor_0_mag', 'phasor_0_ang_deg', 'phasor_1_mag', 'phasor_1_ang_deg',
    'phasor_2_mag', 'phasor_2_ang_deg', 'phasor_3_mag', 'phasor_3_ang_deg',
    'phasor_4_mag', 'phasor_4_ang_deg', 'phasor_5_mag', 'phasor_5_ang_deg',
    'phasor_6_mag', 'phasor_6_ang_deg', 'phasor_7_mag', 'phasor_7_ang_deg',
    'phasor_8_mag', 'phasor_8_ang_deg', 'phasor_9_mag', 'phasor_9_ang_deg',
    'phasor_10_mag', 'phasor_10_ang_deg', 'phasor_11_mag', 'phasor_11_ang_deg',
    'analog_0', 'analog_1', 'analog_2', 'analog_3',
    'analog_4', 'analog_5', 'analog_6', 'analog_7',
    'digital_0', 'digital_1'
]
# Cria o template da query SQL dinamicamente baseado nas colunas
SQL_INSERT_QUERY = f"INSERT INTO phasor_data ({', '.join(TABLE_COLUMNS)}) VALUES %s"


def format_row_from_redis(msg_data: dict) -> tuple:
    """
    Converte o dicionário (flat) vindo do Redis para uma tupla
    na ordem exata das colunas da tabela 'phasor_data'.

    Usar .get(key) retorna None se a chave não existir, o que
    é convertido para NULL pelo psycopg2, evitando quebras.
    """
    # 1. Converte o timestamp (float) para um objeto datetime
    #    que o TimescaleDB entende.
    try:
        msg_data['time'] = datetime.fromtimestamp(float(msg_data['ts']))
    except (TypeError, ValueError):
        msg_data['time'] = None  # Ignora se o timestamp estiver corrompido

    # 2. Mapeia as chaves do dicionário para a tupla
    row = tuple(msg_data.get(col) for col in TABLE_COLUMNS)
    return row


def flush_buffer_to_db():
    """
    Executa o bulk insert no TimescaleDB usando execute_values.
    (Ref: ARQUITETURA.MD, Seção 5.3)
    """
    global data_buffer, db_conn, acks_to_send
    if not data_buffer:
        return True  # Nada a fazer

    try:
        # Garante que a conexão esteja viva
        if db_conn is None or db_conn.closed:
            logger.warning("Conexão com DB perdida. Reconectando...")
            db_conn = get_db_connection()
            db_conn.autocommit = False  # Desliga autocommit para transações

        with db_conn.cursor() as cursor:
            # execute_values é a função chave para performance de escrita
            execute_values(cursor, SQL_INSERT_QUERY, data_buffer, page_size=BATCH_SIZE)

        db_conn.commit()  # Confirma a transação

        logger.info(f"Sucesso: {len(data_buffer)} registros inseridos no TimescaleDB.")
        data_buffer.clear()
        return True

    except Exception as e:
        logger.error(f"Erro no bulk insert: {e}")
        if db_conn:
            db_conn.rollback()  # Desfaz a transação em caso de erro
        # Não limpa o buffer, tentará novamente no próximo ciclo
        return False


def discover_and_setup_groups(r: redis.Redis) -> dict:
    """
    Procura por streams de PMU, cria o grupo de consumidores
    e registra a PMU no banco de dados.
    """
    logger.info("Procurando por streams de PMU...")
    streams_dict = {}
    try:
        stream_names = r.keys(STREAM_KEY_PATTERN)
        if not stream_names:
            logger.warning("Nenhum stream de PMU encontrado. Aguardando Ingestores...")
            return {}

        for stream_name in stream_names:
            try:
                # 1. Cria o grupo de consumidores no stream
                # mkstream=True garante que o stream não seja acessado antes de existir
                r.xgroup_create(stream_name, GROUP_NAME, id='$', mkstream=True)
                logger.info(f"Grupo '{GROUP_NAME}' criado para o stream '{stream_name}'.")
            except redis.exceptions.ResponseError as e:
                if "name already exists" in str(e):
                    logger.debug(f"Grupo '{GROUP_NAME}' já existe no stream '{stream_name}'.")
                else:
                    raise e

            # 2. Registra a PMU no banco de dados
            try:
                pmu_id = int(stream_name.split(':')[-1])
                register_pmu(pmu_id)
            except (IndexError, ValueError):
                logger.warning(f"Nome de stream inválido, não foi possível extrair PMU ID: {stream_name}")

            # 3. Adiciona ao dicionário para escuta
            # O '>' significa "leia apenas mensagens novas"
            streams_dict[stream_name] = '>'

        logger.info(f"Escutando {len(streams_dict)} stream(s).")
        return streams_dict

    except Exception as e:
        logger.error(f"Erro ao descobrir/configurar grupos de consumidores: {e}")
        return {}


def run_persistor():
    """ Função principal do persistor """
    global running, db_conn, data_buffer, acks_to_send

    logger.info("Iniciando Módulo Persistor...")

    # 1. Garante que as tabelas do DB existam antes de começar
    setup_database()

    # 2. Conecta aos serviços
    db_conn = get_db_connection()
    db_conn.autocommit = False  # Usaremos transações manuais
    r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)

    # 3. Descobre streams e cria grupos de consumidores
    streams_to_listen = discover_and_setup_groups(r)
    last_discovery_time = time.time()

    while running:
        try:
            # 4. A cada 30s, procura por novos Ingestores/Streams
            if time.time() - last_discovery_time > 30:
                streams_to_listen = discover_and_setup_groups(r)
                last_discovery_time = time.time()

            if not streams_to_listen:
                logger.info("Nenhum stream para escutar. Aguardando 10s...")
                time.sleep(10)
                continue

            # 5. Lê do(s) stream(s) usando o Consumer Group
            # (Ref: ARQUITETURA.MD, Seção 3.3)
            response = r.xreadgroup(
                GROUP_NAME,
                CONSUMER_NAME,
                streams_to_listen,
                count=BATCH_SIZE,
                block=BLOCK_TIMEOUT_MS
            )

            if not response:
                # Se não houver mensagens (timeout), salva o buffer residual
                logger.debug("Timeout, salvando buffer residual...")
                flush_buffer_to_db()
                continue

            # 6. Processa as mensagens recebidas
            acks_to_send = {}
            for stream_name, messages in response:
                for msg_id, msg_data in messages:
                    # Adiciona a tupla formatada ao buffer de escrita
                    row = format_row_from_redis(msg_data)
                    if row[0] is not None:  # Garante que o timestamp é válido
                        data_buffer.append(row)

                    # Prepara o ACK (confirmação) para o Redis
                    if stream_name not in acks_to_send:
                        acks_to_send[stream_name] = []
                    acks_to_send[stream_name].append(msg_id)

            # 7. Se o buffer atingiu o tamanho, salva no DB
            if len(data_buffer) >= BATCH_SIZE:
                if flush_buffer_to_db():
                    # Só envia o ACK se a escrita no DB foi bem-sucedida
                    for stream, ids in acks_to_send.items():
                        r.xack(stream, GROUP_NAME, *ids)
                else:
                    logger.error("Falha ao salvar no DB. Os ACKs não foram enviados. As mensagens serão reprocessadas.")

        except redis.exceptions.RedisError as e:
            logger.error(f"Erro de conexão com o Redis: {e}. Tentando novamente...")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Erro inesperado no loop: {e}", exc_info=True)
            time.sleep(1)

    # --- Loop de Desligamento ---
    logger.info("Desligando... salvando buffer final.")
    flush_buffer_to_db()
    if db_conn:
        db_conn.close()
    logger.info("Persistor encerrado.")


def shutdown_handler(signum, frame):
    """ Lida com sinais de desligamento (Ctrl+C, kill, docker stop) """
    global running
    if running:
        logger.warning("Recebido sinal de desligamento. Encerrando graciosamente...")
        running = False


if __name__ == "__main__":
    # Captura os sinais de desligamento para um encerramento limpo
    signal.signal(signal.SIGINT, shutdown_handler)  # Ctrl+C
    signal.signal(signal.SIGTERM, shutdown_handler)  # 'docker stop'

    run_persistor()