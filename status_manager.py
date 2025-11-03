"""
Módulo Gerenciador de Status
============================
Este módulo substitui a lógica do antigo 'process_manager.py'.

Em uma arquitetura de microsserviços orquestrada pelo Docker, a API
não deve ser responsável por INICIAR ou PARAR processos (isso é
trabalho do Docker Compose ou Kubernetes).

A função da API é MONITORAR a saúde do sistema, inspecionando o
barramento de dados (Redis) para ver se os outros serviços
estão vivos e processando dados.
"""

import redis
import os
import logging
from datetime import datetime

# Configuração do logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (API-Status): %(message)s')
logger = logging.getLogger(__name__)

# --- Constantes ---
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
STREAM_KEY_PATTERN = "pmu_data_stream:*"  # Padrão para streams de PMU
PERSISTOR_GROUP_NAME = "persistor_group"


class StatusManager:
    """
    Classe que centraliza a lógica de verificação de status
    dos microsserviços, lendo o estado do Redis.
    """

    def __init__(self):
        try:
            # Nota: Usamos a biblioteca 'redis' síncrona aqui
            # porque o FastAPI gerencia as threads de requisição.
            self.r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
            self.r.ping()
            logger.info(f"Conectado ao Redis em {REDIS_HOST} para monitoramento.")
        except Exception as e:
            logger.error(f"Falha ao conectar ao Redis para monitoramento: {e}")
            self.r = None

    def get_system_health(self) -> dict:
        """
        Verifica a saúde geral dos componentes (Redis e DB).
        """
        if not self.r:
            return {"redis_status": "DOWN", "persistor_status": "UNKNOWN"}

        # 1. Saúde do Redis
        redis_status = "UP"

        # 2. Saúde do Persistor
        persistor_status = self.get_persistor_status()

        # 3. Saúde dos Ingestores
        ingestor_status = self.get_all_ingestors_status()

        return {
            "redis_status": redis_status,
            "persistor_status": persistor_status,
            "ingestor_status": ingestor_status
        }

    def get_all_ingestors_status(self) -> dict:
        """
        Verifica o status de todos os ingestores.
        Um ingestor é considerado "vivo" se ele publicou dados
        recentemente (verificando o último ID do stream).
        """
        if not self.r:
            return {}

        status = {}
        try:
            stream_names = self.r.keys(STREAM_KEY_PATTERN)
            if not stream_names:
                return {"status": "Nenhum ingestor encontrado."}

            for stream in stream_names:
                try:
                    pmu_id = stream.split(':')[-1]
                    # Pega a última entrada do stream
                    last_entry = self.r.xrevrange(stream, count=1)

                    if not last_entry:
                        status[pmu_id] = {"status": "Stream Vazio", "last_seen": None}
                        continue

                    msg_id, msg_data = last_entry[0]
                    # O ID do stream é 'timestamp_ms-seq', ex: '1678886400123-0'
                    last_timestamp_ms = int(msg_id.split('-')[0])
                    last_seen = datetime.fromtimestamp(last_timestamp_ms / 1000.0)

                    # Verifica se a última mensagem é recente (ex: últimos 60s)
                    age_seconds = (datetime.now() - last_seen).total_seconds()

                    if age_seconds < 60:
                        status[pmu_id] = {"status": "ONLINE", "last_seen": last_seen.isoformat()}
                    else:
                        status[pmu_id] = {"status": "OFFLINE", "last_seen": last_seen.isoformat()}

                except Exception as e:
                    logger.warning(f"Erro ao verificar status do stream {stream}: {e}")
                    status[stream] = {"status": "ERRO", "details": str(e)}

            return status

        except Exception as e:
            logger.error(f"Erro ao listar streams: {e}")
            return {"status": "ERRO", "details": str(e)}

    def get_persistor_status(self) -> dict:
        """
        Verifica o status do Persistor medindo o "lag" no
        grupo de consumidores.

        Lag = Quantas mensagens o Ingestor publicou que o
        Persistor ainda não processou (confirmou com XACK).
        """
        if not self.r:
            return {}

        status = {}
        try:
            stream_names = self.r.keys(STREAM_KEY_PATTERN)
            if not stream_names:
                return {"status": "Nenhum stream para monitorar."}

            for stream in stream_names:
                try:
                    # XINFO GROUPS nos dá informações sobre os consumer groups
                    groups_info = self.r.xinfo_groups(stream)
                    found = False
                    for group in groups_info:
                        if group['name'] == PERSISTOR_GROUP_NAME:
                            found = True
                            status[stream] = {
                                "status": "MONITORADO",
                                "lag": group['lag'],  # <-- A métrica mais importante!
                                "pending_messages": group['pending'],
                                "consumers": group['consumers']
                            }
                            break
                    if not found:
                        status[stream] = {"status": "AGUARDANDO"}

                except redis.exceptions.ResponseError:
                    # Acontece se o stream foi criado mas o grupo ainda não
                    status[stream] = {"status": "SEM GRUPO"}
                except Exception as e:
                    status[stream] = {"status": "ERRO", "details": str(e)}

            return status

        except Exception as e:
            logger.error(f"Erro ao verificar grupos: {e}")
            return {"status": "ERRO", "details": str(e)}