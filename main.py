"""
Módulo API (FastAPI) - Ponto de Entrada
=======================================
Este é o servidor web que fornece os endpoints HTTP para
monitorar e, futuramente, controlar a plataforma SMART PDC.
"""

from fastapi import FastAPI, HTTPException
from status_manager import StatusManager
import logging
from dotenv import load_dotenv

# Carrega variáveis de ambiente (para debug local)
load_dotenv()

# Configuração do logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] (API-Main): %(message)s')
logger = logging.getLogger(__name__)

# Cria a aplicação FastAPI
app = FastAPI(
    title="SMART PDC API",
    description="API para monitoramento e gerenciamento da plataforma de sincrofasores.",
    version="2.0.0 (MVP)"
)

# Instancia o gerenciador de status.
# O FastAPI criará uma única instância quando o app iniciar.
try:
    status_manager = StatusManager()
except Exception as e:
    logger.critical(f"Falha ao inicializar o StatusManager: {e}")
    # Se não puder conectar ao Redis, a API ainda sobe,
    # mas os endpoints de status falharão (o que é o esperado).
    status_manager = None


@app.on_event("startup")
async def startup_event():
    """ Evento de inicialização do FastAPI """
    logger.info("Servidor API (FastAPI) iniciando...")
    if status_manager is None or status_manager.r is None:
        logger.warning("API iniciada, mas StatusManager não pôde conectar ao Redis.")
    else:
        logger.info("API iniciada e conectada ao Redis para monitoramento.")


@app.get("/", tags=["Health"])
async def get_root():
    """
    Endpoint raiz para um health check simples.
    """
    return {"status": "SMART PDC API está online."}


@app.get("/status/health", tags=["Status"])
async def get_system_health():
    """
    Fornece um resumo da saúde de todos os componentes
    (Redis, Ingestores, Persistor).
    """
    if status_manager is None:
        raise HTTPException(status_code=503, detail="StatusManager não está conectado ao Redis.")

    health_report = status_manager.get_system_health()
    return health_report


@app.get("/status/ingestors", tags=["Status"])
async def get_ingestor_status():
    """
    Retorna o status detalhado de todos os Ingestores
    procurando por streams 'pmu_data_stream:*' no Redis.
    """
    if status_manager is None:
        raise HTTPException(status_code=503, detail="StatusManager não está conectado ao Redis.")

    ingestor_status = status_manager.get_all_ingestors_status()
    return ingestor_status


@app.get("/status/persistor", tags=["Status"])
async def get_persistor_status():
    """
    Retorna o status detalhado do Persistor, verificando o "lag"
    (mensagens pendentes) no grupo de consumidores 'persistor_group'.
    """
    if status_manager is None:
        raise HTTPException(status_code=503, detail="StatusManager não está conectado ao Redis.")

    persistor_status = status_manager.get_persistor_status()
    return persistor_status

# --- Futuros Endpoints (Fase 2) ---
# @app.post("/config/pmu/add", tags=["Configuração"])
# async def add_pmu(config: PmuConfigModel):
#     # Lógica para adicionar uma nova configuração de PMU
#     # (Exigiria orquestração do Docker, mais complexo)
#     pass
