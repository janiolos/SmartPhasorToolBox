"""
PhasorToolBox Package
---------------------

Este arquivo __init__.py é modificado para expor as classes
principais (como Client, Pdc, PcapParser) no nível superior do pacote.

Isso permite que nosso Ingestor use:
    from phasortoolbox import Client
em vez de:
    from phasortoolbox.client import Client

*** CORREÇÃO DE IMPORTAÇÃO CIRCULAR ***
Os sub-módulos (como client.py) importam classes do "topo" (from phasortoolbox import X).
Portanto, este __init__.py deve carregar (expor) essas classes ANTES
de importar os sub-módulos que dependem delas.
"""

# --- PASSO 1: Expor as Classes de Dependência ---
# Estas são as "ferramentas" que os sub-módulos (client, pdc) precisam.
from .parser.parser import Parser
from .message import Message
from .parser.cfg_2 import Cfg2
from .parser.command import Command
from .parser.header import Header
from .devices import Pmu
from .parser.pcap.pcap import Pcap

# --- PASSO 2: Expor as Classes Principais (que usam as ferramentas) ---
# Agora que 'Parser', 'Message', etc., estão carregados,
# podemos importar com segurança os módulos que dependem deles.
from .client import Client
from .pdc import Pdc
from .synchrophasor import PcapParser


# Define o que é "público" ao usar 'from phasortoolbox import *'
__all__ = [
    'Client',
    'Pdc',
    'PcapParser',
    'Parser',
    'Message',
    'Cfg2',
    'Command',
    'Header',
    'Pmu',
    'Pcap'
]
