Este Dockerfile define como construir a imagem do contêiner para o Persistor.

1. Começamos com uma imagem Python leve

FROM python:3.10-slim

2. Instalamos dependências do sistema operacional

'postgresql-client' é necessário para o 'psycopg2' se conectar ao Postgres

RUN apt-get update && apt-get install -y

postgresql-client

&& rm -rf /var/lib/apt/lists/*

3. Definimos o diretório de trabalho

WORKDIR /app

4. Copiamos o arquivo de dependências e instalamos

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

5. Copiamos o código-fonte do nosso módulo (main.py e database.py)

COPY *.py .

6. Definimos o comando que será executado quando o contêiner iniciar

CMD ["python", "main.py"]