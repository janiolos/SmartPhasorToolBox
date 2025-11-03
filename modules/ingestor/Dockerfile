Este Dockerfile define como construir a imagem do contêiner para o Ingestor.

1. Começamos com uma imagem Python leve

FROM python:3.10-slim

2. Definimos o diretório de trabalho dentro do contêiner

WORKDIR /app

3. Copiamos o arquivo de dependências PRIMEIRO

(Isso otimiza o cache do Docker. Se requirements.txt não mudar,

o Docker reutiliza a camada de instalação, tornando builds futuros mais rápidos)

COPY requirements.txt .

4. Instalamos as dependências

RUN pip install --no-cache-dir -r requirements.txt

5. Copiamos o código-fonte do nosso módulo

COPY main.py .

6. Definimos o comando que será executado quando o contêiner iniciar

CMD ["python", "main.py"]