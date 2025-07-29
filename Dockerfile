FROM python:3.11-slim-bookworm

# Atualiza o sistema e instala dependências mínimas
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tzdata \
        libstdc++6 \
        libffi-dev \
        ca-certificates \
        && apt-get dist-upgrade -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Criação de usuário não-root seguro
RUN groupadd -r jupyter && useradd -r -g jupyter -m -d /home/jupyter jupyter

# Define variáveis de ambiente
ENV HOME=/home/jupyter

# Instala pacotes Python com versão fixada
RUN pip install --upgrade pip && \
    pip install --no-cache-dir \
        notebook==7.0.6 \
        pandas==2.1.4 \
        requests==2.31.0 \
        tqdm==4.66.1

# Cria diretório de trabalho com permissões
RUN mkdir -p /workspace && chown -R jupyter:jupyter /workspace
RUN mkdir -p $HOME/.jupyter && chown -R jupyter:jupyter $HOME/.jupyter

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--NotebookApp.token=", "--NotebookApp.password="]
