#!/bin/bash

set -e

echo "Criando pasta data..."
mkdir -p data

echo "Verificando arquivo .env..."

if [ ! -f .env ]; then
    cp .env.example .env
    echo ".env criado a partir do .env.example"
else
    echo ".env já existe. Mantendo arquivo atual."
fi

echo "Instalando uv..."

curl -LsSf https://astral.sh/uv/install.sh | sh

export PATH="$HOME/.local/bin:$PATH"

echo "Instalando dependências do projeto..."

uv sync

echo "Setup finalizado com sucesso!"