# 💰 Faturamento Platform

Plataforma web para visualização geográfica do faturamento de ligações de água/esgoto, com upload de planilha Excel, mapa interativo e mapa de calor por valor faturado (R$).

---

## Pré-requisitos

- **Python 3.11+** instalado → [python.org](https://www.python.org/downloads/)
- **pip** disponível no terminal

---

## Instalação

### 1. Instalar as dependências

Abra o terminal na pasta raiz do projeto (`faturamento/`) e execute:

```bash
pip install -r requirements.txt
```

---

## Executar o servidor

### Opção A — Script automático (Windows)

Dê duplo clique no arquivo:

```
start.bat
```

### Opção B — Terminal manual

```bash
cd backend
python -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

Aguarde a mensagem:

```
INFO:     Uvicorn running on http://0.0.0.0:8001
```

---

## Acessar a plataforma

Abra o navegador e acesse:

```
http://localhost:8001
```

---

## Como usar

### 1. Importar a planilha Excel
- No painel esquerdo, clique na área **"Clique ou arraste"** e selecione seu arquivo `.xlsx` ou `.xls`
- Ou arraste o arquivo diretamente para a área de upload
- Aguarde a confirmação de importação

> Colunas esperadas: `NUM_LIGACAO`, `NOM_CLIENTE`, `CATEGORIA`, `COD_GRUPO`, `NUM_MEDIDOR`, `TIPO_FATURAMENTO`, `CIDADE`, `MACRO`, `MICRO`, `REFERENCIA`, `SIT_LIGACAO`, `COD_LATITUDE`, `COD_LONGITUDE`, `ISGRANDECONSUMIDOR`, `SUMVALOR`, `VALOR_D1`, `VALOR_D2`, `VALOR_IN1`, `VALOR_IN2`, `VALOR_A`, `QTD_ECO1`, `QTD_ECO2`, `VOL_FAT`

### 2. Visualizar o mapa
- Os pontos aparecem coloridos por tipo de faturamento
- Clique em qualquer ponto para ver detalhes, incluindo o **valor faturado (R$)**

### 3. Mapa de calor por faturamento
- Ative o toggle **"Mapa de calor (Faturamento R$)"** no painel lateral
- Configure cidade e valor mínimo em R$ para filtrar
- Cores: azul (baixo) → verde → amarelo → laranja → vermelho (alto faturamento)

### 4. Filtros
- Filtre por tipo de faturamento, cidade, macro região e grande consumidor (GC)
- Ajuste o limite máximo de pontos com o slider

---

## Diferença em relação ao Volume Platform

| Aspecto | Volume Platform | Faturamento Platform |
|---|---|---|
| Mapa de calor | Volume faturado (m³) | Valor faturado (R$) |
| Tabela por tipo | Vol. Total (m³) | Fat. Total (R$) |
| Threshold heatmap | 100 m³ | R$ 500 |
| Filtro mínimo | Vol. mín. (m³) | Valor mín. (R$) |
| Banco de dados | volume.db | faturamento.db |
| Porta padrão | 8000 | 8001 |

---

## Estrutura do projeto

```
faturamento/
├── backend/
│   ├── main.py
│   ├── database.py
│   └── models.py
├── frontend/
│   └── templates/
│       └── index.html
├── requirements.txt
├── start.bat
└── faturamento.db  ← criado automaticamente
```

---

## API Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/` | Interface web principal |
| `POST` | `/api/upload` | Upload e importação da planilha Excel |
| `GET` | `/api/pontos` | Lista de pontos com coordenadas |
| `GET` | `/api/heatmap` | Dados para o mapa de calor (por R$) |
| `GET` | `/api/filtros` | Valores únicos para dropdowns |
| `GET` | `/api/stats` | Estatísticas gerais com faturamento total por tipo |

Documentação interativa: `http://localhost:8001/docs`
