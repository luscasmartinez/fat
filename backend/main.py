import os
import io
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Query, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd

from database import engine, get_db, Base
from models import Faturamento, Volume, MetaFaturamento

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Faturamento Platform API", version="1.0.0")

# Rastreamento simples de progresso de upload em memória
# {task_id: {"status": "...", "pct": 0-100, "msg": "..."}}
_upload_progress: dict = {}

from fastapi import Request
from fastapi.responses import JSONResponse
import traceback as tb

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    trace = tb.format_exc()
    print("=== ERRO NÃO TRATADO ===")
    print(trace)
    return JSONResponse(status_code=500, content={"detail": f"{str(exc)}\n{trace}"})

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend
FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend")
STATIC_DIR = os.path.join(FRONTEND_DIR, "static")
TEMPLATES_DIR = os.path.join(FRONTEND_DIR, "templates")

if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ── COLUMN MAPS ─────────────────────────────────────────────────────────────
# Faturamento Líquido
# Suporta três variantes de prefixo geradas pelo Power BI / pivot:
#   TB_FATURAMENTO_LIQUIDO_2CAMPO  (underscore entre liquido e 2)
#   TB_FATURAMENTO_LIQUIDO2CAMPO   (sem underscore — brackets [2] removidos)
#   CAMPO                          (sem prefixo)
COLUMN_MAP = {
    # ── sem prefixo ──────────────────────────────────────────
    "ORIGEM": "origem",
    "NUM_LIGACAO": "num_ligacao",
    "CIDADE": "cidade",
    "MACRO": "macro",
    "MICRO": "micro",
    "COD_CLASSE_RUBRICA": "cod_classe_rubrica",
    "CLASSE_RUBRICA": "classe_rubrica",
    "COD_GRUPO_RUBRICA": "cod_grupo_rubrica",
    "GRUPO_RUBRICA": "grupo_rubrica",
    "COD_RUBRICA": "cod_rubrica",
    "DESCRICAO_RUBRICA": "descricao_rubrica",
    "NUM_NOTA": "num_nota",
    "NUM_NOTA_FISCAL": "num_nota_fiscal",
    "NUM_NOTA_ORIGINAL": "num_nota_original",
    "COD_GRUPO": "cod_grupo",
    "MOTIVO_CANCELAMENTO": "motivo_cancelamento",
    # ── prefixo TB_FATURAMENTO_LIQUIDO_2 (underscore) ────────
    "TB_FATURAMENTO_LIQUIDO_2ORIGEM": "origem",
    "TB_FATURAMENTO_LIQUIDO_2NUM_LIGACAO": "num_ligacao",
    "TB_FATURAMENTO_LIQUIDO_2CIDADE": "cidade",
    "TB_FATURAMENTO_LIQUIDO_2COD_CLASSE_RUBRICA": "cod_classe_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2CLASSE_RUBRICA": "classe_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2COD_GRUPO_RUBRICA": "cod_grupo_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2GRUPO_RUBRICA": "grupo_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2COD_RUBRICA": "cod_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2DESCRICAO_RUBRICA": "descricao_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2NUM_NOTA": "num_nota",
    "TB_FATURAMENTO_LIQUIDO_2NUM_NOTA_FISCAL": "num_nota_fiscal",
    "TB_FATURAMENTO_LIQUIDO_2NUM_NOTA_ORIGINAL": "num_nota_original",
    "TB_FATURAMENTO_LIQUIDO_2COD_GRUPO": "cod_grupo",
    "TB_FATURAMENTO_LIQUIDO_2MOTIVO_CANCELAMENTO": "motivo_cancelamento",
    # ── prefixo TB_FATURAMENTO_LIQUIDO2 (sem underscore — [2] direto) ────────
    "TB_FATURAMENTO_LIQUIDO2ORIGEM": "origem",
    "TB_FATURAMENTO_LIQUIDO2NUM_LIGACAO": "num_ligacao",
    "TB_FATURAMENTO_LIQUIDO2CIDADE": "cidade",
    "TB_FATURAMENTO_LIQUIDO2COD_CLASSE_RUBRICA": "cod_classe_rubrica",
    "TB_FATURAMENTO_LIQUIDO2CLASSE_RUBRICA": "classe_rubrica",
    "TB_FATURAMENTO_LIQUIDO2COD_GRUPO_RUBRICA": "cod_grupo_rubrica",
    "TB_FATURAMENTO_LIQUIDO2GRUPO_RUBRICA": "grupo_rubrica",
    "TB_FATURAMENTO_LIQUIDO2COD_RUBRICA": "cod_rubrica",
    "TB_FATURAMENTO_LIQUIDO2DESCRICAO_RUBRICA": "descricao_rubrica",
    "TB_FATURAMENTO_LIQUIDO2NUM_NOTA": "num_nota",
    "TB_FATURAMENTO_LIQUIDO2NUM_NOTA_FISCAL": "num_nota_fiscal",
    "TB_FATURAMENTO_LIQUIDO2NUM_NOTA_ORIGINAL": "num_nota_original",
    "TB_FATURAMENTO_LIQUIDO2COD_GRUPO": "cod_grupo",
    "TB_FATURAMENTO_LIQUIDO2MOTIVO_CANCELAMENTO": "motivo_cancelamento",
    # ── valor / totalizador ───────────────────────────────────
    "ISGRANDTOTALROWTOTAL": "is_grand_total",
    "SUMVALOR": "sum_valor",
    "SUM_VALOR": "sum_valor",
    "SUMVALUE": "sum_valor",
}

# ── Meta de Faturamento ────────────────────────────────────────────────────
META_COLUMN_MAP = {
    "CIDADE":     "cidade",
    "DIRETORIA":  "diretoria",
    "MACRO":      "macro",
    "MICRO":      "micro",
    "COD_GRUPO":  "cod_grupo",
    "AGRUPADOR":  "agrupador",
    "VALOR":      "valor",
    # variantes com prefixo de tabela Power BI
    "META_FATURAMENTOCIDADE":    "cidade",
    "META_FATURAMENTODIRETORIA": "diretoria",
    "META_FATURAMENTOMACRO":     "macro",
    "META_FATURAMENTOMICRO":     "micro",
    "META_FATURAMENTOCOD_GRUPO": "cod_grupo",
    "META_FATURAMENTOAGRUPADOR": "agrupador",
    "META_FATURAMENTOVALOR":     "valor",
    # totalizadores
    "SUMVALOR":  "valor",
    "SUM_VALOR": "valor",
    "SUMVALUE":  "valor",
}

# Volume Operacional
VOLUME_COLUMN_MAP = {
    "REGIONAL_AEGEA":                         "regional_aegea",
    "MUNICIPIO":                              "municipio",
    "FAIXA":                                  "faixa",
    "FAIXA_ORDEM":                            "faixa_ordem",
    "REFERENCIA":                             "referencia",
    "MATRICULA":                              "matricula",
    "ISGRANDTOTALROWTOTAL":                   "is_grand_total",
    "SUMTOTAL_VOLUME_ESTIMADO_OPERACIONAL":   "total_volume_estimado_operacional",
    "SUMTOTAL_CONS_MEDIDO":                   "total_cons_medido",
    "SUMTOTAL_VOLUME_ESTIMADO":               "total_volume_estimado",
    "SUMTOTAL_VOLUME_ESTIMADO_REGRAFAT":      "total_volume_estimado_regrafat",
    "VOLUME_TOTAL":                           "volume_total",
    "SUMTOTAL_CON_FATURADO_A":               "total_con_faturado_a",
}


def normalize_col(name: str) -> str:
    import unicodedata
    normalized = unicodedata.normalize("NFD", str(name))
    normalized = "".join(c for c in normalized if unicodedata.category(c) != "Mn")
    return (
        normalized.upper().strip()
        .replace(" ", "_").replace("(", "").replace(")", "")
        .replace("-", "_").replace("[", "").replace("]", "")
    )


@app.get("/", response_class=HTMLResponse)
async def root():
    index_path = os.path.join(TEMPLATES_DIR, "index.html")
    with open(index_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


def _safe_val(val):
    """Sanitiza valores do DataFrame antes de inserir no banco."""
    import datetime, math
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(val, 'item'):
        try:
            val = val.item()
        except Exception:
            pass
    if isinstance(val, (pd.Timestamp, datetime.datetime, datetime.date)):
        return str(val)
    if isinstance(val, float) and (math.isinf(val) or math.isnan(val)):
        return None
    return val


def _process_df(df, col_map, model_class, numeric_fields, db):
    """
    Normaliza colunas do DataFrame, mapeia para o modelo e insere no banco.
    Usa bulk_insert_mappings para performance em arquivos grandes (600k+ linhas).
    Retorna dict com resumo da operação.
    """
    df = df.copy()
    df.columns = [normalize_col(c) for c in df.columns]
    normalized_cols = list(df.columns)

    rename_map, already_mapped = {}, set()
    for col in normalized_cols:
        target = col_map.get(col)
        if target and target not in already_mapped:
            rename_map[col] = target
            already_mapped.add(target)
    df.rename(columns=rename_map, inplace=True)

    for field in numeric_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    if 'is_grand_total' in df.columns:
        df['is_grand_total'] = df['is_grand_total'].map(
            lambda v: False if str(v).strip().lower() == 'false'
                      else (True if str(v).strip().lower() == 'true' else None)
        )

    model_fields = list({c.name for c in model_class.__table__.columns} - {"id"})

    # Seleciona apenas colunas que existem no modelo e no DataFrame
    cols_present = [f for f in model_fields if f in df.columns]
    cols_missing = [f for f in model_fields if f not in df.columns]
    df_model = df[cols_present].copy()

    # Acrescenta colunas ausentes como None (sem precisar de iterrows)
    for col in cols_missing:
        df_model[col] = None

    # Sanitiza tipos não serializáveis vetorialmente
    import math, datetime as _dt
    for col in df_model.columns:
        dtype = df_model[col].dtype
        if str(dtype).startswith('float'):
            df_model[col] = df_model[col].where(
                df_model[col].apply(lambda x: x is None or (not math.isnan(x) and not math.isinf(x)) if isinstance(x, float) else True),
                other=None
            )
        elif str(dtype) == 'object':
            df_model[col] = df_model[col].where(df_model[col].notna(), other=None)
        # Converte Timestamp → string
        if 'datetime' in str(dtype):
            df_model[col] = df_model[col].astype(str).where(df_model[col].notna(), other=None)

    # Converte para lista de dicts e insere em lote
    records = df_model.to_dict(orient='records')

    BATCH_SIZE = 10_000
    for i in range(0, len(records), BATCH_SIZE):
        db.bulk_insert_mappings(model_class, records[i:i + BATCH_SIZE])
    db.commit()

    unmapped = [c for c in normalized_cols
                if c not in rename_map and c not in model_fields]
    return {
        "total": len(records),
        "colunas_mapeadas": list(rename_map.keys()),
        "colunas_nao_mapeadas": unmapped,
    }


@app.get("/api/upload/progress/{task_id}")
def get_upload_progress(task_id: str):
    """Polling endpoint para progresso de upload/processamento."""
    info = _upload_progress.get(task_id)
    if not info:
        raise HTTPException(status_code=404, detail="Task não encontrada")
    return info


@app.post("/api/upload")
async def upload_excel(
    file: UploadFile = File(...),
    tipo_tabela: str = Form("faturamento"),
    task_id: str = Form(""),
    db: Session = Depends(get_db),
):
    """
    Importa planilha Excel.
    - tipo_tabela = "faturamento"  → tb_faturamento_liquido
    - tipo_tabela = "volume"       → tabela de volume operacional
    - task_id = string opcional para polling de progresso via /api/upload/progress/{task_id}
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Arquivo deve ser .xlsx ou .xls")

    def _progress(pct: int, msg: str):
        if task_id:
            _upload_progress[task_id] = {"status": "processing", "pct": pct, "msg": msg}

    _progress(5, "Lendo arquivo Excel...")
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), dtype=object)
    except Exception as e:
        if task_id:
            _upload_progress[task_id] = {"status": "error", "pct": 0, "msg": str(e)}
        raise HTTPException(status_code=400, detail=f"Erro ao ler Excel: {str(e)}")

    total_rows = len(df)
    _progress(20, f"Arquivo lido: {total_rows:,} linhas. Limpando banco...")

    try:
        if tipo_tabela == "volume":
            db.query(Volume).delete()
            db.commit()
            _progress(35, "Normalizando colunas de Volume...")
            numeric = [
                'faixa_ordem',
                'total_volume_estimado_operacional', 'total_cons_medido',
                'total_volume_estimado', 'total_volume_estimado_regrafat',
                'volume_total', 'total_con_faturado_a',
            ]
            result = _process_df(df, VOLUME_COLUMN_MAP, Volume, numeric, db)
            label = "Volume Operacional"
        elif tipo_tabela == "meta":
            db.query(MetaFaturamento).delete()
            db.commit()
            _progress(35, "Normalizando colunas de Meta de Faturamento...")

            # ── Detecta formato wide (pivot) vs long ──────────────────────────
            # Wide: agrupadores como colunas (ex: "DIRETAS ÁGUA", "DIRETAS ESGOTO"…)
            # Long: colunas CIDADE, AGRUPADOR, VALOR em linhas separadas
            _df_norm_cols = [normalize_col(c) for c in df.columns]
            _has_long = any(c in META_COLUMN_MAP for c in _df_norm_cols)

            if not _has_long:
                # ── Formato wide → melt ───────────────────────────────────────
                _EXCL_NORM = {
                    'TOTAL_GERAL', 'GRANDTOTAL', 'GRAND_TOTAL', 'TOTAL',
                    'TOTALGERAL', 'TOTAL_GENERAL',
                }
                _orig_cols   = list(df.columns)
                _city_col    = _orig_cols[0]          # primeira coluna = cidade
                _agrup_cols  = [
                    c for n, c in zip(_df_norm_cols, _orig_cols)
                    if n not in _EXCL_NORM and c != _city_col
                ]

                _df_wide = df[[_city_col] + _agrup_cols].copy()
                _df_wide.rename(columns={_city_col: 'cidade'}, inplace=True)

                # Remove linha de Total Geral e células vazias
                _df_wide = _df_wide[
                    _df_wide['cidade'].notna() &
                    (~_df_wide['cidade'].astype(str).str.strip().str.upper().isin(
                        ['TOTAL GERAL', 'TOTAL', 'GRAND TOTAL', '']
                    ))
                ]

                _df_long = _df_wide.melt(
                    id_vars=['cidade'],
                    value_vars=_agrup_cols,
                    var_name='agrupador',
                    value_name='valor',
                )
                _df_long['valor']    = pd.to_numeric(_df_long['valor'], errors='coerce')
                _df_long['cidade']   = _df_long['cidade'].astype(str).str.strip()
                _df_long['agrupador'] = _df_long['agrupador'].astype(str).str.strip()
                # Remove linhas sem valor ou com valor zero
                _df_long = _df_long[_df_long['valor'].notna() & (_df_long['valor'] != 0)]

                _records = _df_long[['cidade', 'agrupador', 'valor']].to_dict(orient='records')
                _BATCH = 10_000
                for _i in range(0, len(_records), _BATCH):
                    db.bulk_insert_mappings(MetaFaturamento, _records[_i:_i + _BATCH])
                db.commit()
                result = {
                    "total": len(_records),
                    "colunas_mapeadas": ['cidade', 'agrupador', 'valor'],
                    "colunas_nao_mapeadas": [],
                }
            else:
                # ── Formato long (padrão) ─────────────────────────────────────
                numeric = ['cod_grupo', 'valor']
                result = _process_df(df, META_COLUMN_MAP, MetaFaturamento, numeric, db)

            label = "Meta de Faturamento"
        else:
            db.query(Faturamento).delete()
            db.commit()
            _progress(35, "Normalizando colunas de Faturamento...")
            numeric = ['cod_classe_rubrica', 'cod_grupo_rubrica', 'cod_rubrica', 'cod_grupo', 'sum_valor']
            result = _process_df(df, COLUMN_MAP, Faturamento, numeric, db)
            label = "Faturamento Líquido"
    except Exception as e:
        db.rollback()
        if task_id:
            _upload_progress[task_id] = {"status": "error", "pct": 0, "msg": str(e)}
        import traceback
        raise HTTPException(status_code=500, detail=f"Erro ao processar dados: {str(e)} | {traceback.format_exc()}")

    if task_id:
        _upload_progress[task_id] = {
            "status": "done", "pct": 100,
            "msg": f"{result['total']:,} registros de {label} importados.",
        }

    return {
        "message": f"{result['total']} registros de {label} importados com sucesso.",
        "tabela": tipo_tabela,
        **result,
    }


@app.post("/api/diagnostico")
async def diagnostico_excel(file: UploadFile = File(...)):
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), nrows=2)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler Excel: {str(e)}")
    original = list(df.columns)
    normalized = [normalize_col(c) for c in original]
    mapeadas = {n: COLUMN_MAP[n] for n in normalized if n in COLUMN_MAP}
    nao_mapeadas = [n for n in normalized if n not in COLUMN_MAP]
    return {
        "colunas_originais": original,
        "colunas_normalizadas": normalized,
        "mapeadas": mapeadas,
        "nao_mapeadas": nao_mapeadas,
    }


@app.get("/api/faturamento")
def get_faturamento(
    db: Session = Depends(get_db),
    cidade: Optional[str] = Query(None),
    macro: Optional[str] = Query(None),
    micro: Optional[str] = Query(None),
    origem: Optional[str] = Query(None),
    cod_rubrica: Optional[int] = Query(None),
    grupo_rubrica: Optional[str] = Query(None),
    classe_rubrica: Optional[str] = Query(None),
    limit: int = Query(10000, le=100000),
    offset: int = Query(0),
):
    query = db.query(Faturamento)
    if cidade:
        query = query.filter(Faturamento.cidade == cidade)
    if macro:
        query = query.filter(Faturamento.macro == macro)
    if micro:
        query = query.filter(Faturamento.micro == micro)
    if origem:
        query = query.filter(Faturamento.origem == origem)
    if cod_rubrica is not None:
        query = query.filter(Faturamento.cod_rubrica == cod_rubrica)
    if grupo_rubrica:
        query = query.filter(Faturamento.grupo_rubrica == grupo_rubrica)
    if classe_rubrica:
        query = query.filter(Faturamento.classe_rubrica == classe_rubrica)

    total = query.count()
    rows = query.offset(offset).limit(limit).all()

    return {
        "total": total,
        "data": [
            {
                "id": r.id,
                "origem": r.origem,
                "num_ligacao": r.num_ligacao,
                "cidade": r.cidade,
                "macro": r.macro,
                "micro": r.micro,
                "cod_classe_rubrica": r.cod_classe_rubrica,
                "classe_rubrica": r.classe_rubrica,
                "cod_grupo_rubrica": r.cod_grupo_rubrica,
                "grupo_rubrica": r.grupo_rubrica,
                "cod_rubrica": r.cod_rubrica,
                "descricao_rubrica": r.descricao_rubrica,
                "num_nota": r.num_nota,
                "num_nota_fiscal": r.num_nota_fiscal,
                "num_nota_original": r.num_nota_original,
                "cod_grupo": r.cod_grupo,
                "motivo_cancelamento": r.motivo_cancelamento,
                "is_grand_total": r.is_grand_total,
                "sum_valor": r.sum_valor,
            }
            for r in rows
        ],
    }


@app.get("/api/rubricas")
def get_rubricas(db: Session = Depends(get_db)):
    from sqlalchemy import func as sqlfunc
    rows = (
        db.query(
            Faturamento.cod_classe_rubrica,
            Faturamento.classe_rubrica,
            Faturamento.cod_grupo_rubrica,
            Faturamento.grupo_rubrica,
            Faturamento.cod_rubrica,
            Faturamento.descricao_rubrica,
            sqlfunc.sum(Faturamento.sum_valor).label("total_valor"),
            sqlfunc.count(Faturamento.id).label("qtd"),
        )
        .group_by(
            Faturamento.cod_classe_rubrica,
            Faturamento.classe_rubrica,
            Faturamento.cod_grupo_rubrica,
            Faturamento.grupo_rubrica,
            Faturamento.cod_rubrica,
            Faturamento.descricao_rubrica,
        )
        .order_by(Faturamento.cod_classe_rubrica, Faturamento.cod_grupo_rubrica, Faturamento.cod_rubrica)
        .all()
    )
    return [
        {
            "cod_classe_rubrica": r.cod_classe_rubrica,
            "classe_rubrica": r.classe_rubrica,
            "cod_grupo_rubrica": r.cod_grupo_rubrica,
            "grupo_rubrica": r.grupo_rubrica,
            "cod_rubrica": r.cod_rubrica,
            "descricao_rubrica": r.descricao_rubrica,
            "total_valor": r.total_valor,
            "qtd": r.qtd,
        }
        for r in rows
    ]


@app.get("/api/filtros")
def get_filtros(db: Session = Depends(get_db)):
    cidades         = [r[0] for r in db.query(Faturamento.cidade).distinct().order_by(Faturamento.cidade).all() if r[0]]
    macros          = [r[0] for r in db.query(Faturamento.macro).distinct().order_by(Faturamento.macro).all() if r[0]]
    micros          = [r[0] for r in db.query(Faturamento.micro).distinct().order_by(Faturamento.micro).all() if r[0]]
    origens         = [r[0] for r in db.query(Faturamento.origem).distinct().all() if r[0]]
    tipos_fat       = [r[0] for r in db.query(Faturamento.classe_rubrica).distinct()
                       .order_by(Faturamento.classe_rubrica).all() if r[0]]
    classes_rubrica = tipos_fat  # alias
    grupos_rubrica  = [r[0] for r in db.query(Faturamento.grupo_rubrica).distinct()
                       .order_by(Faturamento.grupo_rubrica).all() if r[0]]
    cod_grupos      = sorted([r[0] for r in db.query(Faturamento.cod_grupo).distinct().all()
                              if r[0] is not None])
    regionais   = [r[0] for r in db.query(Volume.regional_aegea).distinct()
                   .order_by(Volume.regional_aegea).all() if r[0]]
    municipios  = [r[0] for r in db.query(Volume.municipio).distinct()
                   .order_by(Volume.municipio).all() if r[0]]
    referencias = [r[0] for r in db.query(Volume.referencia).distinct()
                   .order_by(Volume.referencia.desc()).all() if r[0]]
    agrupadores = [r[0] for r in db.query(MetaFaturamento.agrupador).distinct()
                   .order_by(MetaFaturamento.agrupador).all() if r[0]]
    return {
        "cidades": cidades,
        "macros": macros,
        "micros": micros,
        "origens": origens,
        "tipos_faturamento": tipos_fat,
        "classes_rubrica": classes_rubrica,
        "grupos_rubrica": grupos_rubrica,
        "cod_grupos": cod_grupos,
        "regionais": regionais,
        "municipios": municipios,
        "referencias": referencias,
        "agrupadores": agrupadores,
    }


@app.get("/api/stats")
def get_stats(db: Session = Depends(get_db)):
    from sqlalchemy import func as sqlfunc

    # ── Faturamento ──
    total_fat = db.query(Faturamento).filter(
        (Faturamento.is_grand_total == False) | (Faturamento.is_grand_total == None)
    ).count()
    ligacoes_distintas = db.query(sqlfunc.count(sqlfunc.distinct(Faturamento.num_ligacao))).scalar() or 0
    result_tipo = (
        db.query(
            Faturamento.classe_rubrica,
            sqlfunc.count(Faturamento.id).label("qtd"),
            sqlfunc.sum(Faturamento.sum_valor).label("total_fat"),
        )
        .filter((Faturamento.is_grand_total == False) | (Faturamento.is_grand_total == None))
        .group_by(Faturamento.classe_rubrica)
        .order_by(sqlfunc.sum(Faturamento.sum_valor).desc())
        .all()
    )
    by_tipo = [{"tipo": r.classe_rubrica or "N/A", "qtd": r.qtd, "total_fat": r.total_fat or 0}
               for r in result_tipo]

    # ── Volume ──
    total_vol = db.query(Volume).filter(
        (Volume.is_grand_total == False) | (Volume.is_grand_total == None)
    ).count()

    return {
        "total": total_fat,
        "com_coords": ligacoes_distintas,  # ligações distintas (proxy enquanto sem geocode)
        "ligacoes_distintas": ligacoes_distintas,
        "by_tipo": by_tipo,
        "total_volume_registros": total_vol,
    }


# ── STUB: /api/pontos ─────────────────────────────────────────────────────────
# Retorna lista vazia até que coordenadas (lat/lng) sejam adicionadas ao modelo.
@app.get("/api/pontos")
def get_pontos(
    db: Session = Depends(get_db),
    tipo_faturamento: Optional[str] = Query(None),
    cidade: Optional[str] = Query(None),
    macro: Optional[str] = Query(None),
    gc: Optional[str] = Query(None),
    limit: int = Query(30000, le=100000),
):
    return {
        "total": 0,
        "data": [],
        "aviso": "Mapa de pontos requer campos lat/lng. Importe uma planilha com coordenadas geográficas.",
    }


# ── STUB: /api/heatmap ────────────────────────────────────────────────────────
@app.get("/api/heatmap")
def get_heatmap(
    db: Session = Depends(get_db),
    cidade: Optional[str] = Query(None),
    val_min: float = Query(0),
):
    return []


# ── /api/volume ───────────────────────────────────────────────────────────────
@app.get("/api/volume")
def get_volume(
    db: Session = Depends(get_db),
    regional: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    referencia: Optional[str] = Query(None),
    matricula: Optional[str] = Query(None),
    limit: int = Query(10000, le=100000),
    offset: int = Query(0),
):
    query = db.query(Volume).filter(
        (Volume.is_grand_total == False) | (Volume.is_grand_total == None)
    )
    if regional:
        query = query.filter(Volume.regional_aegea == regional)
    if municipio:
        query = query.filter(Volume.municipio == municipio)
    if referencia:
        query = query.filter(Volume.referencia == referencia)
    if matricula:
        query = query.filter(Volume.matricula == matricula)

    total = query.count()
    rows  = query.offset(offset).limit(limit).all()

    return {
        "total": total,
        "data": [
            {
                "id":                                   r.id,
                "regional_aegea":                       r.regional_aegea,
                "municipio":                            r.municipio,
                "faixa":                                r.faixa,
                "faixa_ordem":                          r.faixa_ordem,
                "referencia":                           r.referencia,
                "matricula":                            r.matricula,
                "total_volume_estimado_operacional":    r.total_volume_estimado_operacional,
                "total_cons_medido":                    r.total_cons_medido,
                "total_volume_estimado":                r.total_volume_estimado,
                "total_volume_estimado_regrafat":       r.total_volume_estimado_regrafat,
                "volume_total":                         r.volume_total,
                "total_con_faturado_a":                 r.total_con_faturado_a,
            }
            for r in rows
        ],
    }


# ── /api/kpis ─────────────────────────────────────────────────────────────────
# Indicadores operacionais calculados sobre as duas tabelas.
# Estratégia anti-duplicidade:
#   - Faturamento: agrega por num_ligacao ANTES do join (CTE)
#   - Volume: filtra is_grand_total = FALSE para evitar totalizadores
@app.get("/api/kpis")
def get_kpis(
    db: Session = Depends(get_db),
    referencia: Optional[str] = Query(None),
    municipio: Optional[str] = Query(None),
    regional: Optional[str] = Query(None),
):
    from sqlalchemy import func as sqlfunc

    # ── KPIs de Faturamento ──────────────────────────────────────────────────
    fat_q = db.query(
        sqlfunc.sum(Faturamento.sum_valor).label("receita_total"),
        sqlfunc.count(sqlfunc.distinct(Faturamento.num_ligacao)).label("ligacoes"),
    ).filter((Faturamento.is_grand_total == False) | (Faturamento.is_grand_total == None))

    if municipio:
        fat_q = fat_q.filter(Faturamento.cidade == municipio)
    if regional:
        fat_q = fat_q.filter(Faturamento.macro == regional)

    fat_r = fat_q.one()
    receita_total = fat_r.receita_total or 0.0
    ligacoes      = fat_r.ligacoes or 0

    # ── KPIs de Volume ───────────────────────────────────────────────────────
    vol_q = db.query(
        sqlfunc.sum(Volume.volume_total).label("volume_total"),
        sqlfunc.sum(Volume.total_cons_medido).label("cons_medido"),
        sqlfunc.sum(Volume.total_con_faturado_a).label("vol_faturado"),
        sqlfunc.count(sqlfunc.distinct(Volume.matricula)).label("matriculas"),
    ).filter((Volume.is_grand_total == False) | (Volume.is_grand_total == None))

    if municipio:
        vol_q = vol_q.filter(Volume.municipio == municipio)
    if regional:
        vol_q = vol_q.filter(Volume.regional_aegea == regional)
    if referencia:
        vol_q = vol_q.filter(Volume.referencia == referencia)

    vol_r = vol_q.one()
    volume_total  = vol_r.volume_total  or 0.0
    cons_medido   = vol_r.cons_medido   or 0.0
    vol_faturado  = vol_r.vol_faturado  or 0.0
    matriculas    = vol_r.matriculas    or 0

    # ── Cálculo dos KPIs ────────────────────────────────────────────────────
    ticket_medio   = round(receita_total / ligacoes, 2)   if ligacoes   > 0 else None
    receita_por_m3 = round(receita_total / volume_total, 4) if volume_total > 0 else None
    perdas_m3      = round(cons_medido - vol_faturado, 2)
    perdas_pct     = round((perdas_m3 / cons_medido) * 100, 2) if cons_medido > 0 else None

    return {
        "receita_total":    round(receita_total, 2),
        "ligacoes":         ligacoes,
        "matriculas":       matriculas,
        "ticket_medio":     ticket_medio,
        "receita_por_m3":   receita_por_m3,
        "volume_total_m3":  round(volume_total, 2),
        "cons_medido_m3":   round(cons_medido, 2),
        "vol_faturado_m3":  round(vol_faturado, 2),
        "perdas_m3":        perdas_m3,
        "perdas_pct":       perdas_pct,
    }


# ── /api/cruzamento ───────────────────────────────────────────────────────────
# Cruzamento Volume × Faturamento por município/regional.
# Anti-duplicidade: faturamento é pré-agregado por num_ligacao (CTE) antes do JOIN,
# garantindo que múltiplas rubricas por ligação não inflem os volumes.
@app.get("/api/cruzamento")
def get_cruzamento(
    db: Session = Depends(get_db),
    referencia: Optional[str] = Query(None),
    regional: Optional[str] = Query(None),
    limit: int = Query(50, le=500),
):
    where_vol = "WHERE (v.is_grand_total = 0 OR v.is_grand_total IS NULL)"
    if referencia:
        where_vol += f" AND v.referencia = :referencia"
    if regional:
        where_vol += f" AND v.regional_aegea = :regional"

    sql = text(f"""
        WITH fat_agg AS (
            SELECT
                num_ligacao,
                SUM(sum_valor)  AS faturamento_total,
                COUNT(*)        AS qtd_rubricas
            FROM faturamento
            WHERE is_grand_total = 0 OR is_grand_total IS NULL
            GROUP BY num_ligacao
        )
        SELECT
            v.municipio,
            v.regional_aegea,
            v.referencia,
            COUNT(DISTINCT v.matricula)             AS qtd_matriculas,
            ROUND(SUM(v.volume_total), 2)           AS volume_total,
            ROUND(SUM(v.total_cons_medido), 2)      AS cons_medido,
            ROUND(SUM(v.total_con_faturado_a), 2)   AS vol_faturado,
            ROUND(SUM(v.total_cons_medido)
                  - SUM(v.total_con_faturado_a), 2) AS perdas_m3,
            ROUND(COALESCE(SUM(f.faturamento_total), 0), 2)  AS faturamento_total,
            SUM(f.qtd_rubricas)                     AS qtd_rubricas
        FROM volume v
        LEFT JOIN fat_agg f ON v.matricula = f.num_ligacao
        {where_vol}
        GROUP BY v.municipio, v.regional_aegea, v.referencia
        ORDER BY SUM(v.volume_total) DESC
        LIMIT :limit
    """)

    params: dict = {"limit": limit}
    if referencia:
        params["referencia"] = referencia
    if regional:
        params["regional"] = regional

    rows = db.execute(sql, params).fetchall()
    return [
        {
            "municipio":          r.municipio,
            "regional_aegea":     r.regional_aegea,
            "referencia":         r.referencia,
            "qtd_matriculas":     r.qtd_matriculas,
            "volume_total":       r.volume_total,
            "cons_medido":        r.cons_medido,
            "vol_faturado":       r.vol_faturado,
            "perdas_m3":          r.perdas_m3,
            "faturamento_total":  r.faturamento_total,
            "qtd_rubricas":       r.qtd_rubricas,
            # Indicadores derivados
            "receita_por_m3": round(r.faturamento_total / r.volume_total, 4)
                              if r.volume_total and r.volume_total > 0 else None,
            "perdas_pct":     round((r.perdas_m3 / r.cons_medido) * 100, 2)
                              if r.cons_medido and r.cons_medido > 0 else None,
        }
        for r in rows
    ]


# ── /api/dashboard ────────────────────────────────────────────────────────────
# Retorna KPIs + agregações para os 6 eixos de análise do dashboard.
# Todos os filtros são opcionais e co-aplicados (AND).


@app.get("/api/dashboard")
def get_dashboard(
    db: Session = Depends(get_db),
    cidade: Optional[str] = Query(None),
    macro: Optional[str] = Query(None),
    micro: Optional[str] = Query(None),
    classe_rubrica: Optional[str] = Query(None),
    grupo_rubrica: Optional[str] = Query(None),
    cod_grupo: Optional[int] = Query(None),
    top_n: int = Query(15, le=50),
):
    from sqlalchemy import func as sqlfunc

    base = db.query(Faturamento).filter(
        (Faturamento.is_grand_total == False) | (Faturamento.is_grand_total == None)
    )
    if cidade:         base = base.filter(Faturamento.cidade == cidade)
    if macro:          base = base.filter(Faturamento.macro == macro)
    if micro:          base = base.filter(Faturamento.micro == micro)
    if classe_rubrica: base = base.filter(Faturamento.classe_rubrica == classe_rubrica)
    if grupo_rubrica:  base = base.filter(Faturamento.grupo_rubrica == grupo_rubrica)
    if cod_grupo is not None: base = base.filter(Faturamento.cod_grupo == cod_grupo)

    kpi = base.with_entities(
        sqlfunc.sum(Faturamento.sum_valor).label("receita_total"),
        sqlfunc.count(Faturamento.id).label("qtd"),
        sqlfunc.count(sqlfunc.distinct(Faturamento.num_ligacao)).label("ligacoes"),
    ).one()
    receita_total = kpi.receita_total or 0.0
    qtd           = kpi.qtd           or 0
    ligacoes      = kpi.ligacoes      or 0

    def agg(col, n=top_n):
        rows = (
            base.with_entities(
                col.label("label"),
                sqlfunc.sum(Faturamento.sum_valor).label("valor"),
                sqlfunc.count(Faturamento.id).label("qtd"),
            )
            .group_by(col)
            .order_by(sqlfunc.sum(Faturamento.sum_valor).desc())
            .limit(n)
            .all()
        )
        return [
            {"label": str(r.label) if r.label is not None else "N/A",
             "valor": round(r.valor or 0, 2),
             "qtd": r.qtd}
            for r in rows
        ]

    return {
        "kpis": {
            "receita_total":      round(receita_total, 2),
            "qtd_registros":      qtd,
            "ligacoes_distintas": ligacoes,
            "ticket_medio":       round(receita_total / ligacoes, 2) if ligacoes > 0 else None,
        },
        "by_classe_rubrica": agg(Faturamento.classe_rubrica),
        "by_cidade":         agg(Faturamento.cidade),
        "by_macro":          agg(Faturamento.macro),
        "by_micro":          agg(Faturamento.micro),
        "by_grupo_rubrica":  agg(Faturamento.grupo_rubrica, n=10),
        "by_cod_grupo":      agg(Faturamento.cod_grupo, n=10),
    }


@app.get("/api/decomposicao-receita")
def get_decomposicao_receita(
    db: Session = Depends(get_db),
    cidade: Optional[str] = Query(None),
    macro: Optional[str] = Query(None),
    micro: Optional[str] = Query(None),
    classe_rubrica: Optional[str] = Query(None),
    grupo_rubrica: Optional[str] = Query(None),
    cod_grupo: Optional[int] = Query(None),
    top_n_descricoes: int = Query(120, ge=20, le=500),
):
    """
    Decomposicao hierarquica da receita:
    TOTAL -> grupo_rubrica -> classe_rubrica -> descricao_rubrica.
    """
    from sqlalchemy import func as sqlfunc

    base = db.query(Faturamento).filter(
        (Faturamento.is_grand_total == False) | (Faturamento.is_grand_total == None)
    )
    if cidade:
        base = base.filter(Faturamento.cidade == cidade)
    if macro:
        base = base.filter(Faturamento.macro == macro)
    if micro:
        base = base.filter(Faturamento.micro == micro)
    if classe_rubrica:
        base = base.filter(Faturamento.classe_rubrica == classe_rubrica)
    if grupo_rubrica:
        base = base.filter(Faturamento.grupo_rubrica == grupo_rubrica)
    if cod_grupo is not None:
        base = base.filter(Faturamento.cod_grupo == cod_grupo)

    rows = (
        base.with_entities(
            Faturamento.grupo_rubrica.label("grupo_rubrica"),
            Faturamento.classe_rubrica.label("classe_rubrica"),
            Faturamento.descricao_rubrica.label("descricao_rubrica"),
            sqlfunc.sum(Faturamento.sum_valor).label("valor"),
        )
        .group_by(
            Faturamento.grupo_rubrica,
            Faturamento.classe_rubrica,
            Faturamento.descricao_rubrica,
        )
        .order_by(sqlfunc.sum(Faturamento.sum_valor).desc())
        .limit(top_n_descricoes)
        .all()
    )

    data = []
    total = 0.0
    for r in rows:
        v = float(r.valor or 0.0)
        total += v
        data.append(
            {
                "grupo_rubrica": r.grupo_rubrica or "N/A",
                "classe_rubrica": r.classe_rubrica or "N/A",
                "descricao_rubrica": r.descricao_rubrica or "N/A",
                "valor": round(v, 2),
            }
        )

    return {
        "total": round(total, 2),
        "qtd_itens": len(data),
        "data": data,
    }


# ── /api/metas ────────────────────────────────────────────────────────────────
# Realizado vs Meta por cidade e por agrupador.
# Anti-duplicidade: faturamento é pré-agregado por cidade (CTE) antes do JOIN,
# garantindo que múltiplas rubricas por cidade não inflem os valores de meta.


@app.get("/api/metas")
def get_metas(
    db: Session = Depends(get_db),
    cidade: Optional[str] = Query(None),
    macro:  Optional[str] = Query(None),
    micro:  Optional[str] = Query(None),
    agrupador: Optional[str] = Query(None),
    top_n: int = Query(50, le=200),
):
    from sqlalchemy import func as sqlfunc

    # ── Conta se existem dados de meta ────────────────────────────────────────
    cidades_meta = (
        db.query(sqlfunc.count(sqlfunc.distinct(MetaFaturamento.cidade))).scalar() or 0
    )
    if cidades_meta == 0:
        return {
            "kpis": {
                "meta_total": 0, "realizado": 0,
                "pct_atingimento": None, "gap": 0,
                "cidades_meta": 0, "cidades_acima": 0,
            },
            "by_cidade": [],
            "by_agrupador": [],
        }

    # Apenas estes agrupadores compõem a meta total
    _IN = "('DIRETAS \u00c1GUA','DIRETAS AGUA'," \
          "'DIRETAS ESGOTO'," \
          "'INDIRETAS \u00c1GUA','INDIRETAS AGUA'," \
          "'INDIRETAS ESGOTO'," \
          "'SERVI\u00c7O B\u00c1SICO','SERVICO BASICO')"

    params: dict = {
        "cidade":    cidade,
        "macro":     macro,
        "micro":     micro,
        "agrupador": agrupador,
        "top_n":     top_n,
    }

    # ── Meta agregada por cidade (respeitando filtros) ─────────────────────────
    meta_sql = text(f"""
        SELECT
            UPPER(TRIM(cidade)) AS cidade_key,
            cidade,
            MAX(macro)          AS macro,
            MAX(micro)          AS micro,
            SUM(valor)          AS meta
        FROM meta_faturamento
        WHERE (:cidade    IS NULL OR cidade    = :cidade)
          AND (:macro     IS NULL OR macro     = :macro)
          AND (:micro     IS NULL OR micro     = :micro)
          AND (:agrupador IS NULL OR agrupador = :agrupador)
          AND UPPER(TRIM(agrupador)) IN {_IN}
        GROUP BY UPPER(TRIM(cidade))
        ORDER BY meta DESC
        LIMIT :top_n
    """)

    # ── Meta TOTAL (sem LIMIT) e número de municípios ──────────────────────────
    total_meta_sql = text(f"""
        SELECT 
            SUM(valor) AS total_meta,
            COUNT(DISTINCT UPPER(TRIM(cidade))) AS cidades_meta
        FROM meta_faturamento
        WHERE (:cidade    IS NULL OR cidade    = :cidade)
          AND (:macro     IS NULL OR macro     = :macro)
          AND (:micro     IS NULL OR micro     = :micro)
          AND (:agrupador IS NULL OR agrupador = :agrupador)
          AND UPPER(TRIM(agrupador)) IN {_IN}
    """)
    total_result = db.execute(total_meta_sql, params).one()
    total_meta = float(total_result.total_meta or 0)
    total_cidades_meta = total_result.cidades_meta or 0

    # ── Faturamento realizado agregado por cidade (sem filtro de tipo) ─────────
    fat_sql = text("""
        SELECT
            UPPER(TRIM(cidade)) AS cidade_key,
            SUM(sum_valor)                  AS realizado,
            COUNT(DISTINCT num_ligacao)     AS ligacoes
        FROM faturamento
        WHERE (is_grand_total IS NULL OR is_grand_total = 0)
        GROUP BY UPPER(TRIM(cidade))
    """)
    fat_dict  = {r.cidade_key: r for r in db.execute(fat_sql).fetchall()}

    # ── Lista de todas as cidades com meta (sem LIMIT) para cálculo do realizado total
    cidades_meta_keys_sql = text(f"""
        SELECT DISTINCT UPPER(TRIM(cidade)) AS cidade_key
        FROM meta_faturamento
        WHERE (:cidade    IS NULL OR cidade    = :cidade)
          AND (:macro     IS NULL OR macro     = :macro)
          AND (:micro     IS NULL OR micro     = :micro)
          AND (:agrupador IS NULL OR agrupador = :agrupador)
          AND UPPER(TRIM(agrupador)) IN {_IN}
    """)
    cidades_meta_keys = [row[0] for row in db.execute(cidades_meta_keys_sql, params).fetchall()]

    # Realizado total sobre as cidades que têm meta (respeitando filtros)
    realizado_total = 0.0
    for key in cidades_meta_keys:
        f = fat_dict.get(key)
        if f:
            realizado_total += float(f.realizado or 0)

    # ── Construir lista de cidades para exibição (limitada a top_n) ───────────
    meta_rows = db.execute(meta_sql, params).fetchall()
    by_cidade = []
    for m in meta_rows:
        f         = fat_dict.get(m.cidade_key)
        realizado = float(f.realizado or 0) if f else 0.0
        meta_val  = float(m.meta or 0)
        gap       = round(meta_val - realizado, 2)
        pct       = round(realizado / meta_val * 100, 2) if meta_val > 0 else 0.0
        by_cidade.append({
            "cidade":          m.cidade,
            "macro":           m.macro,
            "micro":           m.micro,
            "meta":            round(meta_val, 2),
            "realizado":       round(realizado, 2),
            "gap":             gap,
            "pct_atingimento": pct,
            "ligacoes":        int(f.ligacoes) if f else 0,
        })

    # Ordena por % atingimento (pior primeiro — foco nos desvios)
    by_cidade.sort(key=lambda x: x["pct_atingimento"])

    # ── Meta por agrupador ─────────────────────────────────────────────────────
    ag_sql = text(f"""
        SELECT
            agrupador,
            SUM(valor) AS meta
        FROM meta_faturamento
        WHERE (:cidade    IS NULL OR cidade    = :cidade)
          AND (:macro     IS NULL OR macro     = :macro)
          AND (:micro     IS NULL OR micro     = :micro)
          AND UPPER(TRIM(agrupador)) IN {_IN}
        GROUP BY agrupador
        ORDER BY meta DESC
    """)
    ag_rows = db.execute(ag_sql, {
        "cidade": cidade, "macro": macro, "micro": micro
    }).fetchall()
    by_agrupador = [
        {"agrupador": r.agrupador or "N/A", "meta": round(float(r.meta or 0), 2)}
        for r in ag_rows
    ]

    # ── KPIs globais (sobre todos os dados, não apenas top_n) ──────────────────
    gap_total     = round(total_meta - realizado_total, 2)
    pct_global    = round(realizado_total / total_meta * 100, 2) if total_meta > 0 else 0.0
    cidades_acima = sum(1 for r in by_cidade if r["pct_atingimento"] >= 100)

    return {
        "kpis": {
            "meta_total":       round(total_meta, 2),
            "realizado":        round(realizado_total, 2),
            "pct_atingimento":  pct_global,
            "gap":              gap_total,
            "cidades_meta":     total_cidades_meta,
            "cidades_acima":    cidades_acima,
        },
        "by_cidade":    by_cidade,
        "by_agrupador": by_agrupador,
    }


# ── /api/metas/por-cod-grupo ──────────────────────────────────────────────────
# Compara meta vs faturado agrupados por cod_grupo.
# Retorna: cod_grupo, meta, faturado e % atingimento.

@app.get("/api/metas/por-cod-grupo")
def get_metas_por_cod_grupo(
    db: Session = Depends(get_db),
    cidade: Optional[str] = Query(None),
    macro:  Optional[str] = Query(None),
    micro:  Optional[str] = Query(None),
):
    _IN = "('DIRETAS \u00c1GUA','DIRETAS AGUA'," \
          "'DIRETAS ESGOTO'," \
          "'INDIRETAS \u00c1GUA','INDIRETAS AGUA'," \
          "'INDIRETAS ESGOTO'," \
          "'SERVI\u00c7O B\u00c1SICO','SERVICO BASICO')"

    params: dict = {"cidade": cidade, "macro": macro, "micro": micro}

    sql = text(f"""
        SELECT
            m.cod_grupo,
            SUM(m.valor)                        AS meta,
            COALESCE(MAX(f.faturado), 0)        AS faturado
        FROM meta_faturamento m
        LEFT JOIN (
            SELECT cod_grupo, SUM(sum_valor) AS faturado
            FROM faturamento
            WHERE (is_grand_total IS NULL OR is_grand_total = 0)
            GROUP BY cod_grupo
        ) f ON m.cod_grupo = f.cod_grupo
        WHERE m.cod_grupo IS NOT NULL
          AND UPPER(TRIM(m.agrupador)) IN {_IN}
          AND (:cidade IS NULL OR m.cidade = :cidade)
          AND (:macro  IS NULL OR m.macro  = :macro)
          AND (:micro  IS NULL OR m.micro  = :micro)
        GROUP BY m.cod_grupo
        ORDER BY m.cod_grupo
    """)

    rows = db.execute(sql, params).fetchall()
    return [
        {
            "cod_grupo":       r.cod_grupo,
            "meta":            round(float(r.meta or 0), 2),
            "faturado":        round(float(r.faturado or 0), 2),
            "pct_atingimento": round(float(r.faturado or 0) / float(r.meta) * 100, 2)
                               if r.meta and float(r.meta) > 0 else 0.0,
        }
        for r in rows
    ]


# ── /api/db ────────────────────────────────────────────────────────────────────
# Painel de administração do banco de dados: listar, editar, excluir, limpar.

ALLOWED_TABLES = {
    "faturamento":      Faturamento,
    "volume":           Volume,
    "meta_faturamento": MetaFaturamento,
}


def _row_to_dict(row):
    return {c.name: getattr(row, c.name) for c in row.__table__.columns}


@app.get("/api/db/tables")
def db_tables(db: Session = Depends(get_db)):
    from sqlalchemy import func as sqlfunc
    return {
        tname: db.query(sqlfunc.count(model.id)).scalar() or 0
        for tname, model in ALLOWED_TABLES.items()
    }


@app.get("/api/db/{table}/rows")
def db_rows(
    table: str,
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, le=500),
    q: Optional[str] = Query(None),
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail="Tabela não encontrada")
    model = ALLOWED_TABLES[table]
    query = db.query(model)
    if q:
        from sqlalchemy import or_
        from sqlalchemy.types import String as SAString
        str_attrs = [
            getattr(model, c.name)
            for c in model.__table__.columns
            if isinstance(c.type, SAString)
        ]
        if str_attrs:
            query = query.filter(or_(*[a.ilike(f"%{q}%") for a in str_attrs]))
    total = query.count()
    rows  = query.offset((page - 1) * per_page).limit(per_page).all()
    return {
        "total":    total,
        "page":     page,
        "per_page": per_page,
        "pages":    max(1, (total + per_page - 1) // per_page),
        "rows":     [_row_to_dict(r) for r in rows],
    }


@app.delete("/api/db/{table}/clear")
def db_clear(table: str, db: Session = Depends(get_db)):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail="Tabela não encontrada")
    deleted = db.query(ALLOWED_TABLES[table]).delete()
    db.commit()
    return {"deleted": deleted}


@app.delete("/api/db/{table}/{row_id}")
def db_delete_row(table: str, row_id: int, db: Session = Depends(get_db)):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail="Tabela não encontrada")
    model = ALLOWED_TABLES[table]
    row = db.query(model).filter(model.id == row_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Registro não encontrado")
    db.delete(row)
    db.commit()
    return {"ok": True}


@app.patch("/api/db/{table}/{row_id}")
async def db_update_row(
    table: str,
    row_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    if table not in ALLOWED_TABLES:
        raise HTTPException(status_code=404, detail="Tabela não encontrada")
    model = ALLOWED_TABLES[table]
    row = db.query(model).filter(model.id == row_id).first()
    if not row:
        raise HTTPException(status_code=404, detail="Registro não encontrado")
    data = await request.json()
    for col in model.__table__.columns:
        if col.name == "id":
            continue
        if col.name in data:
            setattr(row, col.name, data[col.name])
    db.commit()
    db.refresh(row)
    return _row_to_dict(row)