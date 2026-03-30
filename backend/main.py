import os
import io
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import text
import pandas as pd

from database import engine, get_db, Base
from models import Faturamento

Base.metadata.create_all(bind=engine)

app = FastAPI(title="Faturamento Platform API", version="1.0.0")

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


# Column name normalization mapping
COLUMN_MAP = {
    # Identificação da ligação
    "ORIGEM": "origem",
    "TB_FATURAMENTO_LIQUIDO_2ORIGEM": "origem",
    "NUM_LIGACAO": "num_ligacao",
    "TB_FATURAMENTO_LIQUIDO_2NUM_LIGACAO": "num_ligacao",
    "CIDADE": "cidade",
    "TB_FATURAMENTO_LIQUIDO_2CIDADE": "cidade",
    "MACRO": "macro",
    "MICRO": "micro",
    # Hierarquia de rubricas
    "COD_CLASSE_RUBRICA": "cod_classe_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2COD_CLASSE_RUBRICA": "cod_classe_rubrica",
    "CLASSE_RUBRICA": "classe_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2CLASSE_RUBRICA": "classe_rubrica",
    "COD_GRUPO_RUBRICA": "cod_grupo_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2COD_GRUPO_RUBRICA": "cod_grupo_rubrica",
    "GRUPO_RUBRICA": "grupo_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2GRUPO_RUBRICA": "grupo_rubrica",
    "COD_RUBRICA": "cod_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2COD_RUBRICA": "cod_rubrica",
    "DESCRICAO_RUBRICA": "descricao_rubrica",
    "TB_FATURAMENTO_LIQUIDO_2DESCRICAO_RUBRICA": "descricao_rubrica",
    # Nota / cancelamento
    "NUM_NOTA": "num_nota",
    "TB_FATURAMENTO_LIQUIDO_2NUM_NOTA": "num_nota",
    "NUM_NOTA_FISCAL": "num_nota_fiscal",
    "TB_FATURAMENTO_LIQUIDO_2NUM_NOTA_FISCAL": "num_nota_fiscal",
    "NUM_NOTA_ORIGINAL": "num_nota_original",
    "TB_FATURAMENTO_LIQUIDO_2NUM_NOTA_ORIGINAL": "num_nota_original",
    "COD_GRUPO": "cod_grupo",
    "TB_FATURAMENTO_LIQUIDO_2COD_GRUPO": "cod_grupo",
    "MOTIVO_CANCELAMENTO": "motivo_cancelamento",
    "TB_FATURAMENTO_LIQUIDO_2MOTIVO_CANCELAMENTO": "motivo_cancelamento",
    # Valor
    "ISGRANDTOTALROWTOTAL": "is_grand_total",
    "SUMVALOR": "sum_valor",
    "SUM_VALOR": "sum_valor",
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


@app.post("/api/upload")
async def upload_excel(file: UploadFile = File(...), db: Session = Depends(get_db)):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(status_code=400, detail="Arquivo deve ser .xlsx ou .xls")

    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents), dtype=object)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Erro ao ler Excel: {str(e)}")

    original_cols = list(df.columns)
    df.columns = [normalize_col(c) for c in df.columns]
    normalized_cols = list(df.columns)

    rename_map = {}
    already_mapped = set()
    for col in normalized_cols:
        target = COLUMN_MAP.get(col)
        if target and target not in already_mapped:
            rename_map[col] = target
            already_mapped.add(target)

    df.rename(columns=rename_map, inplace=True)

    NUMERIC_FIELDS = [
        'cod_classe_rubrica', 'cod_grupo_rubrica', 'cod_rubrica',
        'cod_grupo', 'sum_valor',
    ]
    for field in NUMERIC_FIELDS:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce')

    BOOL_FIELDS = ['is_grand_total']
    for field in BOOL_FIELDS:
        if field in df.columns:
            df[field] = df[field].map(lambda v: False if str(v).strip().lower() == 'false' else (True if str(v).strip().lower() == 'true' else None))

    try:
        db.query(Faturamento).delete()
        db.commit()
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Erro ao limpar banco: {str(e)}")

    model_fields = {c.name for c in Faturamento.__table__.columns} - {"id"}

    def safe_val(val):
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
        import datetime
        if isinstance(val, (pd.Timestamp, datetime.datetime, datetime.date)):
            return str(val)
        if isinstance(val, float):
            import math
            if math.isinf(val) or math.isnan(val):
                return None
        return val

    try:
        records = []
        for _, row in df.iterrows():
            data = {}
            for field in model_fields:
                val = row[field] if field in row.index else None
                data[field] = safe_val(val)
            records.append(Faturamento(**data))

        db.add_all(records)
        db.commit()
    except Exception as e:
        db.rollback()
        import traceback
        raise HTTPException(status_code=500, detail=f"Erro ao inserir dados: {str(e)} | {traceback.format_exc()}")

    unmapped = [normalized_cols[i] for i, orig in enumerate(normalized_cols)
                if normalized_cols[i] not in rename_map and normalized_cols[i] not in model_fields]

    return {
        "message": f"{len(records)} registros importados com sucesso.",
        "total": len(records),
        "colunas_mapeadas": list(rename_map.keys()),
        "colunas_nao_mapeadas": unmapped,
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
    cidades = [r[0] for r in db.query(Faturamento.cidade).distinct().order_by(Faturamento.cidade).all() if r[0]]
    macros = [r[0] for r in db.query(Faturamento.macro).distinct().order_by(Faturamento.macro).all() if r[0]]
    micros = [r[0] for r in db.query(Faturamento.micro).distinct().order_by(Faturamento.micro).all() if r[0]]
    origens = [r[0] for r in db.query(Faturamento.origem).distinct().all() if r[0]]
    return {"cidades": cidades, "macros": macros, "micros": micros, "origens": origens}


@app.get("/api/stats")
def get_stats(db: Session = Depends(get_db)):
    from sqlalchemy import func as sqlfunc
    total = db.query(Faturamento).count()
    result = (
        db.query(
            Faturamento.classe_rubrica,
            sqlfunc.count(Faturamento.id).label("qtd"),
            sqlfunc.sum(Faturamento.sum_valor).label("total_fat"),
        )
        .group_by(Faturamento.classe_rubrica)
        .order_by(sqlfunc.count(Faturamento.id).desc())
        .all()
    )
    by_classe = [{"classe": r.classe_rubrica or "N/A", "qtd": r.qtd, "total_fat": r.total_fat or 0} for r in result]
    return {"total": total, "by_classe": by_classe}
