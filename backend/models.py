from sqlalchemy import Column, Integer, String, Float, Boolean
from database import Base


class Faturamento(Base):
    __tablename__ = "faturamento"

    id                  = Column(Integer, primary_key=True, index=True)
    origem              = Column(String, nullable=True)
    num_ligacao         = Column(String, nullable=True, index=True)
    cidade              = Column(String, nullable=True, index=True)
    macro               = Column(String, nullable=True, index=True)
    micro               = Column(String, nullable=True, index=True)

    cod_classe_rubrica  = Column(Integer, nullable=True)
    classe_rubrica      = Column(String, nullable=True)
    cod_grupo_rubrica   = Column(Integer, nullable=True)
    grupo_rubrica       = Column(String, nullable=True)
    cod_rubrica         = Column(Integer, nullable=True, index=True)
    descricao_rubrica   = Column(String, nullable=True)

    num_nota            = Column(String, nullable=True)
    num_nota_fiscal     = Column(String, nullable=True)
    num_nota_original   = Column(String, nullable=True)
    cod_grupo           = Column(Integer, nullable=True)
    motivo_cancelamento = Column(String, nullable=True)

    is_grand_total      = Column(Boolean, nullable=True)
    sum_valor           = Column(Float, nullable=True)


class Volume(Base):
    """Tabela de volume operacional — granularidade: matrícula × referência."""
    __tablename__ = "volume"

    id                                  = Column(Integer, primary_key=True, index=True)
    regional_aegea                      = Column(String, nullable=True, index=True)
    municipio                           = Column(String, nullable=True, index=True)
    faixa                               = Column(String, nullable=True)
    faixa_ordem                         = Column(Integer, nullable=True)
    referencia                          = Column(String, nullable=True, index=True)  # YYYY-MM
    matricula                           = Column(String, nullable=True, index=True)  # FK → Faturamento.num_ligacao
    is_grand_total                      = Column(Boolean, nullable=True)
    total_volume_estimado_operacional   = Column(Float, nullable=True)
    total_cons_medido                   = Column(Float, nullable=True)
    total_volume_estimado               = Column(Float, nullable=True)
    total_volume_estimado_regrafat      = Column(Float, nullable=True)
    volume_total                        = Column(Float, nullable=True)
    total_con_faturado_a                = Column(Float, nullable=True)


class MetaFaturamento(Base):
    """Meta de faturamento — granularidade: cidade × agrupador."""
    __tablename__ = "meta_faturamento"

    id        = Column(Integer, primary_key=True, index=True)
    cidade    = Column(String, nullable=True, index=True)
    diretoria = Column(String, nullable=True)
    macro     = Column(String, nullable=True, index=True)
    micro     = Column(String, nullable=True, index=True)
    cod_grupo = Column(Integer, nullable=True)
    agrupador = Column(String, nullable=True, index=True)   # ex: DIRETAS ÁGUA
    valor     = Column(Float, nullable=True)
