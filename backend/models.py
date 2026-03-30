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
