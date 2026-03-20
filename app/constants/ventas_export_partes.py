"""Constantes de números de parte usadas en exportaciones y BOM Breaking."""

# Excel del botón "Descargar" en Registros de Ventas:
# - `producto` en BD = coincidencia exacta con algún valor de la tupla siguiente.
# - `producto_condensado` (Artnr condensed) = coincidencia exacta con los primeros N caracteres de cada valor.
VENTAS_EXPORT_PRODUCTO_CONDENSADO_PREFIX_LEN = 9

NUMEROS_PARTE_EXPORT_REGISTROS_VENTAS = (
    "76D00306A000H4016",
    "76D00306E000H4016",
    "76D00306D000H4016",
    "76D00069GXXXH6012",
    "76D00069GXXXK5500",
    "62001018B000K5608",
    "62001380I000K8002",
    "62001005D000K8002",
    "62A00150M000K8002",
    "6200001BA000K8002",
    "76D00184M000K8002",
    "76D00184M000H6012",
    "76D00069MXXXH6012",
    "76D00069MXXXH4016",
    "76D00069MXXXK5500",
    "76D00306A000H6012",
    "76D00306AXXXK8002",
    "76D00306A000H6008",
)

# Dashboard: descarga Excel BOM Breaking — mismos códigos únicos de 9 caracteres que el condensado de la lista anterior.
NUMEROS_PARTE_BOM_BREAKING = tuple(
    sorted(
        {
            p.strip()[:VENTAS_EXPORT_PRODUCTO_CONDENSADO_PREFIX_LEN]
            for p in NUMEROS_PARTE_EXPORT_REGISTROS_VENTAS
        }
    )
)
