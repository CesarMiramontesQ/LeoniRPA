"""Pruebas de paginación del PDF del certificado C.O. (USMCA / T-MEC)."""
from __future__ import annotations

import unittest
from datetime import date

from main import (
    _CERT_CO_LEONI,
    _cert_co_pdf_total_pages_for_context,
    _render_certificado_co_pdf_reportlab,
    render_table_with_pagination,
)


def _base_ctx(**kwargs):
    ctx = {
        **_CERT_CO_LEONI,
        "codigo_cliente": 1,
        "cliente_nombre": "Cliente Prueba",
        "importer_address": "Calle 1",
        "importer_show_customer_number": True,
        "blanket_period_from": "1/1/26",
        "blanket_period_to": "12/31/26",
        "certification_date": "4/21/26",
        "_blanket_period_from_date": date(2026, 1, 1),
        "_blanket_period_to_date": date(2026, 12, 31),
    }
    ctx.update(kwargs)
    return ctx


def _part(i: int, desc_len: int = 30, customer: str | None = None):
    d = ("X" * desc_len)[:500]
    return {
        "part_number_leoni": f"12345678{i % 10}",
        "part_number": f"12345678{i % 10}AB",
        "customer_part_number": (customer or f"CUST-{i}"),
        "description": d,
        "tariff_schedule": "8544420100",
        "origin": "MX",
    }


class TestCertificadoCoPdfPagination(unittest.TestCase):
    def test_sin_partes_una_pagina(self):
        ctx = _base_ctx(partes_co3=[])
        self.assertEqual(_cert_co_pdf_total_pages_for_context(ctx), 1)
        pdf = _render_certificado_co_pdf_reportlab(ctx)
        self.assertTrue(pdf.startswith(b"%PDF"))

    def test_pocas_filas_dos_paginas(self):
        ctx = _base_ctx(partes_co3=[_part(i) for i in range(4)])
        self.assertEqual(_cert_co_pdf_total_pages_for_context(ctx), 2)

    def test_descripciones_largas_mas_paginas(self):
        ctx = _base_ctx(partes_co3=[_part(i, desc_len=200) for i in range(40)])
        n = _cert_co_pdf_total_pages_for_context(ctx)
        self.assertGreaterEqual(n, 3)

    def test_cientos_de_partes_pdf_coincide_con_total(self):
        from io import BytesIO

        from pypdf import PdfReader

        rows = [_part(i, desc_len=80) for i in range(350)]
        ctx = _base_ctx(partes_co3=rows)
        n = _cert_co_pdf_total_pages_for_context(ctx)
        self.assertGreater(n, 10)
        pdf = _render_certificado_co_pdf_reportlab(ctx)
        r = PdfReader(BytesIO(pdf))
        self.assertEqual(len(r.pages), n)

    def test_cross_reference_distinto_customer(self):
        ctx = _base_ctx(
            partes_co3=[
                _part(0, customer="ALT-001"),
                _part(0, customer="ALT-002"),
            ]
        )
        self.assertEqual(_cert_co_pdf_total_pages_for_context(ctx), 2)
        pdf = _render_certificado_co_pdf_reportlab(ctx)
        self.assertIn(b"PDF", pdf[:8])

    def test_render_table_with_pagination_exportada(self):
        """API explícita solicitada: función reutilizable."""
        self.assertTrue(callable(render_table_with_pagination))


if __name__ == "__main__":
    unittest.main()
