"""Parser de señales mínimo. Solo referencias al PASADO.

Regla central de la skill: `shift >= 0` (hacia atras) esta permitido; cualquier acceso al futuro
debe rechazarse en el propio parser, no en tiempo de ejecucion.
"""
from __future__ import annotations

import ast

_PERMITIDAS = {"close", "open", "high", "low", "volume"}


class _CompruebaFuturo(ast.NodeVisitor):
    """Recorre el AST buscando indices o shifts negativos."""

    def __init__(self) -> None:
        self.fuera = False

    def visit_Subscript(self, node: ast.Subscript) -> None:
        # close[+1] -> Index(value=UnaryOp(USub, Constant(1)))
        sl = node.slice
        # `close[+1]` llega como UnaryOp(USAdd); cualquier signo explicito en el indice se toma
        # como acceso por posicion, y el indice positivo es el futuro.
        if isinstance(sl, ast.UnaryOp) and isinstance(sl.op, (ast.USub, ast.UAdd)):
            self.fuera = True
        elif isinstance(sl, ast.Constant) and isinstance(sl.value, (int, float)):
            if sl.value > 0:
                self.fuera = True
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        # shift(-1)
        if isinstance(node.func, ast.Attribute) and node.func.attr == "shift":
            for a in node.args:
                if isinstance(a, ast.UnaryOp) and isinstance(a.op, ast.USub):
                    self.fuera = True
                elif isinstance(a, ast.Constant) and isinstance(a.value, (int, float)) and a.value < 0:
                    self.fuera = True
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if node.id not in _PERMITIDAS and node.id not in {"nan", "inf"}:
            raise ValueError(f"columna no permitida: {node.id!r}")


def parse_signal(expr: str) -> ast.Expression:
    """Valida y devuelve el AST. Lanza `ValueError` si hay acceso al futuro."""
    try:
        arbol = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"expresion invalida: {expr!r} ({exc})") from exc
    c = _CompruebaFuturo()
    c.visit(arbol)
    if c.fuera:
        raise ValueError(
            f"acceso al futuro rechazado: {expr!r}. "
            "Las señales solo pueden mirar hacia atras (shift >= 0).")
    return arbol
