from typing import List
from pypdf import PdfWriter
from pathlib import Path

def merge(pdfs: List[Path]) -> Path:
