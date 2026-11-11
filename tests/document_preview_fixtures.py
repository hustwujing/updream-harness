from pathlib import Path

from docx import Document
from openpyxl import Workbook
from pptx import Presentation
from reportlab.pdfgen.canvas import Canvas


def create(root: Path) -> None:
    root.mkdir(parents=True, exist_ok=True)
    pdf = Canvas(str(root / "sample.pdf"))
    for page in (1, 2):
        pdf.drawString(72, 760, f"PDF page {page}")
        pdf.showPage()
    pdf.save()

    presentation = Presentation()
    for page in (1, 2):
        slide = presentation.slides.add_slide(presentation.slide_layouts[5])
        slide.shapes.title.text = f"PPT page {page}"
    presentation.save(root / "sample.pptx")

    document = Document()
    document.add_heading("DOCX preview", level=1)
    document.add_paragraph("Scrollable Word document content")
    document.save(root / "sample.docx")

    workbook = Workbook()
    first = workbook.active
    first.title = "Summary"
    first.append(["Metric", "Value"])
    first.append(["Images", 3])
    detail = workbook.create_sheet("Details")
    detail.append(["Item", "Status"])
    detail.append(["Video", "Ready"])
    workbook.save(root / "sample.xlsx")


if __name__ == "__main__":
    create(Path(__file__).resolve().parents[1] / ".data" / "document-preview-fixtures")
