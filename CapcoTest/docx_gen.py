from pathlib import Path

from docxtpl import DocxTemplate



docx_path = Path("/Users/KPRO/Downloads/docx_template.docx")
docx = DocxTemplate(template_file=docx_path)
context = { 'name' : "World company", "detail" : "foo bar ipso facto"}
docx.render(context)
docx.save("/Users/KPRO/Downloads/generated_doc.docx")
