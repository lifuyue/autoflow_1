# AutoFlow IO Demo Guide

## Dependencies
```
pip install pandas==2.2.2 openpyxl==3.1.5 PyYAML==6.0.2 pdfplumber==0.10.3 PyPDF2==3.0.1
```

## Excel Demo
```
python -m tools.demo_excel --source examples/source_a.xlsx --template examples/target_tpl.xlsx \
  --mapping examples/fixed_mapping.yaml --out out/invoice_out.xlsx
```
Add `--dry-run` to preview without writing.

## PDF Demo
```
python -m tools.demo_pdf --pdf examples/sample.pdf --out out --export-pages 1 --set-meta "Title=Demo;Author=AutoFlow"
```

Both commands auto-create sample files under `examples/` if missing. Logs go to `~/AutoFlow/logs/autoflow_io.log` by default.
