"""Validate syntax and execute batch stages in an isolated QA directory."""
import ast
import json
import os
from pathlib import Path
import shutil

root = Path(__file__).resolve().parent
notebook = json.loads((root/'alaska_integration.ipynb').read_text(encoding='utf-8'))
for index, cell in enumerate(notebook['cells']):
    if cell['cell_type'] == 'code':
        ast.parse(''.join(cell['source']), filename=f'cell_{index}')
for name in ['alaska_integration.py', 'lab_extensions.py', 'run_lab.py']:
    ast.parse((root/name).read_text(encoding='utf-8'), filename=name)
import nbformat
nbformat.validate(nbformat.read(root/'alaska_integration.ipynb', as_version=4))
qa = root/'qa_results'
(qa/'lab_data').mkdir(parents=True, exist_ok=True)
shutil.copy2(root/'lab_data/monitor.tar.gz', qa/'lab_data/monitor.tar.gz')
os.chdir(qa)
namespace = {'__name__': '__main__'}
for index in [1, 3, 5, 7, 9, 11]:
    code = ''.join(notebook['cells'][index]['source'])
    if index == 9:
        code = code.split('unified = mongo_integration')[0]
    exec(compile(code, f'cell_{index}', 'exec'), namespace)
summary = {'syntax': 'passed', 'notebook_schema': 'passed',
           'batch_stages': 'passed', 'quality': namespace['quality'],
           'entity_resolution': namespace['er'], 'deduplication': namespace['dedup_summary'],
           'external_services': 'not tested: Docker and Java unavailable in this session'}
(qa/'validation.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
print(json.dumps(summary, indent=2))
