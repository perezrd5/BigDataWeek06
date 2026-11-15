"""Execute all cells and retain the notebook even if a prerequisite fails."""
import json
from datetime import datetime, timezone
from pathlib import Path
import nbformat
from nbclient import NotebookClient

if __name__ == '__main__':
    results = Path('lab_results')
    results.mkdir(exist_ok=True)
    manifest = {'started_utc': datetime.now(timezone.utc).isoformat(), 'status': 'running'}
    manifest_path = results/'latest_execution.json'
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    nb = nbformat.read('alaska_integration.ipynb', as_version=4)
    for cell in nb.cells:
        if cell.cell_type == 'code':
            cell.outputs = []
            cell.execution_count = None
    try:
        NotebookClient(nb, timeout=1800, kernel_name='python3', resources={
            'metadata': {'path': str(Path.cwd())}}).execute()
    except Exception as exc:
        manifest.update(status='failed', error=f'{type(exc).__name__}: {exc}')
        manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
        raise
    finally:
        nbformat.write(nb, 'alaska_integration.executed.ipynb')
    evidence = json.loads(Path('lab_results/evidence.json').read_text())
    assert all(evidence['status'][k].startswith('completed') for k in
               ['pandas', 'dask', 'sql', 'spark', 'mongo', 'kafka']), evidence['status']
    text = '# Findings from the completed run\n\n```json\n' + json.dumps(evidence, indent=2) + '\n```\n'
    text += ('\nUpdate the report with these measured values. API events replay actual Alaska titles; '
             'they are not a live commercial feed. Spark workers are separate containers on one '
             'host, not separate physical machines.\n')
    Path('lab_results/current_findings.md').write_text(text, encoding='utf-8')
    manifest.update(status='completed', finished_utc=datetime.now(timezone.utc).isoformat())
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding='utf-8')
    print('All stages completed; see alaska_integration.executed.ipynb')
