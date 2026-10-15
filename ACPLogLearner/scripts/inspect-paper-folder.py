from pathlib import Path
import hashlib
import sys
from pypdf import PdfReader
sys.stdout.reconfigure(encoding='utf-8')

files = list(Path(r'D:\Dev\books\artigos').rglob('*.pdf'))
seen = set()
print('TOTAL', len(files))
for path in files:
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    if digest in seen:
        continue
    seen.add(digest)
    try:
        reader = PdfReader(path)
        text = reader.pages[0].extract_text() or str(reader.metadata)
        print(str(path), '\n', text[:550].replace('\n', ' '), '\n')
    except Exception as exc:
        print(path, exc)
print('UNIQUE', len(seen))
