"""Lossless storage for large, replayed simulation artifacts."""
import gzip
import hashlib
import json
from pathlib import Path
import shutil


def open_artifact(artifact):
    encoding = artifact.get('encoding', 'identity')
    if encoding not in ('identity', 'gzip'):
        raise ValueError('unsupported artifact encoding')
    return (gzip.open if encoding == 'gzip' else open)(artifact['path'], 'rb')


def read_artifact(artifact):
    with open_artifact(artifact) as data:
        if hashlib.file_digest(data, 'sha256').hexdigest() != artifact['sha256']:
            raise ValueError('artifact hash mismatch')
    with open_artifact(artifact) as data:
        return json.load(data)


def packed_file(path):
    """Verify decoded bytes before returning a replacement; retain the original."""
    path = Path(path)
    with path.open('rb') as source:
        expected = hashlib.file_digest(source, 'sha256').hexdigest()
    destination = path.with_name(path.name + '.gz')
    temporary = destination.with_name(destination.name + '.tmp')
    if not destination.exists():
        with path.open('rb') as source, temporary.open('wb') as target:
            with gzip.GzipFile(filename='', mode='wb', fileobj=target, compresslevel=1, mtime=0) as compressed:
                shutil.copyfileobj(source, compressed, 1024 * 1024)
        temporary.replace(destination)
    result = {'path': str(destination.resolve()), 'encoding': 'gzip', 'sha256': expected,
              'decoded_bytes': path.stat().st_size}
    with open_artifact(result) as data:
        if hashlib.file_digest(data, 'sha256').hexdigest() != expected:
            raise ValueError(f'compression verification failed: {path}')
    return result


def write_json(path, value):
    temporary = path.with_name(path.name + '.tmp')
    temporary.write_text(json.dumps(value, separators=(',', ':')) + '\n')
    temporary.replace(path)


def archive_job(directory, scenario, adapter, profile, size, seed):
    """Archive a finished job, including replay/failure evidence; no active writers."""
    directory = Path(directory).resolve()
    pattern = f'{scenario}--*--{adapter}--{profile}--{size}--{seed}'
    index_path = directory / f'archive-{adapter}-{profile}-{size}-{seed}.json'
    index = json.loads(index_path.read_text()) if index_path.exists() else {'schema': 'kr-classic-archive/v1', 'files': {}}
    # Save each original streamed report before updating its artifact references.
    for report_path in sorted(directory.glob(pattern + '.json')):
        report = json.loads(report_path.read_text())
        if 'artifacts' not in report or all(a.get('encoding') == 'gzip' for a in report['artifacts'].values()):
            continue
        original = packed_file(report_path)
        index['files'].setdefault(report_path.name, original)
        previous_paths = []
        for key, artifact in report['artifacts'].items():
            if artifact.get('encoding') == 'gzip':
                continue
            source = Path(artifact['path']).resolve()
            if not source.resolve().is_relative_to(directory.resolve()):
                raise ValueError('refuse to archive evidence outside the job directory')
            packed = packed_file(source)
            if packed['sha256'] != artifact['sha256']:
                raise ValueError('original artifact hash mismatch')
            report['artifacts'][key] = packed
            index['files'][str(source.relative_to(directory))] = packed
            previous_paths.append(source)
        write_json(index_path, index)
        write_json(report_path, report)
        for source in previous_paths:
            source.unlink()
    # Replay and failed executions also remain reproducible, even without a
    # successful top-level report. Publish the restoration map before removal.
    for sidecars in sorted(directory.glob(pattern + '.*')):
        if not sidecars.is_dir():
            continue
        for source in sorted(sidecars.glob('*.json')):
            packed = packed_file(source)
            index['files'][str(source.relative_to(directory))] = packed
            write_json(index_path, index)
            source.unlink()
    return len(index['files'])
