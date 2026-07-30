from pathlib import Path
import os
import shutil
import tempfile
import yaml

path = Path('/root/.hermes/profiles/worker/config.yaml')
backup = path.with_name('config.yaml.bak_tokenrouter_20260721T0859Z')
if not backup.exists():
    shutil.copy2(path, backup)

data = yaml.safe_load(path.read_text())

def parse_embedded(value, expected):
    if isinstance(value, expected):
        return value
    parsed = yaml.safe_load(value) if isinstance(value, str) else None
    if not isinstance(parsed, expected):
        raise TypeError(f'expected {expected.__name__}, got {type(parsed).__name__}')
    return parsed

thresholds = parse_embedded(data['compression']['thresholds'], dict)
thresholds['tokenr/z-ai/glm-5.2'] = 0.5
data['compression']['thresholds'] = thresholds

models = parse_embedded(data['providers']['omniroute']['models'], list)
if 'tokenr/z-ai/glm-5.2' not in models:
    models.append('tokenr/z-ai/glm-5.2')
data['providers']['omniroute']['models'] = models

custom = parse_embedded(data['custom_providers'], list)
for provider in custom:
    if provider.get('name') == 'omniroute':
        provider_models = provider.setdefault('models', [])
        if 'tokenr/z-ai/glm-5.2' not in provider_models:
            provider_models.append('tokenr/z-ai/glm-5.2')
data['custom_providers'] = custom

aliases = parse_embedded(data['model_aliases'], dict)
for alias in ('glm-5.2-tokenrouter', 'glm 5.2 tokenrouter'):
    aliases[alias] = {
        'model': 'tokenr/z-ai/glm-5.2',
        'provider': 'omniroute',
        'base_url': 'http://127.0.0.1:20128/v1',
    }
data['model_aliases'] = aliases

fd, tmp = tempfile.mkstemp(prefix=path.name + '.', suffix='.tmp', dir=path.parent)
try:
    with os.fdopen(fd, 'w') as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)
        f.flush()
        os.fsync(f.fileno())
    os.chmod(tmp, path.stat().st_mode)
    os.replace(tmp, path)
finally:
    if os.path.exists(tmp):
        os.unlink(tmp)

print(f'fixed={path}')
print(f'backup={backup}')
