"""Check authentication only; never polls updates or sends messages."""
import json
import os
from pathlib import Path
import urllib.error
import urllib.request
from dotenv import load_dotenv


def main():
    load_dotenv(Path(__file__).resolve().parent / '.env')
    token = os.getenv('BOT_TOKEN', '').strip()
    if not token:
        print('BOT_TOKEN is not set')
        return 1
    try:
        with urllib.request.urlopen('https://api.telegram.org/bot' + token + '/getMe', timeout=20) as r:
            data = json.load(r)
            print(json.dumps({'ok': data.get('ok'), 'username': data.get('result', {}).get('username')}, ensure_ascii=False))
            return 0 if data.get('ok') else 1
    except urllib.error.HTTPError as e:
        data = json.load(e)
        print(json.dumps({k: data.get(k) for k in ('ok', 'error_code', 'description')}, ensure_ascii=False))
        return 1
    except Exception as e:
        print('Connection check failed: ' + type(e).__name__)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
