"""Публикация Shorts в YouTube (YouTube Data API v3).

В отличие от Threads/Instagram, YouTube не умеет забирать видео по URL —
ролик нужно скачать и залить резюмируемой загрузкой самим. Отдельный
модуль по той же причине, что и instagram.py: своя схема токена (Google
OAuth, access+refresh) и свой протокол публикации, не пересекающиеся с
остальным.
"""
import requests

UPLOAD_URL = "https://www.googleapis.com/upload/youtube/v3/videos"
TOKEN_URL = "https://oauth2.googleapis.com/token"

REQUEST_TIMEOUT_SEC = 30
DOWNLOAD_TIMEOUT_SEC = 60
UPLOAD_TIMEOUT_SEC = 300
MAX_TITLE_LEN = 100

# Продлеваем заранее, а не когда токен вот-вот протухнет — access_token и
# так живёт всего час, но храним такой же запас в днях, как у Instagram,
# чтобы обе площадки продлевались по одной и той же логике в scheduler.py.
REFRESH_WHEN_DAYS_LEFT = 1


def _error_text(response: requests.Response) -> str:
    try:
        error = response.json().get("error", {})
    except ValueError:
        return f"HTTP {response.status_code}: {response.text[:200]}"
    return error.get("message") or f"HTTP {response.status_code}"


def upload_short(access_token: str, video_url: str, title: str, description: str, log) -> tuple[str | None, str | None]:
    """Скачать видео по URL и залить как YouTube Short. Возвращает (video_id, None) либо (None, ошибка)."""
    try:
        video = requests.get(video_url, timeout=DOWNLOAD_TIMEOUT_SEC, stream=True)
        video.raise_for_status()
        video_bytes = video.content
    except requests.RequestException as e:
        return None, f"YouTube: не удалось скачать видео: {e}"

    metadata = {
        "snippet": {
            "title": title[:MAX_TITLE_LEN],
            "description": description,
        },
        "status": {
            "privacyStatus": "public",
            "selfDeclaredMadeForKids": False,
        },
    }

    try:
        init = requests.post(
            UPLOAD_URL,
            params={"uploadType": "resumable", "part": "snippet,status"},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Type": "video/mp4",
                "X-Upload-Content-Length": str(len(video_bytes)),
            },
            json=metadata,
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, f"YouTube: сеть недоступна при инициализации загрузки: {e}"
    if not init.ok:
        return None, f"YouTube: не удалось начать загрузку — {_error_text(init)}"

    upload_session_url = init.headers.get("Location")
    if not upload_session_url:
        return None, "YouTube: ответ без адреса загрузки"

    try:
        log.info(f"⏳ YouTube: заливаю {len(video_bytes) // 1024} КБ...")
        upload = requests.put(
            upload_session_url,
            data=video_bytes,
            headers={"Content-Type": "video/mp4"},
            timeout=UPLOAD_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, f"YouTube: сеть недоступна при загрузке видео: {e}"
    if not upload.ok:
        return None, f"YouTube: загрузка не удалась — {_error_text(upload)}"

    video_id = upload.json().get("id")
    if not video_id:
        return None, "YouTube: ответ без id видео"
    return video_id, None


def refresh_token(refresh_token_value: str, client_id: str, client_secret: str) -> tuple[str | None, int | None, str | None]:
    """Обновить access_token. Возвращает (новый токен, сколько секунд жить, None) либо (None, None, ошибка).

    refresh_token у Google не меняется при обновлении — сохранять заново не нужно.
    """
    try:
        response = requests.post(
            TOKEN_URL,
            data={
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token_value,
                "grant_type": "refresh_token",
            },
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, None, f"YouTube: сеть недоступна при обновлении токена: {e}"
    if not response.ok:
        return None, None, f"YouTube: обновление токена не удалось — {_error_text(response)}"

    payload = response.json()
    new_token = payload.get("access_token")
    if not new_token:
        return None, None, "YouTube: обновление вернуло ответ без токена"
    return new_token, payload.get("expires_in"), None
