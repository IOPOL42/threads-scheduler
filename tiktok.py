"""Публикация видео в TikTok (Content Posting API, FILE_UPLOAD).

Не PULL_FROM_URL: тот способ требует подтвердить домен, откуда TikTok
скачивает видео, файлом в его корне или DNS-записью — а видео у нас лежит
на общем домене Supabase (`*.supabase.co`), которым мы не владеем и не
можем ни то, ни другое туда добавить. FILE_UPLOAD этого не требует —
планировщик сам скачивает ролик и заливает его байтами, как и для YouTube.

Пока приложение не прошло аудит на video.publish, TikTok сам не позволит
опубликовать ролик как полностью публичный — доступные privacy_level
нужно спросить у creator_info и использовать то, что разрешено (обычно
SELF_ONLY, "только автор"), это не ошибка интеграции.
"""
import time
import requests

API_BASE = "https://open.tiktokapis.com/v2"
REQUEST_TIMEOUT_SEC = 30
DOWNLOAD_TIMEOUT_SEC = 60
UPLOAD_TIMEOUT_SEC = 300
PUBLISH_TIMEOUT_SEC = 180
PUBLISH_POLL_SEC = 5
MAX_TITLE_LEN = 2200


def _error_text(response: requests.Response) -> str:
    try:
        error = response.json().get("error", {})
    except ValueError:
        return f"HTTP {response.status_code}: {response.text[:200]}"
    return error.get("message") or error.get("code") or f"HTTP {response.status_code}"


def _creator_privacy_level(access_token: str) -> tuple[str | None, str | None]:
    """Разрешённый приложением на сейчас уровень приватности публикации."""
    try:
        response = requests.post(
            f"{API_BASE}/post/publish/creator_info/query/",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, f"TikTok: сеть недоступна при запросе creator_info: {e}"
    if not response.ok:
        return None, f"TikTok: creator_info не удался — {_error_text(response)}"

    options = response.json().get("data", {}).get("privacy_level_options") or []
    if not options:
        return None, "TikTok: creator_info не вернул доступные уровни приватности"
    # Публичный уровень предпочтителен, но до прохождения аудита video.publish
    # его в списке не будет — тогда берём первый разрешённый (обычно SELF_ONLY).
    level = "PUBLIC_TO_EVERYONE" if "PUBLIC_TO_EVERYONE" in options else options[0]
    return level, None


def publish_video(access_token: str, video_url: str, caption: str, log) -> tuple[str | None, str | None]:
    """Скачать видео по URL и опубликовать в TikTok. Возвращает (publish_id, None) либо (None, ошибка)."""
    try:
        video = requests.get(video_url, timeout=DOWNLOAD_TIMEOUT_SEC, stream=True)
        video.raise_for_status()
        video_bytes = video.content
    except requests.RequestException as e:
        return None, f"TikTok: не удалось скачать видео: {e}"

    privacy_level, error = _creator_privacy_level(access_token)
    if error:
        return None, error
    if privacy_level != "PUBLIC_TO_EVERYONE":
        log.warning(f"⚠️ TikTok: приложение ещё не прошло аудит — ролик уйдёт с privacy_level={privacy_level}")

    video_size = len(video_bytes)
    try:
        init = requests.post(
            f"{API_BASE}/post/publish/video/init/",
            headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
            json={
                "post_info": {
                    "title": caption[:MAX_TITLE_LEN],
                    "privacy_level": privacy_level,
                    "disable_duet": False,
                    "disable_comment": False,
                    "disable_stitch": False,
                },
                "source_info": {
                    # Один чанк на весь файл — реальный чанкинг (обязателен только
                    # для файлов больше ~64 МБ) роликам такого размера не нужен.
                    "source": "FILE_UPLOAD",
                    "video_size": video_size,
                    "chunk_size": video_size,
                    "total_chunk_count": 1,
                },
            },
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, f"TikTok: сеть недоступна при инициализации публикации: {e}"
    if not init.ok:
        return None, f"TikTok: публикация не удалась — {_error_text(init)}"

    init_data = init.json().get("data", {})
    publish_id = init_data.get("publish_id")
    upload_url = init_data.get("upload_url")
    if not publish_id or not upload_url:
        return None, "TikTok: ответ без publish_id или upload_url"

    try:
        log.info(f"⏳ TikTok: заливаю {video_size // 1024} КБ...")
        upload = requests.put(
            upload_url,
            data=video_bytes,
            headers={
                "Content-Type": "video/mp4",
                "Content-Range": f"bytes 0-{video_size - 1}/{video_size}",
            },
            timeout=UPLOAD_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, f"TikTok: сеть недоступна при загрузке видео: {e}"
    if not upload.ok:
        return None, f"TikTok: загрузка не удалась — {_error_text(upload)}"

    deadline = time.monotonic() + PUBLISH_TIMEOUT_SEC
    while True:
        time.sleep(PUBLISH_POLL_SEC)
        try:
            status = requests.post(
                f"{API_BASE}/post/publish/status/fetch/",
                headers={"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"},
                json={"publish_id": publish_id},
                timeout=REQUEST_TIMEOUT_SEC,
            )
        except requests.RequestException as e:
            return None, f"TikTok: сеть недоступна при опросе статуса: {e}"
        if not status.ok:
            return None, f"TikTok: опрос статуса не удался — {_error_text(status)}"

        data = status.json().get("data", {})
        code = data.get("status")
        if code == "PUBLISH_COMPLETE":
            return publish_id, None
        if code == "FAILED":
            return None, f"TikTok: публикация провалилась — {data.get('fail_reason', 'без причины')}"
        if time.monotonic() > deadline:
            return None, f"TikTok: видео не опубликовалось за {PUBLISH_TIMEOUT_SEC}с (статус {code})"
        log.info(f"⏳ TikTok обрабатывает видео ({code})...")


def refresh_token(refresh_token_value: str, client_key: str, client_secret: str) -> tuple[str | None, str | None, int | None, str | None]:
    """Обновить токен. Возвращает (access_token, новый refresh_token, expires_in, None) либо (None, None, None, ошибка).

    В отличие от Google, TikTok выдаёт новый refresh_token при каждом обновлении —
    старый после этого перестаёт работать, поэтому его обязательно нужно сохранить.
    """
    try:
        response = requests.post(
            "https://open.tiktokapis.com/v2/oauth/token/",
            headers={"Content-Type": "application/x-www-form-urlencoded", "Cache-Control": "no-cache"},
            data={
                "client_key": client_key,
                "client_secret": client_secret,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token_value,
            },
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, None, None, f"TikTok: сеть недоступна при обновлении токена: {e}"
    if not response.ok:
        return None, None, None, f"TikTok: обновление токена не удалось — {_error_text(response)}"

    payload = response.json()
    new_token = payload.get("access_token")
    new_refresh = payload.get("refresh_token")
    if not new_token or not new_refresh:
        return None, None, None, "TikTok: обновление вернуло ответ без токена"
    return new_token, new_refresh, payload.get("expires_in"), None
