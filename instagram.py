"""Публикация рилсов в Instagram (Instagram API with Instagram Login).

Отдельный модуль, а не часть scheduler.py: у Instagram своя трёхшаговая
схема публикации (контейнер → ожидание обработки видео → публикация) и своё
продление токена, и всё это не пересекается с логикой Threads.
"""
import time
import requests

API_BASE = "https://graph.instagram.com"
API_VERSION = "v25.0"

REQUEST_TIMEOUT_SEC = 30
# Instagram обрабатывает видео у себя; на рилс уходит от десятков секунд до
# пары минут. Ограничение держим заметно ниже таймаута job'а GitHub Actions
# (10 минут), иначе один медленный рилс съест окно публикации остальных.
# Незавершённый контейнер безопасен: без media_publish он ничего не
# публикует и протухает сам через 24 часа, а повтор просто создаст новый.
PROCESSING_TIMEOUT_SEC = 180
PROCESSING_POLL_SEC = 5

# Продлеваем заранее, а не в последний день: токен живёт 60 дней, и при
# окне публикации в несколько дней подряд без запусков легко проскочить
# момент. Продлевать можно только токен старше 24 часов.
REFRESH_WHEN_DAYS_LEFT = 10


def _error_text(response: requests.Response) -> str:
    """Вытащить осмысленное сообщение из ответа Instagram."""
    try:
        error = response.json().get("error", {})
    except ValueError:
        return f"HTTP {response.status_code}: {response.text[:200]}"
    parts = [error.get("message"), error.get("error_user_msg")]
    detail = " / ".join(p for p in parts if p)
    code = error.get("code")
    subcode = error.get("error_subcode")
    suffix = f" (code {code}{f'/{subcode}' if subcode else ''})" if code else ""
    return (detail or f"HTTP {response.status_code}") + suffix


def publish_reel(account_id: str, token: str, video_url: str, caption: str, log) -> tuple[str | None, str | None]:
    """Опубликовать рилс. Возвращает (media_id, None) либо (None, ошибка)."""
    try:
        container = requests.post(
            f"{API_BASE}/{API_VERSION}/{account_id}/media",
            data={
                "media_type": "REELS",
                "video_url": video_url,
                "caption": caption,
                "access_token": token,
            },
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, f"IG: сеть недоступна при создании контейнера: {e}"
    if not container.ok:
        return None, f"IG: не удалось создать контейнер — {_error_text(container)}"

    container_id = container.json().get("id")
    if not container_id:
        return None, "IG: ответ без id контейнера"

    deadline = time.monotonic() + PROCESSING_TIMEOUT_SEC
    while True:
        time.sleep(PROCESSING_POLL_SEC)
        try:
            status = requests.get(
                f"{API_BASE}/{API_VERSION}/{container_id}",
                params={"fields": "status_code", "access_token": token},
                timeout=REQUEST_TIMEOUT_SEC,
            )
        except requests.RequestException as e:
            return None, f"IG: сеть недоступна при опросе контейнера: {e}"
        if not status.ok:
            return None, f"IG: опрос контейнера не удался — {_error_text(status)}"

        code = status.json().get("status_code")
        if code == "FINISHED":
            break
        if code in ("ERROR", "EXPIRED"):
            return None, f"IG: обработка видео завершилась статусом {code}"
        if time.monotonic() > deadline:
            return None, f"IG: видео не обработалось за {PROCESSING_TIMEOUT_SEC}с (статус {code})"
        log.info(f"⏳ IG обрабатывает видео ({code})...")

    try:
        published = requests.post(
            f"{API_BASE}/{API_VERSION}/{account_id}/media_publish",
            data={"creation_id": container_id, "access_token": token},
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        # Контейнер обработан, но публикация не подтверждена. Повтор создаст
        # новый контейнер — дубль в ленте возможен, поэтому зовущая сторона
        # помечает слот как проваленный, а не молча уходит в ретрай.
        return None, f"IG: сеть недоступна при публикации контейнера: {e}"
    if not published.ok:
        return None, f"IG: публикация не удалась — {_error_text(published)}"

    media_id = published.json().get("id")
    if not media_id:
        return None, "IG: ответ без id публикации"
    return media_id, None


def refresh_token(token: str) -> tuple[str | None, int | None, str | None]:
    """Продлить long-lived токен ещё на 60 дней.

    Возвращает (новый токен, сколько секунд жить, None) либо (None, None, ошибка).
    """
    try:
        response = requests.get(
            f"{API_BASE}/refresh_access_token",
            params={"grant_type": "ig_refresh_token", "access_token": token},
            timeout=REQUEST_TIMEOUT_SEC,
        )
    except requests.RequestException as e:
        return None, None, f"IG: сеть недоступна при продлении токена: {e}"
    if not response.ok:
        return None, None, f"IG: продление токена не удалось — {_error_text(response)}"

    payload = response.json()
    new_token = payload.get("access_token")
    if not new_token:
        return None, None, "IG: продление вернуло ответ без токена"
    return new_token, payload.get("expires_in"), None
