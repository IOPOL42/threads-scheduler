import os
import re
import time
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
import logging

_SAFE_ID_RE = re.compile(r'^[a-zA-Z0-9_\-]{1,128}$')

# ─── Логирование ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)


# ─── Настройки из окружения ──────────────────────────────────────────────────
THREADS_USER_ID   = os.environ.get("THREADS_USER_ID", "")
THREADS_TOKEN     = os.environ.get("THREADS_TOKEN", "")
WORKSPACE_API_URL = os.environ.get("WORKSPACE_API_URL", "")
WORKSPACE_API_KEY = os.environ.get("WORKSPACE_API_KEY", "")


# ─── Timezone ────────────────────────────────────────────────────────────────
TZ = ZoneInfo("Asia/Ho_Chi_Minh")


# ─── Threads API ─────────────────────────────────────────────────────────────
API_BASE            = "https://graph.threads.net/v1.0"
API_TIMEOUT_SEC     = 15
WAIT_BEFORE_PUBLISH = 3
WAIT_BETWEEN_PARTS  = 2
MAX_POST_LEN        = 500


# ─── Retry ───────────────────────────────────────────────────────────────────
RETRY_MAX_ATTEMPTS  = 5
RETRY_BASE_DELAY    = 5
RETRY_BACKOFF       = 3


# ═════════════════════════════════════════════════════════════════════════════
#  Валидация
# ═════════════════════════════════════════════════════════════════════════════
def validate_config():
    missing = [v for v, val in [
        ("THREADS_USER_ID",   THREADS_USER_ID),
        ("THREADS_TOKEN",     THREADS_TOKEN),
        ("WORKSPACE_API_URL", WORKSPACE_API_URL),
        ("WORKSPACE_API_KEY", WORKSPACE_API_KEY),
    ] if not val]
    if missing:
        log.error(f"❌ Не заданы переменные окружения: {', '.join(missing)}")
        raise SystemExit(1)


def validate_threads_token():
    log.info("🔑 Проверяю токен Threads...")
    r = requests.get(
        f"{API_BASE}/me",
        params={"access_token": THREADS_TOKEN, "fields": "id,username"},
        timeout=API_TIMEOUT_SEC,
    )
    if r.status_code == 200:
        data = r.json()
        log.info(f"✅ Threads токен валиден. Аккаунт: @{data.get('username')} (id={data.get('id')})")
    else:
        log.error(f"❌ Threads токен невалиден ({r.status_code}): {r.text[:200]}")
        raise SystemExit(1)


# ═════════════════════════════════════════════════════════════════════════════
#  HTTP с retry
# ═════════════════════════════════════════════════════════════════════════════
def request_with_retry(method: str, url: str, **kwargs) -> requests.Response:
    last_exc = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            r = requests.request(method, url, **kwargs)
            if r.status_code == 429 or 500 <= r.status_code < 600:
                delay = RETRY_BASE_DELAY * (RETRY_BACKOFF ** (attempt - 1))
                log.warning(f"⏳ HTTP {r.status_code} (попытка {attempt}/{RETRY_MAX_ATTEMPTS}). Жду {delay}с...")
                time.sleep(delay)
                continue
            r.raise_for_status()
            return r
        except (requests.ConnectionError, requests.Timeout) as e:
            last_exc = e
            delay = RETRY_BASE_DELAY * (RETRY_BACKOFF ** (attempt - 1))
            log.warning(f"⏳ Сетевая ошибка: {e} (попытка {attempt}/{RETRY_MAX_ATTEMPTS}). Жду {delay}с...")
            time.sleep(delay)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"API недоступен после {RETRY_MAX_ATTEMPTS} попыток")


# ═════════════════════════════════════════════════════════════════════════════
#  Workspace API
# ═════════════════════════════════════════════════════════════════════════════
def load_ready_posts() -> list[dict]:
    """Получить посты с queued_at (распределённые веб-приложением)."""
    r = request_with_retry(
        "GET",
        f"{WORKSPACE_API_URL}/api/v1/posts",
        params={"status": "ready", "platform": "threads"},
        headers={"X-API-Key": WORKSPACE_API_KEY},
        timeout=API_TIMEOUT_SEC,
    )
    return r.json().get("posts", [])


def delete_published_posts(now: datetime):
    """Удалить посты, опубликованные до сегодняшней полуночи."""
    try:
        today_midnight = datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=TZ)
        r = request_with_retry(
            "DELETE",
            f"{WORKSPACE_API_URL}/api/v1/posts",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            params={"before": today_midnight.isoformat()},
            timeout=API_TIMEOUT_SEC,
        )
        deleted = r.json().get("deleted", 0)
        log.info(f"🗑️ Очистка: удалено {deleted} постов опубликованных до {today_midnight.strftime('%d.%m')}")
    except Exception as e:
        log.error(f"❌ Ошибка очистки опубликованных постов: {e}")


def mark_published(post_id: str):
    """Пометить пост как опубликованный."""
    try:
        request_with_retry(
            "PATCH",
            f"{WORKSPACE_API_URL}/api/v1/posts/{post_id}",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            json={"status": "published", "published_at": datetime.now(tz=TZ).isoformat(), "queued_at": None},
            timeout=API_TIMEOUT_SEC,
        )
    except Exception as e:
        log.error(f"❌ Не удалось обновить статус поста {post_id}: {e}")


def clear_queued(post_id: str, error: str | None = None):
    """Сбросить queued_at у поста при ошибке публикации. Записывает причину ошибки."""
    payload: dict = {"queued_at": None}
    if error:
        payload["publish_error"] = error[:500]
    try:
        request_with_retry(
            "PATCH",
            f"{WORKSPACE_API_URL}/api/v1/posts/{post_id}",
            headers={"X-API-Key": WORKSPACE_API_KEY},
            json=payload,
            timeout=API_TIMEOUT_SEC,
        )
    except Exception as e:
        log.error(f"❌ Не удалось сбросить queued_at у {post_id}: {e}")


# ═════════════════════════════════════════════════════════════════════════════
#  Парсинг контента
# ═════════════════════════════════════════════════════════════════════════════
def parse_spoilers(content: str) -> tuple[str, list[dict] | None]:
    """Конвертирует ||спойлер|| → (plain_text, text_entities)."""
    entities = []
    plain = ""
    offset = 0
    for m in re.finditer(r'\|\|([^|]+)\|\|', content):
        plain += content[offset:m.start()]
        entities.append({
            "entity_type": "SPOILER",
            "offset": len(plain),
            "length": len(m.group(1)),
        })
        plain += m.group(1)
        offset = m.end()
    plain += content[offset:]
    return plain, (entities if entities else None)


def parse_thread(text: str) -> list[str]:
    """Разбить текст на части ветки по строкам начинающимся с '!'."""
    lines = text.split('\n')
    parts = []
    current = []
    for line in lines:
        if line.startswith('!'):
            if current:
                parts.append('\n'.join(current).strip())
            current = [line[1:].strip()]
        else:
            current.append(line)
    if current:
        parts.append('\n'.join(current).strip())
    parts = [p for p in parts if p]
    return parts if parts else [text.strip()]


# ═════════════════════════════════════════════════════════════════════════════
#  Threads API
# ═════════════════════════════════════════════════════════════════════════════
def is_video_url(url: str) -> bool:
    return url.lower().split("?")[0].endswith((".mp4", ".mov", ".webm"))


def create_media_container(url: str) -> str | None:
    """Создать дочерний контейнер для карусели (одно фото/видео)."""
    video = is_video_url(url)
    body = {
        "media_type": "VIDEO" if video else "IMAGE",
        ("video_url" if video else "image_url"): url,
        "is_carousel_item": "true",
    }
    r = request_with_retry(
        "POST",
        f"{API_BASE}/{THREADS_USER_ID}/threads",
        params={"access_token": THREADS_TOKEN},
        data=body,
        timeout=API_TIMEOUT_SEC,
    )
    cid = r.json().get("id")
    if cid and video:
        log.info(f"🎬 Видео-контейнер {cid} создан, жду обработки...")
        if not wait_for_container_ready(cid):
            return None
    return cid


def create_container(
    text: str,
    reply_to_id: str = None,
    media_urls: list[str] = None,
) -> str | None:
    plain_text, entities = parse_spoilers(text)

    if media_urls and len(media_urls) == 1:
        url = media_urls[0]
        video = is_video_url(url)
        params = {
            "media_type": "VIDEO" if video else "IMAGE",
            ("video_url" if video else "image_url"): url,
            "text": plain_text,
            "access_token": THREADS_TOKEN,
        }
    elif media_urls and len(media_urls) > 1:
        child_ids = [create_media_container(u) for u in media_urls]
        if None in child_ids:
            log.error("❌ Не удалось создать один из медиа-контейнеров карусели")
            return None
        log.info(f"⏳ Ожидаем готовности {len(child_ids)} медиаконтейнеров...")
        time.sleep(8)
        params = {
            "media_type": "CAROUSEL",
            "children": ",".join(child_ids),
            "text": plain_text,
            "access_token": THREADS_TOKEN,
        }
    else:
        params = {
            "media_type": "TEXT",
            "text": plain_text,
            "access_token": THREADS_TOKEN,
        }

    if entities:
        params["text_entities"] = entities  # dict, не строка — JSON-тело не кодирует # как %23
    if reply_to_id:
        params["reply_to_id"] = reply_to_id

    # Явно задаём topic_tag из первого #хэштега, чтобы Threads не вырезал # из текста
    topic_match = re.search(r'#(\w+)', plain_text)
    if topic_match:
        params["topic_tag"] = topic_match.group(1).lower()

    # access_token идёт в URL, тело — JSON (иначе form-encoding кодирует # → %23 и Threads его обрезает)
    token = params.pop("access_token")
    hashtags_count = len(re.findall(r'#\w+', plain_text))
    log.info(f"📤 Текст: {len(plain_text)} симв., хэштегов: {hashtags_count}, topic_tag: {params.get('topic_tag') or 'нет'}, entities: {'да' if params.get('text_entities') else 'нет'}")
    r = request_with_retry(
        "POST",
        f"{API_BASE}/{THREADS_USER_ID}/threads",
        params={"access_token": token},
        json=params,
        timeout=API_TIMEOUT_SEC,
    )
    cid = r.json().get("id")
    # Для одиночного видео — ждём обработки перед публикацией
    if cid and media_urls and len(media_urls) == 1 and is_video_url(media_urls[0]):
        log.info(f"🎬 Видео-контейнер {cid} создан, жду обработки...")
        if not wait_for_container_ready(cid):
            return None
    return cid


def wait_for_container_ready(container_id: str, timeout_sec: int = 180, poll_sec: int = 5) -> bool:
    """Ждёт пока Threads обработает медиа-контейнер (нужно для видео)."""
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        try:
            r = request_with_retry(
                "GET",
                f"{API_BASE}/{container_id}",
                params={"fields": "status,error_message", "access_token": THREADS_TOKEN},
                timeout=API_TIMEOUT_SEC,
            )
            data = r.json()
            status = data.get("status")
            log.info(f"⏳ Контейнер {container_id}: статус = {status}")
            if status == "FINISHED":
                return True
            if status in ("ERROR", "EXPIRED"):
                log.error(f"❌ Контейнер {container_id}: {status} — {data.get('error_message', '')}")
                return False
        except Exception as e:
            log.warning(f"⚠️ Опрос статуса контейнера: {e}")
        time.sleep(poll_sec)
    log.error(f"❌ Контейнер {container_id} не готов за {timeout_sec}с")
    return False


def publish_container(container_id: str) -> str | None:
    r = request_with_retry(
        "POST",
        f"{API_BASE}/{THREADS_USER_ID}/threads_publish",
        params={"creation_id": container_id, "access_token": THREADS_TOKEN},
        timeout=API_TIMEOUT_SEC,
    )
    return r.json().get("id")


def publish_parts(parts: list[str], media: list[dict] = None) -> tuple[list[str] | None, str | None]:
    """
    Опубликовать одну или несколько частей ветки.
    `media` — список {"url": str, "branch": int}, branch — номер ветки (1-indexed,
    как в UI). Каждой части достаются только медиа с её собственным branch.
    Возвращает (thread_ids, error_message). error_message = None при успехе.
    """
    try:
        post_ids = []
        prev_id = None
        for i, text in enumerate(parts):
            branch_no = i + 1
            part_media = [m["url"] for m in (media or []) if m.get("branch") == branch_no] or None
            container_id = create_container(text, reply_to_id=prev_id, media_urls=part_media)
            if not container_id:
                return None, "Threads API не вернул ID контейнера"
            time.sleep(WAIT_BEFORE_PUBLISH)
            post_id = publish_container(container_id)
            if not post_id:
                return None, "Threads API не вернул ID публикации"
            log.info(f"✅ Часть {i+1}/{len(parts)} опубликована! ID: {post_id}")
            post_ids.append(post_id)
            prev_id = post_id
            if i < len(parts) - 1:
                time.sleep(WAIT_BETWEEN_PARTS)
        return post_ids, None
    except requests.HTTPError as e:
        try:
            err_data = e.response.json().get("error", {})
            msg = err_data.get("message") or str(e)
            code = err_data.get("code")
            if code:
                msg = f"[{code}] {msg}"
        except Exception:
            msg = str(e)
        log.error(f"❌ Ошибка публикации (HTTP): {msg}")
        return None, msg
    except (requests.RequestException, RuntimeError) as e:
        log.error(f"❌ Ошибка публикации: {e}")
        return None, str(e)


# ═════════════════════════════════════════════════════════════════════════════
#  Главный цикл
# ═════════════════════════════════════════════════════════════════════════════
def run():
    log.info("🚀 Threads Scheduler: запуск")

    validate_config()
    validate_threads_token()

    now = datetime.now(tz=TZ)

    # Ежедневная очистка опубликованных постов — в 18:xx
    if now.hour == 18:
        delete_published_posts(now)

    posts = load_ready_posts()
    published = 0
    for post in posts:
        post_id = post.get("id")
        try:
            queued_at_str = post.get("queued_at")
            if not queued_at_str:
                continue
            queued_at = datetime.fromisoformat(queued_at_str.replace("Z", "+00:00"))
            if queued_at.tzinfo is None:
                queued_at = queued_at.replace(tzinfo=TZ)
            if now < queued_at:
                continue  # ещё рано

            if not _SAFE_ID_RE.match(str(post_id)):
                log.error(f"❌ Небезопасный post_id: {str(post_id)[:40]!r} — пропускаем")
                continue

            parts = parse_thread(post.get("content") or "")
            media = post.get("media") or []

            log.info(f"🕐 Публикуем {post_id[:8]}... ({len(parts)} частей, {queued_at.astimezone(TZ).strftime('%H:%M')})")

            too_long = [i+1 for i, t in enumerate(parts) if len(t) > MAX_POST_LEN]
            if too_long:
                max_len = max(len(t) for t in parts)
                msg = f"Слишком длинная часть {too_long} ({max_len} симв., максимум {MAX_POST_LEN})"
                log.warning(f"⚠️ Пост {post_id[:8]}...: {msg} — пропускаем")
                clear_queued(post_id, error=msg)
                continue

            # Claim the slot *before* calling the Threads API: if we crash or
            # the network dies anywhere between a successful publish and the
            # mark_published() call below, queued_at is already cleared here,
            # so the next run's query (status=ready AND queued_at IS NOT NULL)
            # will never pick this post up again and re-publish it.
            clear_queued(post_id)

            threads_ids, pub_error = publish_parts(parts, media=media or None)
            if threads_ids:
                mark_published(post_id)
                log.info(f"✅ Пост {post_id[:8]}... опубликован")
                published += 1
            else:
                log.error(f"❌ Пост {post_id[:8]}...: {pub_error or 'ошибка публикации'}")
                clear_queued(post_id, error=pub_error)
        except Exception as e:
            log.exception(f"❌ Непредвиденная ошибка при обработке поста {str(post_id)[:8]}...: {e}")
            try:
                clear_queued(post_id, error=str(e))
            except Exception:
                pass

    log.info(f"🏁 Готово. Опубликовано: {published}")


if __name__ == "__main__":
    run()
