"""Тесты планировщика без сети: python -m unittest -v

Запуск ничего не публикует — Threads и Workspace подменены.
"""
import os
import unittest
from unittest import mock

import requests

os.environ.pop("GITHUB_ACTIONS", None)
import scheduler as s  # noqa: E402


class RedactTest(unittest.TestCase):
    def test_token_in_url_and_known_secrets_are_hidden(self):
        s.add_secret("EAAGsecretTOKENvalue")
        text = s.redact("GET https://graph.threads.net/v1.0/me?fields=id&access_token=EAAB12&x=1 · EAAGsecretTOKENvalue")
        self.assertNotIn("EAAB12", text)
        self.assertNotIn("EAAGsecretTOKENvalue", text)
        self.assertIn("access_token=***", text)

    def test_log_records_are_redacted(self):
        s.add_secret("ANOTHERsecretTOKEN42")
        with self.assertLogs(s.log, level="ERROR") as logs:
            s.log.error("ошибка с ANOTHERsecretTOKEN42 и ?access_token=XYZ123")
        self.assertNotIn("ANOTHERsecretTOKEN42", logs.output[0])

    def test_safe_error_has_no_url(self):
        e = requests.ConnectionError("HTTPSConnectionPool: url /v1.0/me?access_token=EAAB")
        self.assertEqual(s._safe_error(e), "ConnectionError")


class ParseTest(unittest.TestCase):
    def test_parts_split_on_exclamation(self):
        self.assertEqual(s.parse_thread("a\n!b\n!\n!c"), ["a", "b", "c"])
        self.assertEqual(s.parse_thread("  один  "), ["один"])

    def test_spoilers(self):
        self.assertEqual(s.parse_spoilers("x ||y|| z"), ("x y z", [{"entity_type": "SPOILER", "offset": 2, "length": 1}]))


class PublishTest(unittest.TestCase):
    def test_lost_publish_response_is_not_retried(self):
        with mock.patch.object(s.requests, "post", side_effect=requests.Timeout("timeout")) as post:
            with self.assertRaises(s.AmbiguousPublish):
                s.publish_container("c1")
        self.assertEqual(post.call_count, 1)

    def test_partial_thread_resumes_after_published_parts(self):
        created = []
        with mock.patch.object(s, "create_container", side_effect=lambda text, reply_to_id=None, media_items=None: created.append((text, reply_to_id)) or f"c-{text}"), \
                mock.patch.object(s, "publish_container", side_effect=lambda cid: f"id-{cid}"), \
                mock.patch.object(s.time, "sleep"):
            ids, error = s.publish_parts(["a", "b", "c"], done_ids=["id-1", "id-2"])
        self.assertIsNone(error)
        self.assertEqual(ids, ["id-1", "id-2", "id-c-c"])
        self.assertEqual(created, [("c", "id-2")], "только третья часть, ответом на вторую")

    def test_failure_returns_parts_that_did_go_out(self):
        def publish(cid):
            if cid == "c-b":
                raise s.AmbiguousPublish("ответ потерялся")
            return f"id-{cid}"
        with mock.patch.object(s, "create_container", side_effect=lambda text, reply_to_id=None, media_items=None: f"c-{text}"), \
                mock.patch.object(s, "publish_container", side_effect=publish), \
                mock.patch.object(s.time, "sleep"):
            ids, error = s.publish_parts(["a", "b"])
        self.assertEqual(ids, ["id-c-a"])
        self.assertIn("Проверьте Threads", error)


if __name__ == "__main__":
    unittest.main()
