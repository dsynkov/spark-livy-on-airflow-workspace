import json
import random
import time
from typing import Dict, Any

from requests import Session

from livyondocker.utils import init_logger


class LivyAPI:
    def __init__(self, host):
        self.host = host
        self.session = Session()
        self.session.headers = {"Content-Type": "application/json"}
        self.current_session_url = self.current_session_id = None
        self.logger = init_logger()

    def start_session(self, kind: str) -> None:

        r = self.session.post(
            url=f"{self.host}/sessions", data=json.dumps({"kind": kind})
        )

        response = r.json()

        self.logger.info(f"{kind.upper()} session instantiated: {response}.")

        self.current_session_id = response["id"]
        self.current_session_url = self.host + r.headers["Location"]

        self.__poll_for_target_state(
            f"{self.host}/sessions/{self.current_session_id}", "idle"
        )

        self.logger.info(
            f"Session reached idle state; URL set to '{self.current_session_url}'."
        )

    def __poll_for_target_state(self, url, state: str) -> None:
        actual_state = None
        while actual_state != state:
            r = self.session.get(url)
            response = r.json()
            self.logger.info(response)
            actual_state = response["state"]
            if actual_state in ["dead", "killed", "error"]:
                self.logger.info("Batch or session has died.")
                return
            time.sleep(random.randint(1, 3))

    def check_session_availability(self, kind):

        if not self.current_session_url:
            self.logger.info("No current session found. Retrieving all sessions...")
            r = self.session.get(f"{self.host}/sessions")
            sessions = r.json()["sessions"]
            self.logger.info(f"Found {len(sessions)} session(s).")
            if not sessions:
                self.start_session(kind)
            else:
                self.current_session_id = sessions[-1]["id"]
                self.current_session_url = (
                    f"{self.host}/sessions/{self.current_session_id}"
                )

    def execute_statement(self, statement):

        r = self.session.post(
            url=f"{self.current_session_url}/statements",
            data=json.dumps({"code": statement}),
        )

        self.logger.info(
            f"Executed statement '{statement}' on session '{self.current_session_id}'."
        )
        statement_url = self.host + r.headers["Location"]
        self.__poll_for_target_state(statement_url, "available")

        r = self.session.get(statement_url)

        response = r.json()
        output = response["output"]["data"]["text/plain"]
        self.logger.info(f"Statement output is:\n\t{output}")

    def execute_batch(self, data: Dict[str, Any]) -> None:

        r = self.session.post(
            url=f"{self.host}/batches",
            data=json.dumps(data),
        )

        r.raise_for_status()

        response = r.json()
        batch_id = response["id"]

        self.logger.info(f"Executed file with batch ID {batch_id}: {response}")

        self.__poll_for_target_state(f"{self.host}/batches/{batch_id}", "success")
