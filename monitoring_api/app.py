import os
import requests
from flask import Flask, jsonify, request

from . import sales_analysis_tools

# ADK API Server base URL (Docker service name in compose)
ADK_BASE_URL = os.getenv("ADK_API_BASE_URL", "http://localhost:8000")

# MUST match the ADK agent folder name
ADK_APP_NAME = "monitoring_agent"


def create_app() -> Flask:
    app = Flask(__name__)

    # -----------------------
    # Health & metadata
    # -----------------------

    @app.get("/health")
    def health():
        return {"status": "ok"}, 200

    @app.get("/schema")
    def schema():
        return jsonify(sales_analysis_tools.get_sales_schema())

    # -----------------------
    # Direct SQL / tool query
    # -----------------------

    @app.post("/query")
    def query():
        payload = request.get_json(silent=True) or {}
        question = (payload.get("question") or "").strip()
        sql = payload.get("sql")
        max_rows = payload.get("max_rows", 200)

        if not question and not sql:
            return jsonify({
                "status": "error",
                "error_message": "Provide a question or SQL to execute.",
            }), 400

        if not isinstance(max_rows, int) or max_rows <= 0:
            return jsonify({
                "status": "error",
                "error_message": "max_rows must be a positive integer.",
            }), 400

        result = sales_analysis_tools.query_sales(
            question=question or "user-provided-sql",
            sql=sql,
            max_rows=max_rows,
        )

        status_code = 200 if result.get("status") != "error" else 400
        return jsonify(result), status_code

    # -----------------------
    # Agent invocation
    # -----------------------

    @app.post("/invoke-agent")
    def invoke_agent():
        """
        Invoke ADK agent via ADK API Server (/run).
        ADK owns sessions, memory, tools, and execution.
        """
        data = request.get_json(silent=True) or {}

        if "query" not in data:
            return jsonify({"error": "Missing 'query' field"}), 400

        user_id = data.get("user_id", "anonymous")
        session_id = data.get("session_id", "default-session")
        user_query = data["query"]

        try:
            # 1️⃣ Ensure session exists (idempotent)
            session_url = (
                f"{ADK_BASE_URL}/apps/{ADK_APP_NAME}"
                f"/users/{user_id}/sessions/{session_id}"
            )

            requests.post(session_url, json={}, timeout=10)

            # 2️⃣ Run agent
            run_payload = {
                "appName": ADK_APP_NAME,
                "userId": user_id,
                "sessionId": session_id,
                "newMessage": {
                    "role": "user",
                    "parts": [{"text": user_query}],
                },
            }

            resp = requests.post(
                f"{ADK_BASE_URL}/run",
                json=run_payload,
                timeout=60,
            )

            resp.raise_for_status()
            events = resp.json()

            # 3️⃣ Extract final model response (last model message)
            final_answer = None
            for event in reversed(events):
                content = event.get("content", {})
                if content.get("role") == "model":
                    parts = content.get("parts", [])
                    if parts and "text" in parts[0]:
                        final_answer = parts[0]["text"]
                        break

            return jsonify({
                "query": user_query,
                "answer": final_answer,
                "events": events,  # keep for debugging / observability
            })

        except requests.RequestException as e:
            return jsonify({"error": str(e)}), 500

    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app = create_app()
    app.run(host="0.0.0.0", port=port)
