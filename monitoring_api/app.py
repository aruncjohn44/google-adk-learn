import os
import requests
from flask import Flask, render_template, request, jsonify

from . import sales_analysis_tools

# ADK API Server base URL (Docker service name in compose)
ADK_BASE_URL = os.getenv("ADK_API_BASE_URL", "http://localhost:8000")

# MUST match the ADK agent folder name
ADK_APP_NAME = "monitoring_agent"

def normalize_adk_response(adk_events: list):
    answer = None
    sql = None
    rows = None

    for event in adk_events:
        parts = event.get("content", {}).get("parts", [])

        for part in parts:
            # Final text answer
            if "text" in part:
                answer = part["text"]

            # Tool response (SQL execution)
            if "functionResponse" in part:
                fr = part["functionResponse"]["response"]
                if fr.get("status") == "success":
                    rows = fr.get("data")
                    sql = fr.get("sql")

    return {
        "answer": answer,
        "data": rows,
        "sql": sql,
    }



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

            normalized = normalize_adk_response(events)

            return jsonify({
                "query": user_query,
                **normalized
            })

        except requests.RequestException as e:
            return jsonify({"error": str(e)}), 500
        
    @app.get("/chat")
    def chat_page():
        # Renders the chat UI
        return render_template("chat.html")


    @app.post("/ask")
    def ask_agent():
        payload = request.get_json() or {}
        query = payload.get("query", "").strip()

        if not query:
            return jsonify({"error": "Empty query"}), 400

        user_id = "web_user"
        session_id = "web_session"

        # Ensure session exists (idempotent, like /invoke-agent)
        session_url = (
            f"{ADK_BASE_URL}/apps/{ADK_APP_NAME}"
            f"/users/{user_id}/sessions/{session_id}"
        )
        try:
            requests.post(session_url, json={}, timeout=10)
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to create ADK session: {e}"}), 502

        adk_payload = {
            "appName": ADK_APP_NAME,
            "userId": user_id,
            "sessionId": session_id,
            "newMessage": {
                "role": "user",
                "parts": [{"text": query}]
            }
        }

        try:
            resp = requests.post(
                f"{ADK_BASE_URL}/run",
                json=adk_payload,
                timeout=60
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            return jsonify({"error": f"Failed to contact ADK agent: {e}"}), 502

        try:
            events = resp.json()
        except Exception:
            return jsonify({"error": "ADK agent did not return valid JSON.", "raw": resp.text}), 502

        if not isinstance(events, (list, dict)):
            return jsonify({"error": "Unexpected response format from ADK agent.", "raw": events}), 502

        try:
            normalized = normalize_adk_response(events)
        except Exception as e:
            return jsonify({"error": f"Failed to process ADK agent response: {e}", "raw": events}), 500

        return jsonify(normalized)

    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app = create_app()
    app.run(host="0.0.0.0", port=port)
