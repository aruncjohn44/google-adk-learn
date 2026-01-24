import os

from flask import Flask, jsonify, request

from . import sales_analysis_tools


def create_app() -> Flask:
    app = Flask(__name__)

    @app.get("/health")
    def health() -> tuple[dict, int]:
        return {"status": "ok"}, 200

    @app.get("/schema")
    def schema() -> tuple[dict, int]:
        return jsonify(sales_analysis_tools.get_sales_schema())

    @app.post("/query")
    def query() -> tuple[dict, int]:
        payload = request.get_json(silent=True) or {}
        question = (payload.get("question") or "").strip()
        sql = payload.get("sql")
        max_rows = payload.get("max_rows", 200)

        if not question and not sql:
            return (
                jsonify(
                    {
                        "status": "error",
                        "error_message": "Provide a question or SQL to execute.",
                    }
                ),
                400,
            )

        if not isinstance(max_rows, int) or max_rows <= 0:
            return (
                jsonify(
                    {
                        "status": "error",
                        "error_message": "max_rows must be a positive integer.",
                    }
                ),
                400,
            )

        result = sales_analysis_tools.query_sales(
            question=question or "user-provided-sql",
            sql=sql,
            max_rows=max_rows,
        )
        status_code = 200 if result.get("status") != "error" else 400
        return jsonify(result), status_code

    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    app = create_app()
    app.run(host="0.0.0.0", port=port)
