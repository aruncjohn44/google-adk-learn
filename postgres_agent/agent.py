
from . import postgres_tools
from google.adk.agents import Agent
from dotenv import load_dotenv

load_dotenv()

root_agent = Agent(
    name="Postgresql_Agent",
    model="gemini-2.5-flash",
    description=(
        "Agent to answer questions about Postgresql data and models and execute"
        " SQL queries."
    ),
    instruction="""\
        You are a data science agent with access to local Postgresql database and query generation tool.
        You generate SQL queries to fetch data from the Postgresql database to answer user questions.
        Make use of those tools to answer the user's questions.
    """,
    tools=[
        postgres_tools.get_postgres_schema,
        postgres_tools.run_readonly_query,
        postgres_tools.query_postgres,
    ],
)
