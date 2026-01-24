from dotenv import load_dotenv
from google.adk.agents import Agent

from . import sales_analysis_tools

load_dotenv()


root_agent = Agent(
    name="Sales_Analysis_Agent",
    model="gemini-2.5-flash",
    description=(
        "Agent to answer questions about chocolate sales data, generate read-only"
        " SQL, and return results ready for visualization."
    ),
    instruction="""\
        You are a sales analytics agent with access to a PostgreSQL database of sales data.
        Understand natural language questions, generate safe read-only SQL queries, and
        return results in a visualization-ready format.
    """,
    tools=[
        sales_analysis_tools.get_sales_schema,
        sales_analysis_tools.run_readonly_query,
        sales_analysis_tools.query_sales,
    ],
)
