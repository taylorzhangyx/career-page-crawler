"""
The working code for the simple agent tool calling example using langgraph and bkng ml agentic package
Reference code: https://gitlab.com/booking-com/core/agentic/bkng-agentic/-/blob/main/pkg/agentic-lc/bkng/ml/agentic/lc/model.py?ref_type=heads
"""
# Step 1: Define tools and model

from langchain.tools import tool
from bkng.ml.agentic.lc import ChatGenAIGatewayModel
from bkng.mlregistry.client.types import Application, Asset
from bkng.mlregistry.client.types import AssetType
from bkng.ml.rs.client import GenAIClient, ServiceInfo, Service

MODEL_MAPPINGS = {
    # Existing models from your snippet
    'gpt4o': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4o'),
    'claude3.5': Asset(asset_type=AssetType.STATIC_MODEL, name='claude_3_5_sonnet'),
    'gpt4turbo': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4-turbo'),
    'gpt-4_5': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4-5'),
    'gpt4omini': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4o-mini'),
    'claude_instant': Asset(asset_type=AssetType.STATIC_MODEL, name='claude_instant'),
    'gemini-2_0-flash': Asset(asset_type=AssetType.STATIC_MODEL, name='gemini-2_0-flash'),
    'gpto1': Asset(asset_type=AssetType.STATIC_MODEL, name='o1'),

    # New models from the provided image
    'claude3_7_sonnet': Asset(asset_type=AssetType.STATIC_MODEL, name='claude_3_7_sonnet'),
    'claude3_sonnet': Asset(asset_type=AssetType.STATIC_MODEL, name='claude_3_sonnet'),
    'gpt3_5_turbo': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-3_5-turbo-1106'),
    'gpt4_1_mini': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4_1-mini'),
    'gpt4_1': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4_1'),
    'gpt4_1_nano': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4-1-nano'),
    'gpt4o_mini_search': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4o-mini-search-preview'),
    'gpt4o_audio': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-4o-audio-preview'),
    'mlp_deepseek_rl': Asset(asset_type=AssetType.STATIC_MODEL, name='mlp_deepseek_rl'),
    'gpt5_nano': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-5-nano'),
    'gpt5_codex': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-5-codex'),
    'gpt5_mini': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-5-mini'),
    'gpt5': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-5'),
    'gpt5_1': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-5_1'),
    'gpt5_1_codex_mini': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-5_1-codex-mini'),
    'gpt5_1_codex': Asset(asset_type=AssetType.STATIC_MODEL, name='gpt-5_1-codex'),
    'claude4_sonnet': Asset(asset_type=AssetType.STATIC_MODEL, name='claude_4_sonnet'),
    'claude4_5_sonnet': Asset(asset_type=AssetType.STATIC_MODEL, name='claude-sonnet-4-5'),
    'claude4_5_opus': Asset(asset_type=AssetType.STATIC_MODEL, name='claude-opus-4-5'),
    'gemini_2_5_flash': Asset(asset_type=AssetType.STATIC_MODEL, name='gemini-2_5-flash'),
    'gemini_2_5_pro': Asset(asset_type=AssetType.STATIC_MODEL, name='gemini-2_5-pro'),
    'gemini_3_pro': Asset(asset_type=AssetType.STATIC_MODEL, name='gemini-3-pro')
}

genai_client = GenAIClient(
                service_info=ServiceInfo(
                    Service.GEN_AI
                ),
                timeout_s=70.5,
                use_json=True,
            )
application = Application(name='taylor-agent-poc')
model_asset = MODEL_MAPPINGS['claude3.5']

model = ChatGenAIGatewayModel(client=genai_client, application=application, model_asset=model_asset)


# Define tools
@tool
def multiply(a: int, b: int) -> int:
    """Multiply `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a * b


@tool
def add(a: int, b: int) -> int:
    """Adds `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a + b


@tool
def divide(a: int, b: int) -> float:
    """Divide `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a / b


# Augment the LLM with tools
tools = [add, multiply, divide]
tools_by_name = {tool.name: tool for tool in tools}
model_with_tools = model.bind_tools(tools)

# Step 2: Define state

from langchain.messages import AnyMessage
from typing_extensions import TypedDict, Annotated
import operator


class MessagesState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]
    llm_calls: int

# Step 3: Define model node
from langchain.messages import SystemMessage


def llm_call(state: dict):
    """LLM decides whether to call a tool or not"""

    return {
        "messages": [
            model_with_tools.invoke(
                [
                    SystemMessage(
                        content="You are a helpful assistant tasked with performing arithmetic on a set of inputs."
                    )
                ]
                + state["messages"]
            )
        ],
        "llm_calls": state.get('llm_calls', 0) + 1
    }


# Step 4: Define tool node

from langchain.messages import ToolMessage


def tool_node(state: dict):
    """Performs the tool call"""

    result = []
    for tool_call in state["messages"][-1].tool_calls:
        tool = tools_by_name[tool_call["name"]]
        observation = tool.invoke(tool_call["args"])
        result.append(ToolMessage(content=observation, tool_call_id=tool_call["id"]))
    return {"messages": result}

# Step 5: Define logic to determine whether to end

from typing import Literal
from langgraph.graph import StateGraph, START, END


# Conditional edge function to route to the tool node or end based upon whether the LLM made a tool call
def should_continue(state: MessagesState) -> Literal["tool_node", END]:
    """Decide if we should continue the loop or stop based upon whether the LLM made a tool call"""

    messages = state["messages"]
    last_message = messages[-1]

    # If the LLM makes a tool call, then perform an action
    if last_message.tool_calls:
        return "tool_node"

    # Otherwise, we stop (reply to the user)
    return END

# Step 6: Build agent

# Build workflow
agent_builder = StateGraph(MessagesState)

# Add nodes
agent_builder.add_node("llm_call", llm_call)
agent_builder.add_node("tool_node", tool_node)

# Add edges to connect nodes
agent_builder.add_edge(START, "llm_call")
agent_builder.add_conditional_edges(
    "llm_call",
    should_continue,
    ["tool_node", END]
)
agent_builder.add_edge("tool_node", "llm_call")

# Compile the agent
agent = agent_builder.compile()


from IPython.display import Image, display
# Show the agent
display(Image(agent.get_graph(xray=True).draw_mermaid_png()))

# Invoke
from langchain.messages import HumanMessage
messages = [HumanMessage(content="Use tools to calculate the average of the list [1,2222,9]")]
messages = agent.invoke({"messages": messages})
for m in messages["messages"]:
    m.pretty_print()
