"""Ask DataHub MCP tools for interacting with the DataHub AI agent.

These tools allow external MCP clients to send questions to Ask DataHub and poll
for responses, providing the same AI-powered data catalog exploration available
in the DataHub UI.

Cloud-only: depends on the chat subsystem which is not part of the OSS MCP server.
"""

import threading
from typing import Any, Optional

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.sdk.main_client import DataHubClient
from fastmcp import FastMCP
from loguru import logger

from datahub_integrations.mcp import graphql_helpers
from datahub_integrations.mcp.mcp_server import _register_tool

# Tag used to identify these tools so they can be filtered out of
# the Ask DataHub agent's own tool set (preventing recursive self-calls).
ASK_DATAHUB_TOOL_TAG = "ask_datahub"

_CREATE_CONVERSATION_MUTATION = """
mutation CreateConversation($input: CreateDataHubAiConversationInput!) {
    createDataHubAiConversation(input: $input) {
        urn
    }
}"""

_GET_CONVERSATION_QUERY = """
query GetConversation($urn: String!) {
    getDataHubAiConversation(urn: $urn) {
        urn
        title
        originType
        lastMessageTime
        messages {
            type
            time
            actor {
                type
                actor
            }
            content {
                text
            }
        }
    }
}
"""

_DEFAULT_MESSAGE_LIMIT = 10


def _get_user_urn() -> str:
    """Resolve the URN of the currently authenticated MCP user."""
    client = graphql_helpers.get_datahub_client()
    result = graphql_helpers.execute_graphql(
        client._graph,
        query="query { me { corpUser { urn } } }",
        variables={},
    )
    urn = result.get("me", {}).get("corpUser", {}).get("urn")
    if not urn:
        raise RuntimeError("Could not determine authenticated user URN")
    return urn


def _create_mcp_conversation(user_client: DataHubClient) -> str:
    """Create a new conversation with MCP origin type via GraphQL."""
    result = graphql_helpers.execute_graphql(
        user_client._graph,
        query=_CREATE_CONVERSATION_MUTATION,
        variables={"input": {"originType": "MCP"}},
    )
    urn = result["createDataHubAiConversation"]["urn"]
    logger.info(f"Created MCP conversation: {urn}")
    return urn


def _get_system_client() -> DataHubClient:
    from datahub_integrations.app import graph as system_graph

    return DataHubClient(graph=system_graph)


def _run_chat(
    tools_client: DataHubClient,
    conversation_urn: str,
    user_urn: str,
    message: str,
) -> Optional[str]:
    """Run the ChatSessionManager.send_message generator to completion.

    All persistence (user message, agent response) happens as side effects of
    iterating through the generator. Returns the agent's final TEXT response,
    or None if no text response was produced.
    """
    from datahub_integrations.chat.chat_session_manager import ChatSessionManager

    manager = ChatSessionManager(
        system_client=_get_system_client(),
        tools_client=tools_client,
    )
    agent_response: Optional[str] = None
    for event in manager.send_message(
        text=message,
        user_urn=user_urn,
        conversation_urn=conversation_urn,
    ):
        if event.message_type == "TEXT" and event.user_urn is None and event.text:
            agent_response = event.text

    return agent_response


def _run_chat_in_background(
    tools_client: DataHubClient,
    conversation_urn: str,
    user_urn: str,
    message: str,
) -> None:
    """Run _run_chat in a background thread with error handling."""
    try:
        _run_chat(tools_client, conversation_urn, user_urn, message)
        logger.info(f"Background chat completed for {conversation_urn}")
    except Exception:
        logger.opt(exception=True).error(
            f"Background chat failed for {conversation_urn}"
        )


def ask_datahub_chat(
    message: str,
    conversation_urn: Optional[str] = None,
    async_mode: bool = False,
) -> dict[str, Any]:
    """Send a message to Ask DataHub, the DataHub AI assistant.

    Submits a question to the DataHub AI agent which can search the data catalog,
    explore lineage, inspect schemas, and answer questions about your data assets.

    If no conversation_urn is provided, a new conversation is created. To continue
    an existing conversation (follow-up questions), pass the conversation_urn
    returned from a previous call.

    By default this tool blocks until the agent finishes and returns the response
    inline. Set async_mode=True to return immediately and poll for the response
    later using get_datahub_chat().

    Args:
        message: The question or message to send to the AI agent.
        conversation_urn: Optional URN of an existing conversation for follow-ups.
            If omitted, a new conversation is created.
        async_mode: If True, return immediately with status "processing" and use
            get_datahub_chat() to poll for the response. If False (default), block
            until the agent responds and return the answer inline.

    Returns:
        Dictionary with:
        - conversation_urn: URN of the conversation (use for follow-ups)
        - status: "complete" or "processing"
        - response: The agent's response text (present when status is "complete")
        - message: Confirmation text (present when status is "processing")

    Example:
        # Synchronous (default) — blocks until the agent responds
        result = ask_datahub_chat(message="What are the most queried datasets?")
        print(result["response"])

        # Send a follow-up in the same conversation
        result = ask_datahub_chat(
            message="Show me the schema of the top one",
            conversation_urn=result["conversation_urn"],
        )

        # Async — returns immediately, poll with get_datahub_chat()
        result = ask_datahub_chat(message="Complex question...", async_mode=True)
        # ... later ...
        response = get_datahub_chat(conversation_urn=result["conversation_urn"])
    """
    client = graphql_helpers.get_datahub_client()
    user_urn = _get_user_urn()

    if not conversation_urn:
        conversation_urn = _create_mcp_conversation(client)

    if async_mode:
        thread = threading.Thread(
            target=_run_chat_in_background,
            args=(client, conversation_urn, user_urn, message),
            daemon=True,
        )
        thread.start()
        return {
            "conversation_urn": conversation_urn,
            "status": "processing",
            "message": (
                f"Message sent to Ask DataHub. Use get_datahub_chat("
                f'conversation_urn="{conversation_urn}") to poll for the response.'
            ),
        }

    # Synchronous: block until the agent responds
    try:
        agent_response = _run_chat(client, conversation_urn, user_urn, message)
    except Exception as e:
        logger.opt(exception=True).error(
            f"Chat processing failed for {conversation_urn}"
        )
        return {
            "conversation_urn": conversation_urn,
            "status": "error",
            "message": f"Agent processing failed: {e}",
        }

    result: dict[str, Any] = {
        "conversation_urn": conversation_urn,
        "status": "complete",
    }
    if agent_response:
        result["response"] = agent_response
    return result


def _extract_text_messages(
    raw_messages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Filter raw GraphQL messages down to user/agent TEXT messages."""
    text_messages: list[dict[str, Any]] = []
    for msg in raw_messages:
        if msg.get("type", "") != "TEXT":
            continue
        actor_type = msg.get("actor", {}).get("type", "")
        text = msg.get("content", {}).get("text", "")
        if not text:
            continue
        text_messages.append(
            {
                "role": "user" if actor_type == "USER" else "assistant",
                "text": text,
                "time": msg.get("time"),
            }
        )
    return text_messages


def _determine_completion_status(
    raw_messages: list[dict[str, Any]],
) -> tuple[bool, Optional[str]]:
    """Check whether the agent has responded to the latest user message.

    Returns (is_complete, last_agent_response_text).
    """
    last_user_msg_time = 0
    last_agent_response: Optional[str] = None
    last_agent_response_time = 0

    for msg in raw_messages:
        actor_type = msg.get("actor", {}).get("type", "")
        msg_type = msg.get("type", "")
        msg_time = msg.get("time", 0)
        text = msg.get("content", {}).get("text", "")

        if actor_type == "USER" and msg_type == "TEXT":
            last_user_msg_time = max(last_user_msg_time, msg_time)
        elif actor_type == "AGENT" and msg_type == "TEXT" and text:
            if msg_time > last_agent_response_time:
                last_agent_response = text
                last_agent_response_time = msg_time

    is_complete = (
        last_agent_response is not None
        and last_agent_response_time > last_user_msg_time
    )
    return is_complete, last_agent_response


def get_datahub_chat(
    conversation_urn: str,
    message_limit: int = _DEFAULT_MESSAGE_LIMIT,
    offset: int = 0,
) -> dict[str, Any]:
    """Retrieve messages and status from an Ask DataHub conversation.

    Returns the current state of a conversation including whether the AI agent
    has finished responding. Use this to:
    - Poll for a response after calling ask_datahub_chat(async_mode=True)
    - Read back any conversation's message history

    Messages are returned in most-recent-first order. Use message_limit and
    offset to page through longer conversations.

    Args:
        conversation_urn: URN of the conversation (from ask_datahub_chat response).
        message_limit: Maximum number of messages to return. Defaults to 10.
        offset: Number of most-recent messages to skip before returning results.
            For example, offset=0 returns the latest messages, offset=10 skips
            the 10 most recent and returns the next batch. Defaults to 0.

    Returns:
        Dictionary with:
        - conversation_urn: The conversation URN
        - status: "complete" if the agent has responded, "processing" if still working
        - response: The agent's latest response text (only present when complete)
        - messages: List of recent messages (most recent first), each with
          role ("user" or "assistant"), text, and time
        - total_messages: Total number of text messages in the conversation

    Example:
        response = get_datahub_chat(conversation_urn="urn:li:dataHubAiConversation:abc")
        if response["status"] == "complete":
            print(response["response"])

        # Page through older messages
        older = get_datahub_chat(
            conversation_urn="urn:li:dataHubAiConversation:abc",
            message_limit=10,
            offset=10,
        )
    """
    client = graphql_helpers.get_datahub_client()

    try:
        result = graphql_helpers.execute_graphql(
            client._graph,
            query=_GET_CONVERSATION_QUERY,
            variables={"urn": conversation_urn},
        )
    except Exception as e:
        raise RuntimeError(
            f"Failed to fetch conversation {conversation_urn}: {e}"
        ) from e

    conversation = result.get("getDataHubAiConversation")
    if not conversation:
        raise RuntimeError(f"Conversation {conversation_urn} not found")

    raw_messages = conversation.get("messages", [])

    is_complete, last_agent_response = _determine_completion_status(raw_messages)

    all_text_messages = _extract_text_messages(raw_messages)
    total_messages = len(all_text_messages)

    # Most-recent-first, then apply offset and limit
    all_text_messages.reverse()
    page = all_text_messages[offset : offset + message_limit]

    response: dict[str, Any] = {
        "conversation_urn": conversation_urn,
        "status": "complete" if is_complete else "processing",
        "messages": page,
        "total_messages": total_messages,
    }
    if is_complete and last_agent_response:
        response["response"] = last_agent_response

    return response


def is_ask_datahub_tools_enabled() -> bool:
    return get_boolean_env_variable("ASK_DATAHUB_TOOLS", default=False)


def register_ask_datahub_tools(mcp_instance: FastMCP) -> None:
    """Register Ask DataHub tools if enabled via ASK_DATAHUB_TOOLS env var."""
    if not is_ask_datahub_tools_enabled():
        logger.info("Ask DataHub Tools DISABLED")
        return

    logger.info("Ask DataHub Tools ENABLED - registering tools")

    tags = {ASK_DATAHUB_TOOL_TAG}
    _register_tool(mcp_instance, "ask_datahub_chat", ask_datahub_chat, tags=tags)
    _register_tool(mcp_instance, "get_datahub_chat", get_datahub_chat, tags=tags)
