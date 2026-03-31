"""Ask DataHub chat tools (Cloud-only).

These tools let external agents interact with the Ask DataHub AI assistant
through the DataHub API. They work by calling the GMS REST chat endpoint
which delegates to the integrations service for agent processing.

Not available on OSS DataHub instances — the tools check for Cloud at runtime
and are excluded from LangChain/ADK builders on non-Cloud connections.
"""

import logging
import threading
from typing import Any, Optional

from datahub_agent_context.context import get_graph
from datahub_agent_context.mcp_tools.base import _is_datahub_cloud, execute_graphql

logger = logging.getLogger(__name__)

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
_CHAT_ENDPOINT = "/openapi/v1/ai-chat/message"
_STREAM_TIMEOUT_SECONDS = 300


def _require_cloud() -> None:
    """Raise if not connected to a DataHub Cloud instance."""
    graph = get_graph()
    if not _is_datahub_cloud(graph):
        raise RuntimeError(
            "Ask DataHub tools require DataHub Cloud. "
            "This instance does not appear to be a Cloud deployment."
        )


def _create_conversation() -> str:
    """Create a new conversation with MCP origin type via GraphQL."""
    graph = get_graph()
    result = execute_graphql(
        graph,
        query=_CREATE_CONVERSATION_MUTATION,
        variables={"input": {"originType": "MCP"}},
    )
    urn = result["createDataHubAiConversation"]["urn"]
    logger.info(f"Created MCP conversation: {urn}")
    return urn


def _consume_chat_stream(conversation_urn: str, text: str) -> None:
    """POST to the GMS chat endpoint and consume the SSE stream to completion.

    The GMS endpoint proxies to the Python integrations service, which runs the
    AI agent and persists messages as side effects. We just need to keep the
    connection alive until the stream finishes.
    """
    graph = get_graph()
    url = f"{graph._gms_server}{_CHAT_ENDPOINT}"
    body = {"conversationUrn": conversation_urn, "text": text}

    response = graph._session.post(
        url,
        json=body,
        stream=True,
        timeout=_STREAM_TIMEOUT_SECONDS,
        headers={"Accept": "text/event-stream"},
    )
    response.raise_for_status()

    for _line in response.iter_lines():
        pass


def _consume_chat_stream_background(conversation_urn: str, text: str) -> None:
    """Wrapper for background thread execution with error handling."""
    try:
        _consume_chat_stream(conversation_urn, text)
        logger.info(f"Background chat completed for {conversation_urn}")
    except Exception:
        logger.exception(f"Background chat failed for {conversation_urn}")


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

    Uses a timestamp heuristic: the conversation is complete when the last
    AGENT TEXT message has a later timestamp than the last USER TEXT message.

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

    **Cloud-only** — raises RuntimeError on OSS DataHub instances.

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
    _require_cloud()

    if not conversation_urn:
        conversation_urn = _create_conversation()

    if async_mode:
        thread = threading.Thread(
            target=_consume_chat_stream_background,
            args=(conversation_urn, message),
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

    # Synchronous: consume the SSE stream, then fetch the result
    try:
        _consume_chat_stream(conversation_urn, message)
    except Exception as e:
        logger.exception(f"Chat processing failed for {conversation_urn}")
        return {
            "conversation_urn": conversation_urn,
            "status": "error",
            "message": f"Agent processing failed: {e}",
        }

    return get_datahub_chat(conversation_urn)


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

    **Cloud-only** — raises RuntimeError on OSS DataHub instances.

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
        response = get_datahub_chat(
            conversation_urn="urn:li:dataHubAiConversation:abc123"
        )
        if response["status"] == "complete":
            print(response["response"])

        # Page through older messages
        older = get_datahub_chat(
            conversation_urn="urn:li:dataHubAiConversation:abc123",
            message_limit=10,
            offset=10,
        )
    """
    _require_cloud()
    graph = get_graph()

    try:
        result = execute_graphql(
            graph,
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
