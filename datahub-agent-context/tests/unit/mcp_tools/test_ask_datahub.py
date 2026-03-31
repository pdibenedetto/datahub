"""Tests for Ask DataHub chat tools (agent-context version)."""

from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub_agent_context.mcp_tools.ask_datahub import (
    _determine_completion_status,
    _extract_text_messages,
    ask_datahub_chat,
    get_datahub_chat,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _user_msg(text: str, time: int) -> dict[str, Any]:
    return {
        "type": "TEXT",
        "time": time,
        "actor": {"type": "USER", "actor": "urn:li:corpuser:alice"},
        "content": {"text": text},
    }


def _agent_msg(text: str, time: int) -> dict[str, Any]:
    return {
        "type": "TEXT",
        "time": time,
        "actor": {"type": "AGENT", "actor": "urn:li:corpuser:datahub-ai"},
        "content": {"text": text},
    }


def _thinking_msg(text: str, time: int) -> dict[str, Any]:
    return {
        "type": "THINKING",
        "time": time,
        "actor": {"type": "AGENT", "actor": "urn:li:corpuser:datahub-ai"},
        "content": {"text": text},
    }


def _mock_graphql_conversation(messages: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "getDataHubAiConversation": {
            "urn": "urn:li:dataHubAiConversation:test1",
            "title": "Test",
            "originType": "MCP",
            "lastMessageTime": 400,
            "messages": messages,
        }
    }


@pytest.fixture
def cloud_graph():
    """Mock graph that reports as Cloud."""
    graph = MagicMock()
    graph.frontend_base_url = "https://mycompany.acryl.io"
    graph._gms_server = "https://mycompany.acryl.io/gms"
    return graph


@pytest.fixture
def oss_graph():
    """Mock graph that reports as OSS (no frontend_base_url)."""
    graph = Mock(spec=[])
    return graph


# ---------------------------------------------------------------------------
# _extract_text_messages
# ---------------------------------------------------------------------------


def test_extract_text_messages_filters_non_text() -> None:
    msgs = [
        _user_msg("hello", 100),
        _thinking_msg("hmm...", 200),
        _agent_msg("hi there", 300),
    ]
    result = _extract_text_messages(msgs)
    assert len(result) == 2
    assert result[0]["role"] == "user"
    assert result[1]["role"] == "assistant"


def test_extract_text_messages_empty_list() -> None:
    assert _extract_text_messages([]) == []


# ---------------------------------------------------------------------------
# _determine_completion_status
# ---------------------------------------------------------------------------


def test_completion_status_agent_responded() -> None:
    msgs = [_user_msg("q", 100), _agent_msg("a", 200)]
    is_complete, response = _determine_completion_status(msgs)
    assert is_complete is True
    assert response == "a"


def test_completion_status_still_processing() -> None:
    msgs = [_user_msg("q", 100)]
    is_complete, response = _determine_completion_status(msgs)
    assert is_complete is False


def test_completion_status_follow_up_pending() -> None:
    msgs = [_user_msg("q1", 100), _agent_msg("a1", 200), _user_msg("q2", 300)]
    is_complete, _ = _determine_completion_status(msgs)
    assert is_complete is False


# ---------------------------------------------------------------------------
# get_datahub_chat
# ---------------------------------------------------------------------------


@patch("datahub_agent_context.mcp_tools.ask_datahub.execute_graphql")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_get_chat_complete(
    mock_get_graph: MagicMock, mock_gql: MagicMock, cloud_graph: MagicMock
) -> None:
    mock_get_graph.return_value = cloud_graph
    mock_gql.return_value = _mock_graphql_conversation(
        [_user_msg("q", 100), _agent_msg("a", 200)]
    )

    result = get_datahub_chat("urn:li:dataHubAiConversation:test1")

    assert result["status"] == "complete"
    assert result["response"] == "a"
    assert result["total_messages"] == 2
    assert result["messages"][0]["role"] == "assistant"


@patch("datahub_agent_context.mcp_tools.ask_datahub.execute_graphql")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_get_chat_processing(
    mock_get_graph: MagicMock, mock_gql: MagicMock, cloud_graph: MagicMock
) -> None:
    mock_get_graph.return_value = cloud_graph
    mock_gql.return_value = _mock_graphql_conversation([_user_msg("q", 100)])

    result = get_datahub_chat("urn:li:dataHubAiConversation:test1")

    assert result["status"] == "processing"
    assert "response" not in result


@patch("datahub_agent_context.mcp_tools.ask_datahub.execute_graphql")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_get_chat_not_found(
    mock_get_graph: MagicMock, mock_gql: MagicMock, cloud_graph: MagicMock
) -> None:
    mock_get_graph.return_value = cloud_graph
    mock_gql.return_value = {"getDataHubAiConversation": None}

    with pytest.raises(RuntimeError, match="not found"):
        get_datahub_chat("urn:li:dataHubAiConversation:missing")


@patch("datahub_agent_context.mcp_tools.ask_datahub.execute_graphql")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_get_chat_pagination(
    mock_get_graph: MagicMock, mock_gql: MagicMock, cloud_graph: MagicMock
) -> None:
    mock_get_graph.return_value = cloud_graph
    messages = []
    for i in range(3):
        messages.append(_user_msg(f"q{i}", 100 + i * 200))
        messages.append(_agent_msg(f"a{i}", 200 + i * 200))
    mock_gql.return_value = _mock_graphql_conversation(messages)

    page1 = get_datahub_chat(
        "urn:li:dataHubAiConversation:test1", message_limit=2, offset=0
    )
    assert len(page1["messages"]) == 2
    assert page1["total_messages"] == 6
    assert page1["messages"][0]["text"] == "a2"


# ---------------------------------------------------------------------------
# get_datahub_chat — OSS guard
# ---------------------------------------------------------------------------


@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_get_chat_raises_on_oss(mock_get_graph: MagicMock, oss_graph: Mock) -> None:
    mock_get_graph.return_value = oss_graph

    with pytest.raises(RuntimeError, match="Cloud"):
        get_datahub_chat("urn:li:dataHubAiConversation:test1")


# ---------------------------------------------------------------------------
# ask_datahub_chat — sync mode
# ---------------------------------------------------------------------------


@patch("datahub_agent_context.mcp_tools.ask_datahub.get_datahub_chat")
@patch("datahub_agent_context.mcp_tools.ask_datahub._consume_chat_stream")
@patch("datahub_agent_context.mcp_tools.ask_datahub._create_conversation")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_ask_chat_sync(
    mock_get_graph: MagicMock,
    mock_create: MagicMock,
    mock_stream: MagicMock,
    mock_get_chat: MagicMock,
    cloud_graph: MagicMock,
) -> None:
    mock_get_graph.return_value = cloud_graph
    mock_create.return_value = "urn:li:dataHubAiConversation:sync1"
    mock_get_chat.return_value = {
        "conversation_urn": "urn:li:dataHubAiConversation:sync1",
        "status": "complete",
        "response": "The answer",
        "messages": [],
        "total_messages": 2,
    }

    result = ask_datahub_chat(message="What datasets?")

    assert result["status"] == "complete"
    assert result["response"] == "The answer"
    mock_create.assert_called_once()
    mock_stream.assert_called_once_with(
        "urn:li:dataHubAiConversation:sync1", "What datasets?"
    )


@patch("datahub_agent_context.mcp_tools.ask_datahub._consume_chat_stream")
@patch("datahub_agent_context.mcp_tools.ask_datahub._create_conversation")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_ask_chat_sync_error(
    mock_get_graph: MagicMock,
    mock_create: MagicMock,
    mock_stream: MagicMock,
    cloud_graph: MagicMock,
) -> None:
    mock_get_graph.return_value = cloud_graph
    mock_create.return_value = "urn:li:dataHubAiConversation:err1"
    mock_stream.side_effect = RuntimeError("Connection failed")

    result = ask_datahub_chat(message="Boom")

    assert result["status"] == "error"
    assert "Connection failed" in result["message"]


# ---------------------------------------------------------------------------
# ask_datahub_chat — async mode
# ---------------------------------------------------------------------------


@patch("datahub_agent_context.mcp_tools.ask_datahub.threading.Thread")
@patch("datahub_agent_context.mcp_tools.ask_datahub._create_conversation")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_ask_chat_async(
    mock_get_graph: MagicMock,
    mock_create: MagicMock,
    mock_thread_cls: MagicMock,
    cloud_graph: MagicMock,
) -> None:
    mock_get_graph.return_value = cloud_graph
    mock_create.return_value = "urn:li:dataHubAiConversation:async1"

    result = ask_datahub_chat(message="What datasets?", async_mode=True)

    assert result["status"] == "processing"
    assert result["conversation_urn"] == "urn:li:dataHubAiConversation:async1"
    mock_thread_cls.return_value.start.assert_called_once()


@patch("datahub_agent_context.mcp_tools.ask_datahub.threading.Thread")
@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_ask_chat_async_existing_conversation(
    mock_get_graph: MagicMock,
    mock_thread_cls: MagicMock,
    cloud_graph: MagicMock,
) -> None:
    mock_get_graph.return_value = cloud_graph

    existing = "urn:li:dataHubAiConversation:existing1"
    result = ask_datahub_chat(
        message="Follow up", conversation_urn=existing, async_mode=True
    )

    assert result["conversation_urn"] == existing
    mock_thread_cls.return_value.start.assert_called_once()


# ---------------------------------------------------------------------------
# ask_datahub_chat — OSS guard
# ---------------------------------------------------------------------------


@patch("datahub_agent_context.mcp_tools.ask_datahub.get_graph")
def test_ask_chat_raises_on_oss(mock_get_graph: MagicMock, oss_graph: Mock) -> None:
    mock_get_graph.return_value = oss_graph

    with pytest.raises(RuntimeError, match="Cloud"):
        ask_datahub_chat(message="Hello")
