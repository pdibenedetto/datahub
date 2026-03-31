"""Tests for ask_datahub MCP tools."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from datahub_integrations.mcp_integration.ask_datahub_tools import (
    ASK_DATAHUB_TOOL_TAG,
    _determine_completion_status,
    _extract_text_messages,
    ask_datahub_chat,
    get_datahub_chat,
    is_ask_datahub_tools_enabled,
    register_ask_datahub_tools,
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


def test_extract_text_messages_skips_empty_text() -> None:
    msgs = [
        _user_msg("hello", 100),
        {
            "type": "TEXT",
            "time": 200,
            "actor": {"type": "AGENT"},
            "content": {"text": ""},
        },
    ]
    result = _extract_text_messages(msgs)
    assert len(result) == 1


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
    assert response is None


def test_completion_status_follow_up_pending() -> None:
    """User sent a follow-up after the agent responded."""
    msgs = [
        _user_msg("q1", 100),
        _agent_msg("a1", 200),
        _user_msg("q2", 300),
    ]
    is_complete, response = _determine_completion_status(msgs)
    assert is_complete is False
    assert response == "a1"


def test_completion_status_ignores_thinking() -> None:
    msgs = [
        _user_msg("q", 100),
        _thinking_msg("thinking...", 200),
    ]
    is_complete, response = _determine_completion_status(msgs)
    assert is_complete is False
    assert response is None


def test_completion_status_empty_messages() -> None:
    is_complete, response = _determine_completion_status([])
    assert is_complete is False
    assert response is None


def test_completion_status_multiple_exchanges() -> None:
    msgs = [
        _user_msg("q1", 100),
        _agent_msg("a1", 200),
        _user_msg("q2", 300),
        _agent_msg("a2", 400),
    ]
    is_complete, response = _determine_completion_status(msgs)
    assert is_complete is True
    assert response == "a2"


# ---------------------------------------------------------------------------
# get_datahub_chat
# ---------------------------------------------------------------------------


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


@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_get_chat_complete(mock_helpers: MagicMock) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_helpers.execute_graphql.return_value = _mock_graphql_conversation(
        [
            _user_msg("q", 100),
            _thinking_msg("thinking", 150),
            _agent_msg("a", 200),
        ]
    )

    result = get_datahub_chat("urn:li:dataHubAiConversation:test1")

    assert result["status"] == "complete"
    assert result["response"] == "a"
    assert result["total_messages"] == 2
    # Most-recent-first
    assert result["messages"][0]["role"] == "assistant"
    assert result["messages"][1]["role"] == "user"


@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_get_chat_processing(mock_helpers: MagicMock) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_helpers.execute_graphql.return_value = _mock_graphql_conversation(
        [
            _user_msg("q", 100),
        ]
    )

    result = get_datahub_chat("urn:li:dataHubAiConversation:test1")

    assert result["status"] == "processing"
    assert "response" not in result
    assert result["total_messages"] == 1


@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_get_chat_not_found(mock_helpers: MagicMock) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_helpers.execute_graphql.return_value = {"getDataHubAiConversation": None}

    with pytest.raises(RuntimeError, match="not found"):
        get_datahub_chat("urn:li:dataHubAiConversation:missing")


@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_get_chat_pagination(mock_helpers: MagicMock) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client

    # 6 TEXT messages
    messages = []
    for i in range(3):
        messages.append(_user_msg(f"q{i}", 100 + i * 200))
        messages.append(_agent_msg(f"a{i}", 200 + i * 200))

    mock_helpers.execute_graphql.return_value = _mock_graphql_conversation(messages)

    # First page: limit=2, offset=0 → 2 most recent
    page1 = get_datahub_chat(
        "urn:li:dataHubAiConversation:test1", message_limit=2, offset=0
    )
    assert len(page1["messages"]) == 2
    assert page1["total_messages"] == 6
    assert page1["messages"][0]["text"] == "a2"
    assert page1["messages"][1]["text"] == "q2"

    # Second page: limit=2, offset=2 → next 2
    page2 = get_datahub_chat(
        "urn:li:dataHubAiConversation:test1", message_limit=2, offset=2
    )
    assert len(page2["messages"]) == 2
    assert page2["messages"][0]["text"] == "a1"
    assert page2["messages"][1]["text"] == "q1"

    # Third page: offset=4 → last 2
    page3 = get_datahub_chat(
        "urn:li:dataHubAiConversation:test1", message_limit=2, offset=4
    )
    assert len(page3["messages"]) == 2
    assert page3["messages"][0]["text"] == "a0"

    # Past the end
    page4 = get_datahub_chat(
        "urn:li:dataHubAiConversation:test1", message_limit=2, offset=10
    )
    assert len(page4["messages"]) == 0
    assert page4["total_messages"] == 6


# ---------------------------------------------------------------------------
# ask_datahub_chat — async mode
# ---------------------------------------------------------------------------


@patch("datahub_integrations.mcp_integration.ask_datahub_tools.threading.Thread")
@patch(
    "datahub_integrations.mcp_integration.ask_datahub_tools._create_mcp_conversation"
)
@patch("datahub_integrations.mcp_integration.ask_datahub_tools._get_user_urn")
@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_ask_chat_async_new_conversation(
    mock_helpers: MagicMock,
    mock_get_urn: MagicMock,
    mock_create: MagicMock,
    mock_thread_cls: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_get_urn.return_value = "urn:li:corpuser:alice"
    mock_create.return_value = "urn:li:dataHubAiConversation:new123"

    result = ask_datahub_chat(message="What datasets?", async_mode=True)

    assert result["status"] == "processing"
    assert result["conversation_urn"] == "urn:li:dataHubAiConversation:new123"
    mock_create.assert_called_once_with(mock_client)
    mock_thread_cls.assert_called_once()
    mock_thread_cls.return_value.start.assert_called_once()


@patch("datahub_integrations.mcp_integration.ask_datahub_tools.threading.Thread")
@patch(
    "datahub_integrations.mcp_integration.ask_datahub_tools._create_mcp_conversation"
)
@patch("datahub_integrations.mcp_integration.ask_datahub_tools._get_user_urn")
@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_ask_chat_async_existing_conversation(
    mock_helpers: MagicMock,
    mock_get_urn: MagicMock,
    mock_create: MagicMock,
    mock_thread_cls: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_get_urn.return_value = "urn:li:corpuser:alice"

    existing_urn = "urn:li:dataHubAiConversation:existing456"
    result = ask_datahub_chat(
        message="Follow up", conversation_urn=existing_urn, async_mode=True
    )

    assert result["conversation_urn"] == existing_urn
    mock_create.assert_not_called()
    mock_thread_cls.return_value.start.assert_called_once()


# ---------------------------------------------------------------------------
# ask_datahub_chat — sync mode (default)
# ---------------------------------------------------------------------------


@patch("datahub_integrations.mcp_integration.ask_datahub_tools._run_chat")
@patch(
    "datahub_integrations.mcp_integration.ask_datahub_tools._create_mcp_conversation"
)
@patch("datahub_integrations.mcp_integration.ask_datahub_tools._get_user_urn")
@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_ask_chat_sync_returns_response(
    mock_helpers: MagicMock,
    mock_get_urn: MagicMock,
    mock_create: MagicMock,
    mock_run_chat: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_get_urn.return_value = "urn:li:corpuser:alice"
    mock_create.return_value = "urn:li:dataHubAiConversation:sync1"
    mock_run_chat.return_value = "Here are the top datasets..."

    result = ask_datahub_chat(message="What datasets?")

    assert result["status"] == "complete"
    assert result["response"] == "Here are the top datasets..."
    assert result["conversation_urn"] == "urn:li:dataHubAiConversation:sync1"
    mock_run_chat.assert_called_once_with(
        mock_client,
        "urn:li:dataHubAiConversation:sync1",
        "urn:li:corpuser:alice",
        "What datasets?",
    )


@patch("datahub_integrations.mcp_integration.ask_datahub_tools._run_chat")
@patch(
    "datahub_integrations.mcp_integration.ask_datahub_tools._create_mcp_conversation"
)
@patch("datahub_integrations.mcp_integration.ask_datahub_tools._get_user_urn")
@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_ask_chat_sync_existing_conversation(
    mock_helpers: MagicMock,
    mock_get_urn: MagicMock,
    mock_create: MagicMock,
    mock_run_chat: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_get_urn.return_value = "urn:li:corpuser:alice"
    mock_run_chat.return_value = "Follow-up answer"

    existing_urn = "urn:li:dataHubAiConversation:existing789"
    result = ask_datahub_chat(message="Follow up", conversation_urn=existing_urn)

    assert result["status"] == "complete"
    assert result["response"] == "Follow-up answer"
    assert result["conversation_urn"] == existing_urn
    mock_create.assert_not_called()


@patch("datahub_integrations.mcp_integration.ask_datahub_tools._run_chat")
@patch(
    "datahub_integrations.mcp_integration.ask_datahub_tools._create_mcp_conversation"
)
@patch("datahub_integrations.mcp_integration.ask_datahub_tools._get_user_urn")
@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_ask_chat_sync_handles_error(
    mock_helpers: MagicMock,
    mock_get_urn: MagicMock,
    mock_create: MagicMock,
    mock_run_chat: MagicMock,
) -> None:
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_get_urn.return_value = "urn:li:corpuser:alice"
    mock_create.return_value = "urn:li:dataHubAiConversation:err1"
    mock_run_chat.side_effect = RuntimeError("Agent crashed")

    result = ask_datahub_chat(message="Boom")

    assert result["status"] == "error"
    assert "Agent crashed" in result["message"]
    assert result["conversation_urn"] == "urn:li:dataHubAiConversation:err1"


@patch("datahub_integrations.mcp_integration.ask_datahub_tools._run_chat")
@patch(
    "datahub_integrations.mcp_integration.ask_datahub_tools._create_mcp_conversation"
)
@patch("datahub_integrations.mcp_integration.ask_datahub_tools._get_user_urn")
@patch("datahub_integrations.mcp_integration.ask_datahub_tools.graphql_helpers")
def test_ask_chat_sync_no_response_text(
    mock_helpers: MagicMock,
    mock_get_urn: MagicMock,
    mock_create: MagicMock,
    mock_run_chat: MagicMock,
) -> None:
    """Agent completed but produced no text response (edge case)."""
    mock_client = MagicMock()
    mock_helpers.get_datahub_client.return_value = mock_client
    mock_get_urn.return_value = "urn:li:corpuser:alice"
    mock_create.return_value = "urn:li:dataHubAiConversation:empty1"
    mock_run_chat.return_value = None

    result = ask_datahub_chat(message="Something")

    assert result["status"] == "complete"
    assert "response" not in result


# ---------------------------------------------------------------------------
# register_ask_datahub_tools
# ---------------------------------------------------------------------------


@patch.dict("os.environ", {"ASK_DATAHUB_TOOLS": "true"})
def test_register_tools_when_enabled() -> None:
    mcp_instance = FastMCP[None](name="test")
    register_ask_datahub_tools(mcp_instance)

    tool_names = list(mcp_instance._tool_manager._tools.keys())
    assert "ask_datahub_chat" in tool_names
    assert "get_datahub_chat" in tool_names

    for tool in mcp_instance._tool_manager._tools.values():
        assert ASK_DATAHUB_TOOL_TAG in (tool.tags or set())


@patch.dict("os.environ", {"ASK_DATAHUB_TOOLS": "false"})
def test_register_tools_when_disabled() -> None:
    mcp_instance = FastMCP[None](name="test")
    register_ask_datahub_tools(mcp_instance)
    assert len(mcp_instance._tool_manager._tools) == 0


@patch.dict("os.environ", {}, clear=False)
def test_register_tools_default_disabled() -> None:
    """When ASK_DATAHUB_TOOLS is not set, tools should not be registered."""
    import os

    os.environ.pop("ASK_DATAHUB_TOOLS", None)
    mcp_instance = FastMCP[None](name="test")
    register_ask_datahub_tools(mcp_instance)
    assert len(mcp_instance._tool_manager._tools) == 0


@patch.dict("os.environ", {"ASK_DATAHUB_TOOLS": "true"})
def test_is_ask_datahub_tools_enabled_true() -> None:
    assert is_ask_datahub_tools_enabled() is True


@patch.dict("os.environ", {"ASK_DATAHUB_TOOLS": "false"})
def test_is_ask_datahub_tools_enabled_false() -> None:
    assert is_ask_datahub_tools_enabled() is False
