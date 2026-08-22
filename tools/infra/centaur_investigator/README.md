# Centaur Investigator

`centaur-investigator` is a read-only operator tool for correlating chat
references with Centaur's durable session state.

## Discord message investigation

```bash
centaur-investigator discord-message \
  'https://discord.com/channels/GUILD_ID/CHANNEL_ID/MESSAGE_ID'
```

The command:

1. reads the linked message through the configured Discord bot;
2. resolves a bot reply to its referenced source message;
3. matches the source message against `session_messages.metadata.message_id`;
4. reads related sessions, executions, events, sandboxes, and traces through a
   Postgres connection configured with `default_transaction_read_only=on`; and
5. optionally adds a strict allowlist of timing fields from VictoriaLogs.

Use `--no-content` when only identifiers and timing metadata are needed. The
tool never returns Discord credentials, attachment URLs, embed bodies, raw log
messages, tool arguments, upstream error bodies, or database message parts.

Discord lookup is limited to channels visible to the configured bot. Treat the
tool as operator-only because the bot may see more channels than an individual
community member. Grant it separately from member-facing agent tools.

Without VictoriaLogs, the command still returns Discord timestamps and durable
Postgres state; ingress, first-token, and individual tool-call timings are then
best-effort or unavailable.

## Other commands

```bash
centaur-investigator investigate '<Discord link, Slack link, or thread key>'
centaur-investigator slack-thread '<Slack permalink>'
centaur-investigator session '<thread_key>'
centaur-investigator parse '<reference>'
```

