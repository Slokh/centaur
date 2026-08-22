import subprocess
import unittest

from prewarm_tools import parse_tool_names, prewarm_tools


class PrewarmToolsTest(unittest.TestCase):
    def test_parses_deduplicated_comma_or_whitespace_separated_names(self) -> None:
        self.assertEqual(
            parse_tool_names("application, company_context\napplication"),
            ["application", "company_context"],
        )

    def test_rejects_values_that_could_be_shell_syntax(self) -> None:
        for value in ("application;id", "$(id)", "../application"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_tool_names(value)

    def test_invokes_each_resolved_tool_without_a_shell(self) -> None:
        calls: list[tuple[str, int]] = []
        prewarm_tools(
            ["application", "company_context"],
            45,
            resolve=lambda name: f"/tools/{name}",
            invoke=lambda executable, timeout: calls.append((executable, timeout)),
        )
        self.assertEqual(
            calls,
            [("/tools/application", 45), ("/tools/company_context", 45)],
        )

    def test_fails_when_a_configured_tool_is_missing(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            prewarm_tools(["missing"], 60, resolve=lambda _name: None)

    def test_fails_when_help_times_out(self) -> None:
        def timeout(_executable: str, seconds: int) -> None:
            raise subprocess.TimeoutExpired("tool", seconds)

        with self.assertRaisesRegex(RuntimeError, "timed out"):
            prewarm_tools(
                ["slow"],
                10,
                resolve=lambda name: f"/tools/{name}",
                invoke=timeout,
            )


if __name__ == "__main__":
    unittest.main()
