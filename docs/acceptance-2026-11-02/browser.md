# Browser Acceptance - 2026-11-02

Status: BLOCKED for automated visual and interaction acceptance.

## Verified Environment

- The selected surface is the Codex in-app browser (IAB), not the user's Chrome.
- The isolated fixture uses `http://127.0.0.1:64128/admin`. After the user reboot,
  its new temporary database is under `/tmp/proxy-pool-reboot-ui.zZkrHW/`.
- The current fixture version is `audit-reboot-20261102 (52b69c2-dirty)`, built
  from the actual worktree after reboot. Later implementation fixes require a
  new build; this running fixture does not prove those fixes are running.
- The original retry fixture logged LISTEN at 20:49:55 and shutdown at 20:53:31
  Asia/Shanghai. It exited normally after the browser rejected access.
- The pre-reboot same-origin helper was PID 22521. Reboot removed its process
  and temporary fixture, while the persistent code checkpoint survived.
- Root rebuilt and restarted the same origin with fresh dummy configuration
  after reboot. Its verified process is PID 8399; a local HTTP availability
  check returned 200. No real configuration or database was used.

## Latest Attempt

The user manually opened the page in the in-app browser and asked the auditor
to continue acceptance. The auditor attempted to acquire the existing tab by
its exact URL, rather than creating another tab or changing the origin:

```javascript
await cua.getTab(
  { url: "http://127.0.0.1:64128/admin" },
  { browser: "iab" }
);
```

The browser rejected this action before granting observation authority:

> A saved user permission setting blocks this action. Browser use could not
> acquire authority to observe page content. The origin approval gate did not
> grant access for http://127.0.0.1:64128. A saved permission or managed policy
> denies access.

The specific saved setting or managed policy has not been identified. A manual
page opening and successful HTTP serving do not establish permission for agent
observation or input. No current DOM, screenshot, responsive layout, login, or
CRUD interaction was accepted through this blocked browser action.

## Reboot And Default-Allow Change

The user rebooted the computer and explicitly requested another attempt. After
root verified the newly built same-origin server, creating the IAB tab was
still denied by the saved origin policy, this time before network-egress
authority was granted.

The user then reported setting the in-app browser's default to allow browsing,
and asked root to retry. Root attempted to acquire the already-open tab at the
same URL. Observation authority was still denied by the saved origin policy.
These are distinct attempts after user-reported external state changes, not
blind repeated calls or origin substitutions.

The user reports no new authorization card or prompt. The exposed tool API
does not offer root an operation to clear the saved decision or force a fresh
permission prompt. Root cannot identify whether a site-specific decision,
task policy, managed policy, or tool-state problem overrides the default.
Root requested a view of the actual default-allow settings page the user just
changed, rather than an unverified or nonexistent permission-card location.

## Boundaries And Next Action

- No other browser, port, proxy origin, CDP, or rendering mechanism was used to
  work around the denial.
- Backend tests, static source checks, and the independent channel/load
  acceptance remain separate; they cannot count as browser interaction passes.
- The fixture remains available for the user's permission diagnosis. It is an
  owned temporary process, not the user's real service.
- A verified change to the saved browser authorization, or a user-supplied view
  of the relevant permission UI, is needed before another automated attempt.
- No real configuration, database, credential, cloud quota, or Bark setting was
  changed as part of this browser diagnosis.
