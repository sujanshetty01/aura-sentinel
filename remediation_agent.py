"""
╔══════════════════════════════════════════════════════════════╗
║        🔱  AURA-SENTINEL  |  Remediation Agent  v1.0        ║
║        Phase 5 — Automated Infrastructure Kill-Switch        ║
╚══════════════════════════════════════════════════════════════╝

Queries Neo4j for C2 hubs (IPs with ≥3 incoming bot connections)
then generates Terraform (AWS Network Firewall) to null-route them.

This mirrors the Black Lotus Labs → AWS Shield automation workflow.

NOTE: Uses the Neo4j HTTP Transaction API (port 7474) for maximum
compatibility across all Neo4j versions without driver version issues.
"""

import sys
import json
import os
import requests
from requests.auth import HTTPBasicAuth

# ── ANSI colours ─────────────────────────────────────────────────────────────
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"
DIM    = "\033[2m"

# ── Neo4j connection (HTTP REST API — avoids Bolt driver version issues) ──────
NEO4J_HTTP = "http://localhost:7474"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password123"

# ── Thresholds ────────────────────────────────────────────────────────────────
MIN_BOT_CONNECTIONS = 3   # IPs with fewer incoming edges ignored
OUTPUT_TF_FILE      = "remediation.tf"
REPORT_FILE         = "remediation_report.json"

# ── Discord / Slack Webhook ───────────────────────────────────────────────────
# Set via environment variable or paste your webhook URL directly here.
# Discord:  https://discord.com/api/webhooks/<id>/<token>
# Slack:    https://hooks.slack.com/services/<id>/<token>
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL", "")

# Colour map for Discord embed sidebar (hex int)
_LEVEL_COLOUR = {
    "CRITICAL": 0xFF0000,   # red
    "HIGH":     0xFF6600,   # orange
    "MEDIUM":   0xFFCC00,   # amber
    "LOW":      0x00AAFF,   # blue
}


# ─────────────────────────────────────────────────────────────────────────────
# Webhook Alerts — Discord & Slack
# ─────────────────────────────────────────────────────────────────────────────

def send_reaper_alert(hub: dict, total_hubs_in_run: int) -> None:
    """
    Fires a webhook to Discord and/or Slack whenever the Reaper kill-switch
    blocks a C2 hub.  Each alert includes the IP, bot count, threat level,
    total bytes harvested, and the count of nodes dismantled in this run.

    Configure via env vars:
        DISCORD_WEBHOOK_URL   — Discord incoming webhook
        SLACK_WEBHOOK_URL     — Slack incoming webhook
    """
    ip            = hub["c2_ip"]
    bot_count     = hub["connection_count"]
    bytes_total   = hub["total_bytes"]
    lvl           = threat_level(bot_count)
    colour        = _LEVEL_COLOUR.get(lvl, 0xAAAAAA)
    ts            = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # ── Discord rich embed ────────────────────────────────────────────────────
    if DISCORD_WEBHOOK_URL:
        embed = {
            "title":       f"🚨 REAPER ALERT — C2 Hub Dismantled",
            "description": (
                f"**`{ip}`** has been **null-routed** via AWS Network Firewall.\n"
                f"Botnet cluster of **{bot_count} nodes** dismantled in this sweep."
            ),
            "color": colour,
            "fields": [
                {"name": "Blocked IP",       "value": f"`{ip}`",          "inline": True},
                {"name": "Bot Nodes",        "value": str(bot_count),     "inline": True},
                {"name": "Threat Level",     "value": f"**{lvl}**",       "inline": True},
                {"name": "Total Bytes",      "value": f"{bytes_total:,}", "inline": True},
                {"name": "Hubs This Run",    "value": str(total_hubs_in_run), "inline": True},
                {"name": "Terraform",        "value": f"`{OUTPUT_TF_FILE}` written", "inline": True},
            ],
            "footer":      {"text": f"Aura-Sentinel Reaper v1.0  •  {ts}"},
            "thumbnail":   {"url": "https://i.imgur.com/iQYoF0x.png"},  # skull icon
        }
        payload = {
            "username":   "Aura-Sentinel Reaper",
            "avatar_url": "https://i.imgur.com/iQYoF0x.png",
            "embeds":     [embed],
        }
        try:
            r = requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
            r.raise_for_status()
            print(f"  {GREEN}🔔  Discord alert sent → {ip}{RESET}")
        except Exception as e:
            print(f"  {YELLOW}⚠  Discord webhook failed: {e}{RESET}")

    # ── Slack message ─────────────────────────────────────────────────────────
    if SLACK_WEBHOOK_URL:
        slack_payload = {
            "text": (
                f"🚨 *REAPER ALERT*: Blocked `{ip}`. "
                f"Botnet cluster of *{bot_count} nodes* dismantled. "
                f"Threat level: *{lvl}* | Bytes: {bytes_total:,}"
            )
        }
        try:
            r = requests.post(SLACK_WEBHOOK_URL, json=slack_payload, timeout=5)
            r.raise_for_status()
            print(f"  {GREEN}🔔  Slack alert sent → {ip}{RESET}")
        except Exception as e:
            print(f"  {YELLOW}⚠  Slack webhook failed: {e}{RESET}")

    if not DISCORD_WEBHOOK_URL and not SLACK_WEBHOOK_URL:
        print(f"  {DIM}[webhook] No DISCORD_WEBHOOK_URL or SLACK_WEBHOOK_URL set — skipping alerts.{RESET}")
        print(f"  {DIM}          Export the env var and re-run to enable live notifications.{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# Graph Queries via HTTP Transaction API
# ─────────────────────────────────────────────────────────────────────────────

SESSION = requests.Session()
SESSION.auth = HTTPBasicAuth(NEO4J_USER, NEO4J_PASS)
SESSION.headers.update({"Content-Type": "application/json", "Accept": "application/json"})


def run_cypher(cypher: str, params: dict = None) -> list[dict]:
    """Execute a Cypher query via Neo4j HTTP Transaction API and return rows."""
    url = f"{NEO4J_HTTP}/db/data/transaction/commit"
    payload = {
        "statements": [
            {
                "statement": cypher,
                "parameters": params or {},
                "resultDataContents": ["row", "graph"]
            }
        ]
    }
    resp = SESSION.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    # Surface any Cypher-level errors
    errors = data.get("errors", [])
    if errors:
        raise RuntimeError(f"Cypher error: {errors}")

    # Flatten rows into list-of-dicts
    results = []
    for stmt in data.get("results", []):
        cols = stmt["columns"]
        for row in stmt["data"]:
            results.append(dict(zip(cols, row["row"])))
    return results


def check_neo4j_connection():
    """Lightweight health check against the HTTP API."""
    resp = SESSION.get(f"{NEO4J_HTTP}/", timeout=5)
    resp.raise_for_status()
    info = resp.json()
    return info.get("neo4j_version", "unknown")


def find_c2_hubs():
    """
    Returns all IPs that receive data from ≥ MIN_BOT_CONNECTIONS unique bots.
    Ordered by threat severity (highest connection count first).
    """
    cypher = """
    MATCH (bot:IP)-[r:SENT_DATA]->(hub:IP)
    WITH hub,
         count(DISTINCT bot) AS connection_count,
         sum(r.bytes)        AS total_bytes
    WHERE connection_count >= $threshold
    RETURN hub.address   AS c2_ip,
           connection_count,
           total_bytes
    ORDER BY connection_count DESC
    """
    return run_cypher(cypher, {"threshold": MIN_BOT_CONNECTIONS})


def label_hub_in_graph(ip: str, lvl: str):
    """
    Tags the Neo4j node with metadata so future queries can filter by label.
    This is the equivalent of a SIEM 'case update'.
    """
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat()
    cypher = """
    MATCH (n:IP {address: $ip})
    SET n.threat_level  = $lvl,
        n.remediated_at = $ts
    WITH n
    CALL apoc.create.addLabels(n, ['C2Hub']) YIELD node
    RETURN node
    """
    # APOC may not be available; fall back to plain SET
    try:
        run_cypher(cypher, {"ip": ip, "lvl": lvl, "ts": ts})
    except Exception:
        simple = """
        MATCH (n:IP {address: $ip})
        SET n.threat_level  = $lvl,
            n.remediated_at = $ts
        """
        run_cypher(simple, {"ip": ip, "lvl": lvl, "ts": ts})


# ─────────────────────────────────────────────────────────────────────────────
# Terraform Generator
# ─────────────────────────────────────────────────────────────────────────────

def threat_level(connection_count: int) -> str:
    if connection_count >= 10:
        return "CRITICAL"
    elif connection_count >= 6:
        return "HIGH"
    elif connection_count >= 3:
        return "MEDIUM"
    return "LOW"


def generate_terraform(hubs: list[dict]) -> str:
    """
    Produces valid AWS Network Firewall HCL that null-routes every C2 hub.
    One Suricata stateful rule per IP, bundled into a single rule group.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    # --- Header block ---
    header = f"""# ═══════════════════════════════════════════════════════════
# Aura-Sentinel  |  AUTO-GENERATED REMEDIATION CONFIG
# Generated  : {datetime.now(timezone.utc).isoformat()}
# Threat IPs : {len(hubs)} C2 hub(s) identified
# DO NOT EDIT manually — managed by remediation_agent.py
# ═══════════════════════════════════════════════════════════

terraform {{
  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }}
  }}
}}

provider "aws" {{
  region = var.aws_region
}}

variable "aws_region" {{
  description = "AWS region to deploy firewall rules"
  type        = string
  default     = "us-east-1"
}}

"""

    # --- One rule group containing ALL blocked IPs via Suricata rules ---
    rules_source_list = "\n".join(
        f'      "{hub["c2_ip"]}"  # {threat_level(hub["connection_count"])} — '
        f'{hub["connection_count"]} bots, {hub["total_bytes"]} bytes'
        for hub in hubs
    )

    # Suricata-compatible stateful rules — one DROP per C2 IP
    suricata_rules = "\n".join(
        f'drop ip any any -> {hub["c2_ip"]} any '
        f'(msg:"AURA-SENTINEL C2 block {hub["c2_ip"]}"; '
        f'sid:{10000 + i}; rev:1;)'
        for i, hub in enumerate(hubs)
    )

    rule_group_block = f"""resource "aws_networkfirewall_rule_group" "aura_sentinel_block_c2" {{
  capacity = {max(100, len(hubs) * 50)}
  name     = "aura-sentinel-c2-block-{timestamp}"
  type     = "STATEFUL"
  description = "Auto-generated by Aura-Sentinel Reaper Agent"

  rule_group {{
    rules_source {{
      rules_string = <<-EOT
{suricata_rules}
      EOT
    }}
    stateful_rule_options {{
      rule_order = "STRICT_ORDER"
    }}
  }}

  tags = {{
    ManagedBy   = "aura-sentinel"
    GeneratedAt = "{datetime.now(timezone.utc).isoformat()}"
    ThreatClass = "C2-Botnet-Hub"
  }}
}}

"""

    # --- Firewall policy attachment ---
    policy_block = f"""resource "aws_networkfirewall_firewall_policy" "aura_sentinel_policy" {{
  name = "aura-sentinel-policy-{timestamp}"

  firewall_policy {{
    stateless_default_actions          = ["aws:forward_to_sfe"]
    stateless_fragment_default_actions = ["aws:forward_to_sfe"]

    stateful_rule_group_reference {{
      priority     = 1
      resource_arn = aws_networkfirewall_rule_group.aura_sentinel_block_c2.arn
    }}
  }}

  tags = {{
    ManagedBy = "aura-sentinel"
  }}
}}

"""

    # --- Outputs ---
    outputs_block = """output "blocked_ips" {
  description = "IPs null-routed by this deployment"
  value       = aws_networkfirewall_rule_group.aura_sentinel_block_c2.name
}

output "policy_arn" {
  description = "ARN of the enforced firewall policy"
  value       = aws_networkfirewall_firewall_policy.aura_sentinel_policy.arn
}
"""

    return header + rule_group_block + policy_block + outputs_block


# ─────────────────────────────────────────────────────────────────────────────
# Report Generator
# ─────────────────────────────────────────────────────────────────────────────

def generate_report(hubs: list[dict]) -> dict:
    return {
        "agent":        "Aura-Sentinel Reaper v1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "threshold":    MIN_BOT_CONNECTIONS,
        "total_threats": len(hubs),
        "threats": [
            {
                "ip":               h["c2_ip"],
                "bot_count":        h["connection_count"],
                "total_bytes":      h["total_bytes"],
                "threat_level":     threat_level(h["connection_count"]),
                "action":           "NULL_ROUTE_GENERATED",
                "remediation_file": OUTPUT_TF_FILE,
            }
            for h in hubs
        ]
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def print_banner():
    print(f"\n{BOLD}{CYAN}╔══════════════════════════════════════════════════════════════╗")
    print(f"║        🔱  AURA-SENTINEL  |  Remediation Agent  v1.0        ║")
    print(f"║        Phase 5 — The Reaper  |  Automated Kill-Switch        ║")
    print(f"╚══════════════════════════════════════════════════════════════╝{RESET}\n")


def main():
    print_banner()

    # 1. Connect to Neo4j (HTTP)
    print(f"{DIM}[*] Connecting to Neo4j HTTP API at {NEO4J_HTTP}...{RESET}")
    try:
        version = check_neo4j_connection()
        print(f"{GREEN}[✓] Neo4j {version} — HTTP API healthy.{RESET}\n")
    except Exception as e:
        print(f"{RED}[✗] Cannot reach Neo4j HTTP API: {e}{RESET}")
        sys.exit(1)

    # 2. Hunt for C2 hubs
    print(f"{YELLOW}[🔍] Querying graph for C2 hubs (≥{MIN_BOT_CONNECTIONS} incoming bot connections)...{RESET}")
    try:
        hubs = find_c2_hubs()
    except Exception as e:
        print(f"{RED}[✗] Graph query failed: {e}{RESET}")
        sys.exit(1)

    if not hubs:
        print(f"\n{GREEN}[🟢] Network Clear — No C2 clusters detected above threshold.{RESET}")
        print(f"     Try running the producer again to populate more traffic data.\n")
        return

    # 3. Print threat table
    print(f"\n{RED}{BOLD}[🎯] {len(hubs)} C2 Hub(s) Identified:{RESET}")
    print(f"{'─'*62}")
    print(f"  {'IP Address':<20}  {'Bots':>6}  {'Total Bytes':>12}  {'Level'}")
    print(f"{'─'*62}")
    for hub in hubs:
        lvl = threat_level(hub['connection_count'])
        colour = RED if lvl in ("CRITICAL","HIGH") else YELLOW
        print(
            f"  {hub['c2_ip']:<20}  "
            f"{hub['connection_count']:>6}  "
            f"{hub['total_bytes']:>12,}  "
            f"{colour}{lvl}{RESET}"
        )
    print(f"{'─'*62}\n")

    # 4. Label hubs in Neo4j
    print(f"[*] Tagging C2 nodes in Neo4j graph...")
    for hub in hubs:
        lvl = threat_level(hub['connection_count'])
        try:
            label_hub_in_graph(hub['c2_ip'], lvl)
            print(f"    {GREEN}✓{RESET}  {hub['c2_ip']} → tagged threat_level={lvl}")
        except Exception as e:
            print(f"    {YELLOW}⚠{RESET}  {hub['c2_ip']} → tagging skipped ({e})")

    # 5. Generate Terraform
    print(f"\n[*] Generating Terraform null-route configuration...")
    tf_content = generate_terraform(hubs)
    with open(OUTPUT_TF_FILE, "w") as f:
        f.write(tf_content)
    print(f"  {GREEN}🛡️  {OUTPUT_TF_FILE} written — {len(hubs)} rule(s) generated.{RESET}")

    # 6. Write JSON report
    report = generate_report(hubs)
    with open(REPORT_FILE, "w") as f:
        json.dump(report, f, indent=2)
    print(f"  {GREEN}📋  {REPORT_FILE} written — audit trail saved.{RESET}")

    # 7. Fire Discord / Slack webhook and update Prometheus metrics for each blocked hub
    print(f"\n[*] Dispatching kill-switch notifications...")
    for hub in hubs:
        send_reaper_alert(hub, total_hubs_in_run=len(hubs))
        # Update Grafana dashboard
        try:
            requests.post("http://localhost:8000/dismantle", timeout=2)
            print(f"  {GREEN}📈  Grafana metric updated → C2 Hub Dismantled{RESET}")
        except Exception as e:
            print(f"  {YELLOW}⚠   Could not update Grafana metric (is metrics-exporter port-forwarded?): {e}{RESET}")

    # 8. Print next steps
    print(f"""
{BOLD}{CYAN}══════════════  DEPLOYMENT INSTRUCTIONS  ══════════════{RESET}
  To apply these firewall rules against a real AWS account:

    terraform init
    terraform plan   -out=reaper.plan
    terraform apply  reaper.plan

  {DIM}(Requires: AWS credentials with NetworkFirewall:* permissions){RESET}

  To enable live Discord alerts:
    export DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/<id>/<token>"

  To enable live Slack alerts:
    export SLACK_WEBHOOK_URL="https://hooks.slack.com/services/<id>/<token>"

{BOLD}Reaper Agent complete.{RESET}  {len(hubs)} IP(s) queued for null-routing.
""")


from datetime import datetime, timezone

if __name__ == "__main__":
    main()
