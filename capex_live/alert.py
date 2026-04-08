"""
alert.py — Detect new/changed incentive programs and send email alerts.

Usage:
    python alert.py --email you@company.com
    # or set EMAIL_TO environment variable

Schedule weekly with cron:
    0 8 * * 1 cd /path/to/scraper && python scraper.py && python alert.py
"""

import sqlite3
import argparse
import os
import smtplib
import json
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path("incentives.db")


def get_recent_programs(conn, days=7):
    cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()
    rows = conn.execute("""
        SELECT name, org, program_type, state, equipment,
               incentive_amount, source_url, first_seen, last_updated
        FROM programs
        WHERE first_seen > ? OR last_updated > ?
        ORDER BY first_seen DESC
    """, (cutoff, cutoff)).fetchall()
    return rows


def build_email_html(programs):
    if not programs:
        return "<p>No new or updated incentive programs found this week.</p>"

    rows_html = ""
    for p in programs:
        name, org, ptype, state, equip_json, amt, url, first_seen, updated = p
        try:
            equips = ", ".join(json.loads(equip_json))
        except Exception:
            equips = equip_json or ""
        is_new = "NEW" if first_seen == updated else "UPDATED"
        tag_color = "#1D9E75" if is_new == "NEW" else "#BA7517"
        rows_html += f"""
        <tr>
          <td style="padding:8px;border-bottom:1px solid #eee;">
            <span style="background:{tag_color};color:#fff;font-size:11px;padding:2px 6px;border-radius:10px;margin-right:6px">{is_new}</span>
            <a href="{url}" style="color:#185FA5;text-decoration:none;font-weight:500">{name}</a>
          </td>
          <td style="padding:8px;border-bottom:1px solid #eee;font-size:13px">{org}</td>
          <td style="padding:8px;border-bottom:1px solid #eee;font-size:13px">{state}</td>
          <td style="padding:8px;border-bottom:1px solid #eee;font-size:13px">{equips}</td>
          <td style="padding:8px;border-bottom:1px solid #eee;font-size:13px;font-weight:500;color:#3B6D11">{amt}</td>
        </tr>
        """

    return f"""
    <html><body style="font-family:sans-serif;color:#222">
      <h2 style="color:#185FA5">Energy Incentive Tracker — Weekly Digest</h2>
      <p style="color:#666">{len(programs)} programs updated since last scan.</p>
      <table style="width:100%;border-collapse:collapse;font-size:14px">
        <thead>
          <tr style="background:#f5f5f5">
            <th style="text-align:left;padding:8px">Program</th>
            <th style="text-align:left;padding:8px">Organization</th>
            <th style="text-align:left;padding:8px">State</th>
            <th style="text-align:left;padding:8px">Equipment</th>
            <th style="text-align:left;padding:8px">Incentive</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
      <p style="font-size:12px;color:#999;margin-top:24px">
        Generated {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')} · 
        Edit alert.py to change recipients or frequency.
      </p>
    </body></html>
    """


def send_email(to_addr, html_body,
               smtp_host="smtp.gmail.com", smtp_port=587,
               smtp_user=None, smtp_pass=None):
    smtp_user = smtp_user or os.getenv("SMTP_USER", "")
    smtp_pass = smtp_pass or os.getenv("SMTP_PASS", "")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Energy Incentives Weekly Digest — {datetime.utcnow().strftime('%b %d %Y')}"
    msg["From"] = smtp_user
    msg["To"] = to_addr
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(smtp_user, smtp_pass)
        server.sendmail(smtp_user, to_addr, msg.as_string())
    print(f"Alert email sent to {to_addr}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--email", default=os.getenv("EMAIL_TO", ""))
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--print-only", action="store_true", help="Print HTML to stdout instead of sending")
    args = parser.parse_args()

    if not DB_PATH.exists():
        print("incentives.db not found — run scraper.py first.")
        exit(1)

    conn = sqlite3.connect(DB_PATH)
    programs = get_recent_programs(conn, days=args.days)
    print(f"Found {len(programs)} programs updated in last {args.days} days.")

    html = build_email_html(programs)

    if args.print_only or not args.email:
        print(html[:2000])
    else:
        send_email(args.email, html)
