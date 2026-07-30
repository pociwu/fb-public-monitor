from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

import uvicorn

from .config import load_settings
from .db import Database, utcnow


def main() -> None:
    parser = argparse.ArgumentParser(prog="fb-monitor")
    parser.add_argument("--config", default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run")
    scan = sub.add_parser("scan")
    scan.add_argument("profile", help="profile ID、名稱或 Facebook URL")
    diagnose = sub.add_parser("diagnose")
    diagnose.add_argument("profile", nargs="?", help="可選：profile ID、名稱、顯示名稱或 Facebook URL")
    sub.add_parser("status")
    args = parser.parse_args()
    settings = load_settings(args.config)
    if args.command == "run":
        from .web import create_app
        uvicorn.run(create_app(settings), host=settings.web_host, port=settings.web_port, log_level="info")
        return
    db = Database(settings.db_path)
    db.sync_profiles(settings.profiles)
    if args.command == "scan":
        profile = db.row("SELECT * FROM profiles WHERE CAST(id AS TEXT)=? OR name=? OR display_name=? OR url=?", (args.profile, args.profile, args.profile, args.profile.rstrip("/")))
        if not profile:
            raise SystemExit("找不到指定 profile")
        pending = db.row("SELECT id FROM jobs WHERE profile_id=? AND job_type='visit' AND status='pending' ORDER BY id LIMIT 1", (profile["id"],))
        if pending:
            db.execute("UPDATE jobs SET priority=0,available_at=? WHERE id=?", (utcnow(), pending["id"]))
        else:
            db.execute(
                "INSERT INTO jobs(profile_id,job_type,priority,payload_json,available_at,created_at) VALUES(?,'visit',0,'{}',?,?)",
                (profile["id"], utcnow(), utcnow()),
            )
        print(f"已將 {profile['name']} 排到佇列最前端；仍遵守全域間隔與預算上限。")
    elif args.command == "status":
        profiles = db.rows("SELECT id,name,display_name,fb_id,public_state,last_success_at,next_visit_at,consecutive_failures,last_error FROM profiles ORDER BY id")
        month = datetime.now(UTC).strftime("%Y-%m")
        print(json.dumps({"profiles": profiles, "apify_month": month, "apify_estimated_usd": db.usage_total(month)}, ensure_ascii=False, indent=2))
    elif args.command == "diagnose":
        profile = None
        if args.profile:
            profile = db.row("SELECT * FROM profiles WHERE CAST(id AS TEXT)=? OR name=? OR display_name=? OR url=?", (args.profile, args.profile, args.profile, args.profile.rstrip("/")))
            if not profile:
                raise SystemExit("找不到指定 profile")
        params = (profile["id"],) if profile else ()
        where = "WHERE profile_id=?" if profile else ""
        runs = db.rows(f"SELECT * FROM actor_runs {where} ORDER BY id DESC LIMIT 50", params)
        for run in runs:
            for key in ("input_json", "summary_json", "samples_json"):
                if run.get(key):
                    run[key.removesuffix("_json")] = json.loads(run[key])
                run.pop(key, None)
        print(json.dumps({"profile": profile.get("display_name") or profile.get("name") if profile else None, "runs": runs}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
