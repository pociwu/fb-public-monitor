from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime

import uvicorn

from .config import load_settings
from .db import Database


def main() -> None:
    parser = argparse.ArgumentParser(prog="fb-monitor")
    parser.add_argument("--config", default=None)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run")
    scan = sub.add_parser("scan")
    scan.add_argument("profile", help="profile ID、名稱或 Facebook URL")
    diagnose = sub.add_parser("diagnose")
    diagnose.add_argument("profile", nargs="?", help="可選：profile ID、名稱、顯示名稱或 Facebook URL")
    reconcile_probe = sub.add_parser(
        "reconcile-access-probe",
        help="人工對帳一筆 needs_reconcile Apify 公開探測",
    )
    reconcile_probe.add_argument("batch_id", type=int)
    resolution = reconcile_probe.add_mutually_exclusive_group(required=True)
    resolution.add_argument("--run-id", help="Apify 已啟動的 run ID")
    resolution.add_argument(
        "--confirm-not-launched",
        action="store_true",
        help="明確確認 provider 未啟動 run；此 request 會安全關閉為 failed",
    )
    reconcile_probe.add_argument("--dataset-id", default="")
    reconcile_probe.add_argument("--key-value-store-id", default="")
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
        db.queue_profile_visits([int(profile["id"])])
        print(f"已將 {profile['name']} 排到佇列最前端；仍遵守全域間隔與預算上限。")
    elif args.command == "reconcile-access-probe":
        try:
            batch = db.reconcile_paid_access_probe_batch(
                args.batch_id,
                run_id=args.run_id,
                dataset_id=args.dataset_id,
                key_value_store_id=args.key_value_store_id,
                confirm_not_launched=args.confirm_not_launched,
            )
        except (RuntimeError, ValueError) as exc:
            raise SystemExit(str(exc)) from exc
        print(
            json.dumps(
                {
                    "batch_id": batch["id"],
                    "status": batch["status"],
                    "run_id": batch.get("run_id"),
                    "message": (
                        "已連回既有 Apify run；下次排程只會完成與重播，不會重新購買"
                        if batch["status"] == "run_started"
                        else "已確認未啟動；原 request 已關閉，不會重新購買"
                    ),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
    elif args.command == "status":
        profiles = db.rows("SELECT id,name,display_name,fb_id,public_state,last_success_at,next_visit_at,consecutive_failures,last_error FROM profiles ORDER BY id")
        month = datetime.now(UTC).strftime("%Y-%m")
        contracts = db.rows(
            """SELECT provider,actor_id,purpose,status,schema_fingerprint,
            passed_at,expires_at,invalidated_at,evidence_json,updated_at
            FROM actor_contracts ORDER BY id DESC"""
        )
        epochs = db.rows(
            """SELECT ce.id,ce.profile_id,COALESCE(p.display_name,p.name) account,
            ce.trigger_reason,ce.status,ce.priority,ce.reserved_budget_usd,
            COALESCE((SELECT SUM(b.charged_usd) FROM paid_source_batches b
                      WHERE b.epoch_id=ce.id),0) spent_budget_usd,
            ce.created_at,ce.updated_at,ce.started_at,ce.completed_at,
            ce.terminal_reason
            FROM capture_epochs ce JOIN profiles p ON p.id=ce.profile_id
            ORDER BY ce.id DESC LIMIT 50"""
        )
        coverage = db.rows(
            """SELECT cs.epoch_id,cs.stream,cs.surface,cs.scope_type,cs.scope_id,
            cs.status,cs.provider,cs.input_cursor,cs.output_cursor,
            cs.seen_count,cs.new_count,cs.updated_count,cs.duplicate_count,
            cs.terminal_evidence_json,cs.limited_reason,cs.updated_at
            FROM coverage_streams cs ORDER BY cs.id DESC LIMIT 100"""
        )
        batches = db.rows(
            """SELECT id,profile_id,epoch_id,intent,status,actor_id,input_cursor,
            output_cursor,raw_result_count,parsed_result_count,new_result_count,
            updated_result_count,duplicate_result_count,charged_usd,run_id,
            launched_at,raw_saved_at,imported_at,committed_at,updated_at,error
            FROM paid_source_batches
            ORDER BY id DESC LIMIT 20"""
        )
        access_probes = db.rows(
            """SELECT id,profile_id,status,actor_id,observation_window,
            max_charge_usd,charged_usd,run_id,dataset_id,raw_result_count,
            parsed_result_count,launched_at,raw_saved_at,imported_at,
            committed_at,updated_at,error
            FROM paid_access_probe_batches ORDER BY id DESC LIMIT 20"""
        )
        print(json.dumps({
            "capture_v2": {
                "enabled": settings.capture_v2_enabled,
                "v1_backfill_enabled": settings.apify_v1_backfill_enabled,
                "contracts": contracts,
                "epochs": epochs,
                "coverage": coverage,
                "recent_paid_batches": batches,
                "recent_paid_access_probes": access_probes,
            },
            "profiles": profiles,
            "serpapi_usage": db.serpapi_usage_snapshot(),
            "apify_official_usage": db.apify_usage_snapshot(),
            "apify_month": month,
            "apify_estimated_usd": db.usage_total(month),
        }, ensure_ascii=False, indent=2))
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
