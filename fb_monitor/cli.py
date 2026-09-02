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
    reconcile_photo = sub.add_parser(
        "reconcile-photo-batch",
        help="人工對帳 needs_reconcile 或放棄 import_failed Apify 照片批次",
    )
    reconcile_photo.add_argument("batch_id", type=int)
    photo_resolution = reconcile_photo.add_mutually_exclusive_group(required=True)
    photo_resolution.add_argument("--run-id", help="Apify 已啟動的 run ID")
    photo_resolution.add_argument(
        "--confirm-not-launched",
        action="store_true",
        help="明確確認 provider 未啟動 run；保留稽核紀錄並建立可重試 identity",
    )
    photo_resolution.add_argument(
        "--abandon-import-failed",
        action="store_true",
        help=(
            "Actor/schema 已人工切換後，放棄無法匯入的舊 raw；"
            "raw 與稽核紀錄保留，並建立新的安全重試 identity"
        ),
    )
    reconcile_photo.add_argument("--dataset-id", default="")
    reconcile_photo.add_argument("--key-value-store-id", default="")
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
    elif args.command == "reconcile-photo-batch":
        try:
            batch = db.reconcile_paid_photo_batch(
                args.batch_id,
                run_id=args.run_id,
                dataset_id=args.dataset_id,
                key_value_store_id=args.key_value_store_id,
                confirm_not_launched=args.confirm_not_launched,
                abandon_import_failed=args.abandon_import_failed,
            )
        except (RuntimeError, ValueError) as exc:
            raise SystemExit(str(exc)) from exc
        capture = db.row(
            "SELECT * FROM profile_photo_captures WHERE id=?",
            (batch["photo_capture_id"],),
        )
        if not capture:
            raise SystemExit("找不到照片回溯狀態")
        try:
            checkpoint = json.loads(capture.get("checkpoint_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            checkpoint = {}
        checkpoint = checkpoint if isinstance(checkpoint, dict) else {}
        if batch["status"] == "failed":
            checkpoint["actor_retry_nonce"] = int(
                checkpoint.get("actor_retry_nonce") or 0
            ) + 1
        now = datetime.now(UTC).isoformat()
        db.execute(
            """UPDATE profile_photo_captures SET status='in_progress',
            checkpoint_json=?,limited_reason=NULL,next_job_at=?,completed_at=NULL,
            updated_at=? WHERE id=?""",
            (
                json.dumps(checkpoint, ensure_ascii=False, sort_keys=True),
                now, now, capture["id"],
            ),
        )
        db.queue_unique_job(
            profile_id=int(batch["profile_id"]),
            job_type="capture_profile_photos",
            priority=-120,
            dedupe_key=f"capture-account-photos:{capture['id']}:reconcile:{batch['id']}",
            payload={
                "photo_capture_id": int(capture["id"]),
                "iteration": int(checkpoint.get("capture_iteration") or 0),
                "manual": True,
            },
            available_at=now,
        )
        print(
            json.dumps(
                {
                    "batch_id": batch["id"],
                    "status": batch["status"],
                    "run_id": batch.get("run_id"),
                    "message": (
                        "已連回既有 Apify run；只會完成該 run，不會重新購買"
                        if batch["status"] == "run_started"
                        else (
                            "已放棄無法匯入的舊 raw；稽核證據保留，已排入切換後的安全重試"
                            if args.abandon_import_failed
                            else "已確認未啟動；舊 request 保留，已排入新的安全重試 identity"
                        )
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
        photo_batches = db.rows(
            """SELECT id,profile_id,photo_capture_id,status,actor_id,input_cursor,
            output_cursor,max_charge_usd,charged_usd,run_id,dataset_id,
            raw_result_count,parsed_result_count,launched_at,raw_saved_at,
            imported_at,committed_at,updated_at,error
            FROM paid_photo_batches ORDER BY id DESC LIMIT 20"""
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
                "recent_paid_photo_batches": photo_batches,
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
