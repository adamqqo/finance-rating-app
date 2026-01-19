from __future__ import annotations

import argparse

from .logging_config import setup_logging


def _lazy_imports():
    # Delay heavier imports until after logging is configured. This prevents
    # module-level log handlers (if any) from attaching to STDERR before we
    # route logs to STDOUT.
    from .db import get_conn, ensure_schema
    from .etl import runner, rpo_bulk, ruz_units, ruz_statements, ruz_reports

    return get_conn, ensure_schema, runner, rpo_bulk, ruz_units, ruz_statements, ruz_reports


def main() -> None:
    setup_logging()

    ap = argparse.ArgumentParser(prog="findexio", description="Findexio ETL")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("schema", help="Create/ensure DB schema")
    sub.add_parser("bootstrap", help="RPO (INIT+DAILY) + RUZ pipeline")
    sub.add_parser("daily", help="Cron-friendly daily pipeline")
    sub.add_parser("update02", help="Update report items and templates")
    sub.add_parser("fin_ddl_run", help="DDL Pipeline")
    sub.add_parser("fin_etl_run", help="ETL Pipeline")
    sub.add_parser("backfill-report-items-2024", help="2024 Report Items")

    p_rpo = sub.add_parser("rpo", help="Run only RPO bulk sync")
    p_rpo.add_argument("--no-daily", action="store_true")
    p_rpo.add_argument("--max-daily", type=int, default=None)
    p_rpo.add_argument("--reset-init", action="store_true")
    p_rpo.add_argument("--reset-daily", action="store_true")
    p_rpo.add_argument("--reset-all", action="store_true")

    p_u = sub.add_parser("ruz-units", help="Run only RUZ units sync")
    p_u.add_argument("--since", type=str, default=None)
    p_u.add_argument("--limit-ids", type=int, default=None)
    p_u.add_argument("--page-size", type=int, default=1000)

    p_s = sub.add_parser("ruz-statements", help="Run only RUZ statements sync")
    p_s.add_argument("--batch-size", type=int, default=1000)
    p_s.add_argument("--refresh-all", action="store_true")
    p_s.add_argument("--hard-limit", type=int, default=None)

    p_r = sub.add_parser("ruz-reports", help="Run only RUZ reports sync")
    p_r.add_argument("--batch-size", type=int, default=1000)
    p_r.add_argument("--refresh-all", action="store_true")
    p_r.add_argument("--hard-limit", type=int, default=None)

    args = ap.parse_args()

    get_conn, ensure_schema, runner, rpo_bulk, ruz_units, ruz_statements, ruz_reports = _lazy_imports()

    if args.cmd == "schema":
        with get_conn() as conn:
            ensure_schema(conn)
        return

    if args.cmd == "bootstrap":
        runner.bootstrap()
        return

    if args.cmd == "daily":
        runner.daily()
        return

    if args.cmd == "update02":
        runner.update02()
        return
    if args.cmd == "fin_ddl_run":
        runner.fin_ddl_run()
        return
    if args.cmd == "fin_etl_run":
        runner.fin_etl_run()
        return

    if args.cmd == "backfill-report-items-2024":
        runner.backfill_report_items_year(year=2024)
        return

    if args.cmd == "rpo":
        rpo_bulk.run_full_sync(
            apply_daily=not args.no_daily,
            max_daily=args.max_daily,
            reset_init=args.reset_init,
            reset_daily=args.reset_daily,
            reset_all=args.reset_all,
        )
        return

    if args.cmd == "ruz-units":
        ruz_units.run_sync(since=args.since, limit_ids=args.limit_ids, page_size=args.page_size)
        return

    if args.cmd == "ruz-statements":
        ruz_statements.run_sync(batch_size=args.batch_size, refresh_all=args.refresh_all, hard_limit=args.hard_limit)
        return


    if args.cmd == "ruz-reports":
        ruz_reports.run_sync(batch_size=args.batch_size, refresh_all=args.refresh_all, hard_limit=args.hard_limit)
        return


if __name__ == "__main__":
    main()
