from collections import OrderedDict
from io import BytesIO
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4
from zipfile import ZIP_DEFLATED, ZipFile

import pandas as pd
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from 共通スクリプト.Excel出力.excel_exporter import build_excel_bytes
from 共通スクリプト.analysis_service import (
    DEFAULT_ANALYSIS_KEYS,
    analyze_prepared_event_log,
    create_pattern_flow_snapshot,
    create_pattern_bottleneck_details,
    get_available_analysis_definitions,
    load_prepared_event_log,
)


BASE_DIR = Path(__file__).resolve().parent
SAMPLE_FILE = BASE_DIR / "sample_event_log.csv"
MAX_STORED_RUNS = 5
PREVIEW_ROW_COUNT = 10
PROCESS_FLOW_PATTERN_CAP = 300
MAX_PATTERN_FLOW_CACHE = 24

DEFAULT_HEADERS = {
    "case_id_column": "case_id",
    "activity_column": "activity",
    "timestamp_column": "start_time",
}

app = FastAPI(title="Process Mining Workbench")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
RUN_STORE = OrderedDict()


def get_static_version():
    static_dir = BASE_DIR / "static"
    return str(
        max(
            entry.stat().st_mtime_ns
            for entry in static_dir.iterdir()
            if entry.is_file()
        )
    )


def save_run_data(source_file_name, selected_analysis_keys, prepared_df, result):
    run_id = uuid4().hex
    RUN_STORE[run_id] = {
        "source_file_name": source_file_name,
        "selected_analysis_keys": selected_analysis_keys,
        "prepared_df": prepared_df,
        "result": result,
        "pattern_flow_cache": OrderedDict(),
    }
    RUN_STORE.move_to_end(run_id)

    while len(RUN_STORE) > MAX_STORED_RUNS:
        RUN_STORE.popitem(last=False)

    return run_id


def get_run_data(run_id):
    run_data = RUN_STORE.get(run_id)

    if not run_data:
        raise HTTPException(status_code=404, detail="分析セッションが見つかりません。再分析してください。")

    RUN_STORE.move_to_end(run_id)
    return run_data


def build_analysis_payload(analysis, row_limit=None, row_offset=0):
    total_row_count = len(analysis["rows"])
    safe_row_offset = max(0, int(row_offset or 0))

    if row_limit is None:
        safe_row_limit = total_row_count
        rows = analysis["rows"][safe_row_offset:]
    else:
        safe_row_limit = max(0, int(row_limit))
        rows = analysis["rows"][safe_row_offset : safe_row_offset + safe_row_limit]

    page_end_row_number = safe_row_offset + len(rows)
    has_previous_page = safe_row_offset > 0
    has_next_page = page_end_row_number < total_row_count
    previous_row_offset = max(0, safe_row_offset - safe_row_limit) if has_previous_page else None
    next_row_offset = page_end_row_number if has_next_page else None

    return {
        "analysis_name": analysis["analysis_name"],
        "sheet_name": analysis["sheet_name"],
        "output_file_name": analysis.get("output_file_name"),
        "row_count": total_row_count,
        "returned_row_count": len(rows),
        "row_offset": safe_row_offset,
        "page_size": safe_row_limit,
        "page_start_row_number": safe_row_offset + 1 if rows else 0,
        "page_end_row_number": page_end_row_number,
        "has_previous_page": has_previous_page,
        "has_next_page": has_next_page,
        "previous_row_offset": previous_row_offset,
        "next_row_offset": next_row_offset,
        "rows": rows,
        "excel_file": analysis["excel_file"],
    }


def get_pattern_flow_snapshot(
    run_data,
    pattern_percent,
    pattern_count,
    activity_percent,
    connection_percent,
):
    cache_key = (
        int(pattern_percent),
        None if pattern_count is None else int(pattern_count),
        int(activity_percent),
        int(connection_percent),
    )
    cache = run_data.setdefault("pattern_flow_cache", OrderedDict())

    cached_snapshot = cache.get(cache_key)
    if cached_snapshot is not None:
        cache.move_to_end(cache_key)
        return cached_snapshot

    analyses = run_data["result"]["analyses"]
    pattern_analysis = analyses.get("pattern")
    snapshot = create_pattern_flow_snapshot(
        pattern_rows=pattern_analysis["rows"],
        frequency_rows=analyses.get("frequency", {}).get("rows", []),
        pattern_percent=pattern_percent,
        pattern_count=pattern_count,
        activity_percent=activity_percent,
        connection_percent=connection_percent,
        pattern_cap=PROCESS_FLOW_PATTERN_CAP,
    )

    cache[cache_key] = snapshot
    cache.move_to_end(cache_key)
    while len(cache) > MAX_PATTERN_FLOW_CACHE:
        cache.popitem(last=False)

    return snapshot


def build_preview_response(run_id, source_file_name, selected_analysis_keys, result):
    return {
        "run_id": run_id,
        "source_file_name": source_file_name,
        "selected_analysis_keys": selected_analysis_keys,
        "case_count": result["case_count"],
        "event_count": result["event_count"],
        "analyses": {
            analysis_key: build_analysis_payload(analysis, PREVIEW_ROW_COUNT)
            for analysis_key, analysis in result["analyses"].items()
        },
    }


def get_analysis_options():
    analysis_definitions = get_available_analysis_definitions()
    analysis_options = []

    for analysis_key in DEFAULT_ANALYSIS_KEYS:
        analysis_options.append(
            {
                "key": analysis_key,
                "label": analysis_definitions[analysis_key]["config"]["analysis_name"],
            }
        )

    return analysis_options


@app.get("/")
def index(request: Request):
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "analysis_options": get_analysis_options(),
            "default_headers": DEFAULT_HEADERS,
            "sample_file_name": SAMPLE_FILE.name,
            "static_version": get_static_version(),
        },
    )


@app.get("/analysis/patterns/{pattern_index}")
def pattern_detail_page(request: Request, pattern_index: int):
    return templates.TemplateResponse(
        "pattern_detail.html",
        {
            "request": request,
            "pattern_index": pattern_index,
            "static_version": get_static_version(),
        },
    )


@app.get("/analysis/{analysis_key}")
def analysis_detail(request: Request, analysis_key):
    analysis_definitions = get_available_analysis_definitions()

    if analysis_key not in analysis_definitions:
        raise HTTPException(status_code=404, detail="分析が見つかりません。")

    return templates.TemplateResponse(
        "analysis_detail.html",
        {
            "request": request,
            "analysis_key": analysis_key,
            "analysis_name": analysis_definitions[analysis_key]["config"]["analysis_name"],
            "static_version": get_static_version(),
        },
    )


@app.get("/api/runs/{run_id}/patterns/{pattern_index}")
def pattern_detail_api(run_id: str, pattern_index: int):
    run_data = get_run_data(run_id)
    pattern_analysis = run_data["result"]["analyses"].get("pattern")

    if not pattern_analysis:
        raise HTTPException(status_code=400, detail="処理順パターン分析が実行されていません。")

    pattern_rows = pattern_analysis["rows"]
    if pattern_index < 0 or pattern_index >= len(pattern_rows):
        raise HTTPException(status_code=404, detail="処理順パターン行が見つかりません。")

    summary_row = pattern_rows[pattern_index]
    pattern = summary_row.get("処理順パターン")
    if not pattern:
        raise HTTPException(status_code=500, detail="処理順パターン列の値を取得できません。")

    detail = create_pattern_bottleneck_details(run_data["prepared_df"], pattern)
    return JSONResponse(
        content={
            "run_id": run_id,
            "pattern_index": pattern_index,
            "source_file_name": run_data["source_file_name"],
            "analysis_name": pattern_analysis["analysis_name"],
            "summary_row": summary_row,
            **detail,
        }
    )


@app.get("/api/runs/{run_id}/analyses/{analysis_key}")
def analysis_detail_api(
    run_id: str,
    analysis_key: str,
    row_limit: int | None = None,
    row_offset: int = 0,
):
    run_data = get_run_data(run_id)
    analyses = run_data["result"]["analyses"]
    analysis = analyses.get(analysis_key)

    if not analysis:
        raise HTTPException(status_code=404, detail="指定した分析結果が見つかりません。")

    response_analyses = {
        analysis_key: build_analysis_payload(
            analysis,
            row_limit=row_limit,
            row_offset=row_offset,
        )
    }

    return JSONResponse(
        content={
            "run_id": run_id,
            "source_file_name": run_data["source_file_name"],
            "selected_analysis_keys": run_data["selected_analysis_keys"],
            "case_count": run_data["result"]["case_count"],
            "event_count": run_data["result"]["event_count"],
            "analyses": response_analyses,
        }
    )


@app.get("/api/runs/{run_id}/excel-files/{analysis_key}")
def analysis_excel_file_api(run_id: str, analysis_key: str):
    run_data = get_run_data(run_id)
    analyses = run_data["result"]["analyses"]
    analysis = analyses.get(analysis_key)

    if not analysis:
        raise HTTPException(status_code=404, detail="指定した分析結果が見つかりません。")

    excel_df = pd.DataFrame(analysis["rows"])
    excel_bytes = build_excel_bytes(excel_df, analysis["sheet_name"])
    output_file_name = analysis.get("output_file_name") or f"{analysis['analysis_name']}.xlsx"

    return Response(
        content=excel_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(output_file_name)}",
        },
    )


@app.get("/api/runs/{run_id}/excel-archive")
def analysis_excel_archive_api(run_id: str):
    run_data = get_run_data(run_id)
    analyses = run_data["result"]["analyses"]

    archive_buffer = BytesIO()
    with ZipFile(archive_buffer, mode="w", compression=ZIP_DEFLATED) as archive_file:
        for analysis in analyses.values():
            excel_df = pd.DataFrame(analysis["rows"])
            output_file_name = analysis.get("output_file_name") or f"{analysis['analysis_name']}.xlsx"
            excel_bytes = build_excel_bytes(excel_df, analysis["sheet_name"])
            archive_file.writestr(output_file_name, excel_bytes)

    archive_file_name = f"{Path(run_data['source_file_name']).stem}_analysis_excels.zip"

    return Response(
        content=archive_buffer.getvalue(),
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename*=UTF-8''{quote(archive_file_name)}",
        },
    )


@app.get("/api/runs/{run_id}/pattern-flow")
def pattern_flow_api(
    run_id: str,
    pattern_percent: int = 10,
    pattern_count: int | None = None,
    activity_percent: int = 40,
    connection_percent: int = 30,
):
    run_data = get_run_data(run_id)
    analyses = run_data["result"]["analyses"]
    pattern_analysis = analyses.get("pattern")

    if not pattern_analysis:
        raise HTTPException(status_code=400, detail="処理順パターン分析が実行されていません。")

    snapshot = get_pattern_flow_snapshot(
        run_data=run_data,
        pattern_percent=pattern_percent,
        pattern_count=pattern_count,
        activity_percent=activity_percent,
        connection_percent=connection_percent,
    )

    return JSONResponse(
        content={
            "run_id": run_id,
            **snapshot,
        }
    )


@app.post("/api/analyze")
async def analyze(request: Request):
    form = await request.form()
    uploaded_file = form.get("csv_file")
    case_id_column = (form.get("case_id_column") or DEFAULT_HEADERS["case_id_column"]).strip()
    activity_column = (form.get("activity_column") or DEFAULT_HEADERS["activity_column"]).strip()
    timestamp_column = (form.get("timestamp_column") or DEFAULT_HEADERS["timestamp_column"]).strip()
    selected_analysis_keys = form.getlist("analysis_keys")

    if uploaded_file and uploaded_file.filename:
        uploaded_file.file.seek(0)
        file_source = uploaded_file.file
        source_file_name = uploaded_file.filename
    else:
        file_source = SAMPLE_FILE
        source_file_name = SAMPLE_FILE.name

    try:
        prepared_df = load_prepared_event_log(
            file_source=file_source,
            case_id_column=case_id_column,
            activity_column=activity_column,
            timestamp_column=timestamp_column,
        )
        result = analyze_prepared_event_log(
            prepared_df=prepared_df,
            selected_analysis_keys=selected_analysis_keys,
            output_root_dir=None,
            export_excel=False,
        )
        run_id = save_run_data(
            source_file_name=source_file_name,
            selected_analysis_keys=selected_analysis_keys,
            prepared_df=prepared_df,
            result=result,
        )
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "error": "分析中に予期しないエラーが発生しました。",
                "detail": str(exc),
            },
        )

    return JSONResponse(
        content=build_preview_response(
            run_id=run_id,
            source_file_name=source_file_name,
            selected_analysis_keys=selected_analysis_keys,
            result=result,
        )
    )


if __name__ == "__main__":
    uvicorn.run("web_app:app", host="127.0.0.1", port=5000, reload=True)
