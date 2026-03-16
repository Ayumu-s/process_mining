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
    create_analysis_records,
    create_bottleneck_summary,
    create_case_trace_details,
    create_log_diagnostics,
    create_pattern_index_entries,
    create_transition_case_drilldown,
    filter_prepared_df,
    filter_prepared_df_by_pattern,
    get_filter_options,
    create_variant_flow_snapshot,
    create_variant_summary,
    create_pattern_flow_snapshot,
    create_pattern_bottleneck_details,
    get_available_analysis_definitions,
    load_prepared_event_log,
    merge_filter_params,
    normalize_filter_params,
    normalize_filter_column_settings,
)


BASE_DIR = Path(__file__).resolve().parent
SAMPLE_FILE = BASE_DIR / "sample_event_log.csv"
MAX_STORED_RUNS = 5
PREVIEW_ROW_COUNT = 10
PROCESS_FLOW_PATTERN_CAP = 300
MAX_PATTERN_FLOW_CACHE = 24
FILTER_PARAM_NAMES = ("date_from", "date_to", "filter_value_1", "filter_value_2", "filter_value_3")
FILTER_COLUMN_NAMES = ("filter_column_1", "filter_column_2", "filter_column_3")
FILTER_LABEL_NAMES = ("filter_label_1", "filter_label_2", "filter_label_3")

DEFAULT_HEADERS = {
    "case_id_column": "case_id",
    "activity_column": "activity",
    "timestamp_column": "start_time",
}

app = FastAPI(title="Process Mining Workbench")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
RUN_STORE = OrderedDict()


def read_csv_headers(file_source):
    if hasattr(file_source, "seek"):
        file_source.seek(0)

    try:
        header_df = pd.read_csv(file_source, nrows=0)
    finally:
        if hasattr(file_source, "seek"):
            file_source.seek(0)

    headers = [str(column_name) for column_name in header_df.columns.tolist()]
    if not headers:
        raise ValueError("CSV headers could not be read. Please use a CSV whose first row contains column names.")

    return headers


def build_column_selection_payload(headers):
    header_set = set(headers)
    return {
        "headers": headers,
        "default_selection": {
            field_name: (
                default_header if default_header in header_set else ""
            )
            for field_name, default_header in DEFAULT_HEADERS.items()
        },
    }


def validate_selected_columns(case_id_column, activity_column, timestamp_column):
    selected_columns = {
        "Case ID": case_id_column,
        "Activity": activity_column,
        "Timestamp": timestamp_column,
    }

    missing_fields = [
        field_label
        for field_label, column_name in selected_columns.items()
        if not str(column_name or "").strip()
    ]
    if missing_fields:
        raise ValueError(f"Please select: {' / '.join(missing_fields)}")

    normalized_columns = [column_name.strip() for column_name in selected_columns.values()]
    if len(set(normalized_columns)) != len(normalized_columns):
        raise ValueError("Case ID / Activity / Timestamp にはそれぞれ異なる列を選択してください。")


def validate_filter_column_settings(filter_column_settings):
    selected_filter_columns = [
        filter_config["column_name"]
        for filter_config in filter_column_settings.values()
        if filter_config["column_name"]
    ]

    if len(selected_filter_columns) != len(set(selected_filter_columns)):
        raise ValueError("グループ/カテゴリー フィルター①〜③ にはそれぞれ異なる列を選択してください。")


def read_raw_log_dataframe(file_source):
    if hasattr(file_source, "seek"):
        file_source.seek(0)

    try:
        raw_df = pd.read_csv(file_source, dtype=str, keep_default_na=False)
    finally:
        if hasattr(file_source, "seek"):
            file_source.seek(0)

    return raw_df


def get_static_version():
    static_dir = BASE_DIR / "static"
    return str(
        max(
            entry.stat().st_mtime_ns
            for entry in static_dir.iterdir()
            if entry.is_file()
        )
    )


def save_run_data(
    source_file_name,
    selected_analysis_keys,
    prepared_df,
    result,
    column_settings,
    base_filter_params,
):
    run_id = uuid4().hex
    RUN_STORE[run_id] = {
        "source_file_name": source_file_name,
        "selected_analysis_keys": selected_analysis_keys,
        "prepared_df": prepared_df,
        "result": result,
        "column_settings": column_settings,
        "base_filter_params": base_filter_params,
        "pattern_index_entries": None,
        "pattern_flow_cache": OrderedDict(),
        "variant_cache": {},
        "bottleneck_cache": {},
        "analysis_cache": {},
        "filter_options": None,
    }
    RUN_STORE.move_to_end(run_id)

    while len(RUN_STORE) > MAX_STORED_RUNS:
        RUN_STORE.popitem(last=False)

    return run_id


def get_run_data(run_id):
    run_data = RUN_STORE.get(run_id)

    if not run_data:
        raise HTTPException(status_code=404, detail="Run data was not found.")

    RUN_STORE.move_to_end(run_id)
    return run_data


def get_request_filter_params(request: Request):
    return normalize_filter_params(
        **{
            filter_name: request.query_params.get(filter_name)
            for filter_name in FILTER_PARAM_NAMES
        }
    )


def get_form_filter_params(form):
    return normalize_filter_params(
        **{
            filter_name: form.get(filter_name)
            for filter_name in FILTER_PARAM_NAMES
        }
    )


def get_form_filter_column_settings(form):
    raw_settings = {
        setting_name: form.get(setting_name)
        for setting_name in (*FILTER_COLUMN_NAMES, *FILTER_LABEL_NAMES)
    }
    return normalize_filter_column_settings(**raw_settings)


def get_effective_filter_params(run_data, filter_params=None):
    return merge_filter_params(run_data.get("base_filter_params"), filter_params)


def build_filter_cache_key(filter_params):
    normalized_filters = normalize_filter_params(**(filter_params or {}))
    return tuple(
        normalized_filters.get(filter_name)
        for filter_name in FILTER_PARAM_NAMES
    )


def build_filtered_meta(prepared_df):
    return {
        "case_count": int(prepared_df["case_id"].nunique()) if not prepared_df.empty else 0,
        "event_count": int(len(prepared_df)),
    }


def build_column_settings_payload(column_settings):
    raw_column_settings = column_settings or {}
    filter_slot_names = ("filter_value_1", "filter_value_2", "filter_value_3")

    if any(isinstance(raw_column_settings.get(filter_slot_name), dict) for filter_slot_name in filter_slot_names):
        default_filter_settings = normalize_filter_column_settings()
        normalized_filter_settings = {
            filter_slot_name: {
                "column_name": str((raw_column_settings.get(filter_slot_name) or {}).get("column_name") or "").strip() or None,
                "label": str((raw_column_settings.get(filter_slot_name) or {}).get("label") or "").strip()
                or default_filter_settings[filter_slot_name]["label"],
            }
            for filter_slot_name in filter_slot_names
        }
    else:
        normalized_filter_settings = normalize_filter_column_settings(**raw_column_settings)

    return {
        "case_id_column": str(raw_column_settings.get("case_id_column") or "").strip(),
        "activity_column": str(raw_column_settings.get("activity_column") or "").strip(),
        "timestamp_column": str(raw_column_settings.get("timestamp_column") or "").strip(),
        "filters": [
            {
                "slot": filter_key,
                **normalized_filter_settings[filter_key],
            }
            for filter_key in normalized_filter_settings
        ],
    }


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


def build_variant_response_item(variant_item):
    return {
        "variant_id": variant_item["variant_id"],
        "activities": variant_item["activities"],
        "count": variant_item["count"],
        "ratio": variant_item["ratio"],
    }


def build_variant_coverage_payload(total_case_count, variant_items):
    covered_case_count = sum(int(variant_item["count"]) for variant_item in variant_items)
    return {
        "displayed_variant_count": len(variant_items),
        "covered_case_count": covered_case_count,
        "total_case_count": int(total_case_count),
        "ratio": round(covered_case_count / total_case_count, 4) if total_case_count else 0.0,
    }


def get_filter_options_payload(run_data):
    filter_options = run_data.get("filter_options")
    if filter_options is None:
        filter_options = get_filter_options(
            run_data["prepared_df"],
            filter_column_settings=run_data.get("column_settings"),
        )
        run_data["filter_options"] = filter_options

    return filter_options


def get_variant_items(run_data, filter_params=None):
    cache_key = build_filter_cache_key(filter_params)
    variant_cache = run_data.setdefault("variant_cache", {})

    if cache_key not in variant_cache:
        filtered_df = filter_prepared_df(
            run_data["prepared_df"],
            filter_params,
            filter_column_settings=run_data.get("column_settings"),
        )
        variant_cache[cache_key] = create_variant_summary(filtered_df, limit=None)

    return variant_cache[cache_key]


def get_variant_item(run_data, variant_id, filter_params=None):
    safe_variant_id = int(variant_id)

    for variant_item in get_variant_items(run_data, filter_params=filter_params):
        if variant_item["variant_id"] == safe_variant_id:
            return variant_item

    raise HTTPException(status_code=404, detail="Variant was not found.")


def get_pattern_summary_row(run_data, pattern_index):
    pattern_analysis = run_data["result"]["analyses"].get("pattern")

    if not pattern_analysis:
        raise HTTPException(status_code=400, detail="Pattern analysis is not available.")

    pattern_rows = pattern_analysis["rows"]
    safe_pattern_index = int(pattern_index)
    if safe_pattern_index < 0 or safe_pattern_index >= len(pattern_rows):
        raise HTTPException(status_code=404, detail="Pattern index was not found.")

    pattern_index_entries = run_data.get("pattern_index_entries")
    if pattern_index_entries is None:
        pattern_index_entries = create_pattern_index_entries(run_data["prepared_df"])
        run_data["pattern_index_entries"] = pattern_index_entries

    if safe_pattern_index >= len(pattern_index_entries):
        raise HTTPException(status_code=404, detail="Pattern index was not found.")

    summary_row = pattern_rows[safe_pattern_index]
    pattern_entry = pattern_index_entries[safe_pattern_index]
    pattern = str(pattern_entry.get("pattern") or "").strip()

    if not pattern:
        raise HTTPException(status_code=500, detail="Pattern text could not be resolved.")

    return pattern_analysis, summary_row, pattern_entry, pattern


def get_filtered_prepared_df(run_data, variant_id=None, pattern_index=None, filter_params=None):
    prepared_df = filter_prepared_df(
        run_data["prepared_df"],
        filter_params,
        filter_column_settings=run_data.get("column_settings"),
    )

    if pattern_index is not None:
        _, _, _, pattern = get_pattern_summary_row(run_data, pattern_index)
        prepared_df = filter_prepared_df_by_pattern(prepared_df, pattern)

    if variant_id is not None:
        variant_item = get_variant_item(run_data, variant_id, filter_params=filter_params)
        prepared_df = filter_prepared_df_by_pattern(prepared_df, variant_item["pattern"])

    return prepared_df


def get_analysis_data(run_data, analysis_key, filter_params=None):
    normalized_filter_key = build_filter_cache_key(filter_params)
    analysis_cache = run_data.setdefault("analysis_cache", {})

    if all(filter_value is None for filter_value in normalized_filter_key):
        analysis = run_data["result"]["analyses"].get(analysis_key)
        if analysis:
            return analysis

        cache_key = ("analysis", analysis_key, normalized_filter_key)
        if cache_key not in analysis_cache:
            analysis_cache[cache_key] = create_analysis_records(run_data["prepared_df"], analysis_key)
        return analysis_cache[cache_key]

    cache_key = ("analysis", analysis_key, normalized_filter_key)

    if cache_key not in analysis_cache:
        filtered_df = filter_prepared_df(
            run_data["prepared_df"],
            filter_params,
            filter_column_settings=run_data.get("column_settings"),
        )
        analysis_cache[cache_key] = create_analysis_records(filtered_df, analysis_key)

    return analysis_cache[cache_key]


def get_bottleneck_summary(run_data, variant_id=None, pattern_index=None, filter_params=None):
    cache_key = (
        "bottleneck",
        None if variant_id is None else int(variant_id),
        None if pattern_index is None else int(pattern_index),
        build_filter_cache_key(filter_params),
    )
    cache = run_data.setdefault("bottleneck_cache", {})

    if cache_key not in cache:
        filtered_df = get_filtered_prepared_df(
            run_data,
            variant_id=variant_id,
            pattern_index=pattern_index,
            filter_params=filter_params,
        )
        cache[cache_key] = create_bottleneck_summary(filtered_df, limit=None)

    return cache[cache_key]


def get_pattern_flow_snapshot(
    run_data,
    pattern_percent,
    pattern_count,
    activity_percent,
    connection_percent,
    variant_id=None,
    filter_params=None,
):
    filter_cache_key = build_filter_cache_key(filter_params)
    if variant_id is None:
        cache_key = (
            "pattern",
            int(pattern_percent),
            None if pattern_count is None else int(pattern_count),
            int(activity_percent),
            int(connection_percent),
            filter_cache_key,
        )
    else:
        cache_key = (
            "variant",
            int(variant_id),
            int(activity_percent),
            int(connection_percent),
            filter_cache_key,
        )
    cache = run_data.setdefault("pattern_flow_cache", OrderedDict())

    cached_snapshot = cache.get(cache_key)
    if cached_snapshot is not None:
        cache.move_to_end(cache_key)
        return cached_snapshot

    analyses = run_data["result"]["analyses"]
    if variant_id is None:
        pattern_analysis = get_analysis_data(run_data, "pattern", filter_params=filter_params)
        frequency_analysis = get_analysis_data(run_data, "frequency", filter_params=filter_params)
        snapshot = create_pattern_flow_snapshot(
            pattern_rows=pattern_analysis["rows"],
            frequency_rows=frequency_analysis.get("rows", []),
            pattern_percent=pattern_percent,
            pattern_count=pattern_count,
            activity_percent=activity_percent,
            connection_percent=connection_percent,
            pattern_cap=PROCESS_FLOW_PATTERN_CAP,
        )
    else:
        filtered_df = filter_prepared_df(
            run_data["prepared_df"],
            filter_params,
            filter_column_settings=run_data.get("column_settings"),
        )
        variant_item = get_variant_item(run_data, variant_id, filter_params=filter_params)
        snapshot = create_variant_flow_snapshot(
            prepared_df=filtered_df,
            variant_pattern=variant_item["pattern"],
            activity_percent=activity_percent,
            connection_percent=connection_percent,
        )
        snapshot["selected_variant"] = build_variant_response_item(variant_item)

    cache[cache_key] = snapshot
    cache.move_to_end(cache_key)
    while len(cache) > MAX_PATTERN_FLOW_CACHE:
        cache.popitem(last=False)

    return snapshot


def build_preview_response(run_id, source_file_name, selected_analysis_keys, result, run_data):
    return {
        "run_id": run_id,
        "source_file_name": source_file_name,
        "selected_analysis_keys": selected_analysis_keys,
        "case_count": result["case_count"],
        "event_count": result["event_count"],
        "applied_filters": run_data.get("base_filter_params"),
        "column_settings": build_column_settings_payload(run_data.get("column_settings")),
        "filter_options": get_filter_options_payload(run_data),
        "analyses": {
            analysis_key: build_analysis_payload(analysis, PREVIEW_ROW_COUNT)
            for analysis_key, analysis in result["analyses"].items()
        },
    }


def build_log_profile_payload(
    raw_df,
    source_file_name,
    case_id_column="",
    activity_column="",
    timestamp_column="",
    filter_column_settings=None,
):
    headers = [str(column_name) for column_name in raw_df.columns.tolist()]
    selection_payload = build_column_selection_payload(headers)
    resolved_case_id_column = case_id_column or selection_payload["default_selection"]["case_id_column"]
    resolved_activity_column = activity_column or selection_payload["default_selection"]["activity_column"]
    resolved_timestamp_column = timestamp_column or selection_payload["default_selection"]["timestamp_column"]
    normalized_filter_column_settings = normalize_filter_column_settings(**(filter_column_settings or {}))

    return {
        "source_file_name": source_file_name,
        **selection_payload,
        "column_settings": build_column_settings_payload(
            {
                "case_id_column": resolved_case_id_column,
                "activity_column": resolved_activity_column,
                "timestamp_column": resolved_timestamp_column,
                **normalized_filter_column_settings,
            }
        ),
        "diagnostics": create_log_diagnostics(
            raw_df,
            case_id_column=resolved_case_id_column,
            activity_column=resolved_activity_column,
            timestamp_column=resolved_timestamp_column,
            filter_column_settings=normalized_filter_column_settings,
        ),
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
    sample_profile_payload = build_log_profile_payload(
        raw_df=read_raw_log_dataframe(SAMPLE_FILE),
        source_file_name=SAMPLE_FILE.name,
    )

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "analysis_options": get_analysis_options(),
            "default_headers": DEFAULT_HEADERS,
            "sample_profile_payload": sample_profile_payload,
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
        raise HTTPException(status_code=404, detail="Analysis key was not found.")

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
    pattern_analysis, summary_row, _, pattern = get_pattern_summary_row(run_data, pattern_index)

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
    request: Request,
    run_id: str,
    analysis_key: str,
    row_limit: int | None = None,
    row_offset: int = 0,
):
    run_data = get_run_data(run_id)
    filter_params = get_effective_filter_params(run_data, get_request_filter_params(request))
    analysis = get_analysis_data(run_data, analysis_key, filter_params=filter_params)
    filtered_meta = build_filtered_meta(
        filter_prepared_df(
            run_data["prepared_df"],
            filter_params,
            filter_column_settings=run_data.get("column_settings"),
        )
    )

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
            "case_count": filtered_meta["case_count"],
            "event_count": filtered_meta["event_count"],
            "applied_filters": filter_params,
            "column_settings": build_column_settings_payload(run_data.get("column_settings")),
            "analyses": response_analyses,
        }
    )


@app.get("/api/runs/{run_id}/excel-files/{analysis_key}")
def analysis_excel_file_api(run_id: str, analysis_key: str):
    run_data = get_run_data(run_id)
    analyses = run_data["result"]["analyses"]
    analysis = analyses.get(analysis_key)

    if not analysis:
        raise HTTPException(status_code=404, detail="Analysis data was not found.")

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


@app.get("/api/runs/{run_id}/filter-options")
def filter_options_api(run_id: str):
    run_data = get_run_data(run_id)

    return JSONResponse(
        content={
            "run_id": run_id,
            "options": get_filter_options_payload(run_data),
            "applied_filters": run_data.get("base_filter_params"),
            "column_settings": build_column_settings_payload(run_data.get("column_settings")),
        }
    )


@app.get("/api/runs/{run_id}/pattern-flow")
def pattern_flow_api(
    request: Request,
    run_id: str,
    pattern_percent: int = 10,
    pattern_count: int | None = None,
    activity_percent: int = 40,
    connection_percent: int = 30,
    variant_id: int | None = None,
):
    run_data = get_run_data(run_id)
    filter_params = get_effective_filter_params(run_data, get_request_filter_params(request))
    pattern_analysis = get_analysis_data(run_data, "pattern", filter_params=filter_params)

    if not pattern_analysis and variant_id is None:
        raise HTTPException(status_code=400, detail="Pattern analysis is not available.")

    snapshot = get_pattern_flow_snapshot(
        run_data=run_data,
        pattern_percent=pattern_percent,
        pattern_count=pattern_count,
        activity_percent=activity_percent,
        connection_percent=connection_percent,
        variant_id=variant_id,
        filter_params=filter_params,
    )
    filtered_meta = build_filtered_meta(
        filter_prepared_df(
            run_data["prepared_df"],
            filter_params,
            filter_column_settings=run_data.get("column_settings"),
        )
    )

    return JSONResponse(
        content={
            "run_id": run_id,
            "filtered_case_count": filtered_meta["case_count"],
            "filtered_event_count": filtered_meta["event_count"],
            "applied_filters": filter_params,
            **snapshot,
        }
    )


@app.get("/api/runs/{run_id}/variants")
def variant_list_api(request: Request, run_id: str, limit: int = 10):
    run_data = get_run_data(run_id)
    filter_params = get_effective_filter_params(run_data, get_request_filter_params(request))
    safe_limit = max(0, int(limit))
    filtered_df = filter_prepared_df(
        run_data["prepared_df"],
        filter_params,
        filter_column_settings=run_data.get("column_settings"),
    )
    variant_items = get_variant_items(run_data, filter_params=filter_params)[:safe_limit]

    return JSONResponse(
        content={
            "run_id": run_id,
            "variants": [
                build_variant_response_item(variant_item)
                for variant_item in variant_items
            ],
            "coverage": build_variant_coverage_payload(
                total_case_count=int(filtered_df["case_id"].nunique()) if not filtered_df.empty else 0,
                variant_items=variant_items,
            ),
            "filtered_case_count": int(filtered_df["case_id"].nunique()) if not filtered_df.empty else 0,
            "filtered_event_count": int(len(filtered_df)),
            "applied_filters": filter_params,
        }
    )


@app.get("/api/runs/{run_id}/bottlenecks")
def bottleneck_list_api(
    request: Request,
    run_id: str,
    limit: int = 5,
    variant_id: int | None = None,
    pattern_index: int | None = None,
):
    run_data = get_run_data(run_id)
    filter_params = get_effective_filter_params(run_data, get_request_filter_params(request))
    safe_limit = max(0, int(limit))
    bottleneck_summary = get_bottleneck_summary(
        run_data,
        variant_id=variant_id,
        pattern_index=pattern_index,
        filter_params=filter_params,
    )
    filtered_df = get_filtered_prepared_df(
        run_data,
        variant_id=variant_id,
        pattern_index=pattern_index,
        filter_params=filter_params,
    )

    return JSONResponse(
        content={
            "run_id": run_id,
            "limit": safe_limit,
            "variant_id": variant_id,
            "pattern_index": pattern_index,
            "filtered_case_count": int(filtered_df["case_id"].nunique()) if not filtered_df.empty else 0,
            "filtered_event_count": int(len(filtered_df)),
            "applied_filters": filter_params,
            "activity_bottlenecks": bottleneck_summary["activity_bottlenecks"][:safe_limit],
            "transition_bottlenecks": bottleneck_summary["transition_bottlenecks"][:safe_limit],
            "activity_heatmap": bottleneck_summary["activity_heatmap"],
            "transition_heatmap": bottleneck_summary["transition_heatmap"],
        }
    )


@app.get("/api/runs/{run_id}/transition-cases")
def transition_case_drilldown_api(
    request: Request,
    run_id: str,
    from_activity: str,
    to_activity: str,
    limit: int = 20,
    variant_id: int | None = None,
    pattern_index: int | None = None,
):
    run_data = get_run_data(run_id)
    filter_params = get_effective_filter_params(run_data, get_request_filter_params(request))
    filtered_df = get_filtered_prepared_df(
        run_data,
        variant_id=variant_id,
        pattern_index=pattern_index,
        filter_params=filter_params,
    )
    safe_limit = max(0, int(limit))
    case_rows = create_transition_case_drilldown(
        filtered_df,
        from_activity=from_activity,
        to_activity=to_activity,
        limit=safe_limit,
    )

    return JSONResponse(
        content={
            "run_id": run_id,
            "variant_id": variant_id,
            "pattern_index": pattern_index,
            "from_activity": from_activity,
            "to_activity": to_activity,
            "transition_key": f"{from_activity}__TO__{to_activity}",
            "transition_label": f"{from_activity} → {to_activity}",
            "limit": safe_limit,
            "returned_case_count": len(case_rows),
            "applied_filters": filter_params,
            "cases": case_rows,
        }
    )


@app.get("/api/runs/{run_id}/cases/{case_id:path}")
def case_trace_api(run_id: str, case_id: str):
    run_data = get_run_data(run_id)
    normalized_case_id = str(case_id or "").strip()

    if not normalized_case_id:
        return JSONResponse(
            status_code=400,
            content={
                "run_id": run_id,
                "case_id": "",
                "found": False,
                "summary": None,
                "events": [],
                "error": "Case ID is required.",
            },
        )

    case_trace = create_case_trace_details(run_data["prepared_df"], normalized_case_id)
    return JSONResponse(
        content={
            "run_id": run_id,
            **case_trace,
        }
    )


@app.post("/api/csv-headers")
async def csv_headers(request: Request):
    form = await request.form()
    uploaded_file = form.get("csv_file")
    raw_case_id_column = form.get("case_id_column")
    raw_activity_column = form.get("activity_column")
    raw_timestamp_column = form.get("timestamp_column")
    filter_column_settings = get_form_filter_column_settings(form)

    if uploaded_file and uploaded_file.filename:
        uploaded_file.file.seek(0)
        file_source = uploaded_file.file
        source_file_name = uploaded_file.filename
    else:
        file_source = SAMPLE_FILE
        source_file_name = SAMPLE_FILE.name

    try:
        raw_df = read_raw_log_dataframe(file_source)
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except Exception as exc:
        return JSONResponse(
            status_code=400,
            content={
                "error": "CSV headers could not be read. Please check the file encoding and header row.",
                "detail": str(exc),
            },
        )

    return JSONResponse(
        content=build_log_profile_payload(
            raw_df=raw_df,
            source_file_name=source_file_name,
            case_id_column=str(raw_case_id_column or "").strip(),
            activity_column=str(raw_activity_column or "").strip(),
            timestamp_column=str(raw_timestamp_column or "").strip(),
            filter_column_settings=filter_column_settings,
        )
    )


@app.post("/api/analyze")
async def analyze(request: Request):
    form = await request.form()
    uploaded_file = form.get("csv_file")
    raw_case_id_column = form.get("case_id_column")
    raw_activity_column = form.get("activity_column")
    raw_timestamp_column = form.get("timestamp_column")
    case_id_column = (
        DEFAULT_HEADERS["case_id_column"]
        if raw_case_id_column is None
        else str(raw_case_id_column).strip()
    )
    activity_column = (
        DEFAULT_HEADERS["activity_column"]
        if raw_activity_column is None
        else str(raw_activity_column).strip()
    )
    timestamp_column = (
        DEFAULT_HEADERS["timestamp_column"]
        if raw_timestamp_column is None
        else str(raw_timestamp_column).strip()
    )
    selected_analysis_keys = form.getlist("analysis_keys")
    filter_column_settings = get_form_filter_column_settings(form)
    base_filter_params = get_form_filter_params(form)

    if uploaded_file and uploaded_file.filename:
        uploaded_file.file.seek(0)
        file_source = uploaded_file.file
        source_file_name = uploaded_file.filename
    else:
        file_source = SAMPLE_FILE
        source_file_name = SAMPLE_FILE.name

    try:
        validate_selected_columns(
            case_id_column=case_id_column,
            activity_column=activity_column,
            timestamp_column=timestamp_column,
        )
        validate_filter_column_settings(filter_column_settings)
        prepared_df = load_prepared_event_log(
            file_source=file_source,
            case_id_column=case_id_column,
            activity_column=activity_column,
            timestamp_column=timestamp_column,
        )
        filtered_prepared_df = filter_prepared_df(
            prepared_df,
            base_filter_params,
            filter_column_settings=filter_column_settings,
        )
        result = analyze_prepared_event_log(
            prepared_df=filtered_prepared_df,
            selected_analysis_keys=selected_analysis_keys,
            output_root_dir=None,
            export_excel=False,
        )
        run_id = save_run_data(
            source_file_name=source_file_name,
            selected_analysis_keys=selected_analysis_keys,
            prepared_df=prepared_df,
            result=result,
            column_settings={
                "case_id_column": case_id_column,
                "activity_column": activity_column,
                "timestamp_column": timestamp_column,
                **filter_column_settings,
            },
            base_filter_params=base_filter_params,
        )
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"error": str(exc)})
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "error": "Analysis failed unexpectedly.",
                "detail": str(exc),
            },
        )

    return JSONResponse(
        content=build_preview_response(
            run_id=run_id,
            source_file_name=source_file_name,
            selected_analysis_keys=selected_analysis_keys,
            result=result,
            run_data=get_run_data(run_id),
        )
    )


if __name__ == "__main__":
    uvicorn.run("web_app:app", host="127.0.0.1", port=5000, reload=True)

