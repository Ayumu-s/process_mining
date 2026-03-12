from collections import OrderedDict
from pathlib import Path
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from 共通スクリプト.analysis_service import (
    DEFAULT_ANALYSIS_KEYS,
    analyze_prepared_event_log,
    create_pattern_bottleneck_details,
    get_available_analysis_definitions,
    load_prepared_event_log,
)


BASE_DIR = Path(__file__).resolve().parent
SAMPLE_FILE = BASE_DIR / "sample_event_log.csv"
OUTPUT_ROOT_DIR = BASE_DIR / "出力ファイル"
MAX_STORED_RUNS = 5

DEFAULT_HEADERS = {
    "case_id_column": "case_id",
    "activity_column": "activity",
    "timestamp_column": "start_time",
}

app = FastAPI(title="Process Mining Workbench")
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
RUN_STORE = OrderedDict()


def save_run_data(source_file_name, selected_analysis_keys, prepared_df, result):
    run_id = uuid4().hex
    RUN_STORE[run_id] = {
        "source_file_name": source_file_name,
        "selected_analysis_keys": selected_analysis_keys,
        "prepared_df": prepared_df,
        "result": result,
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
        },
    )


@app.get("/analysis/patterns/{pattern_index}")
def pattern_detail_page(request: Request, pattern_index: int):
    return templates.TemplateResponse(
        "pattern_detail.html",
        {
            "request": request,
            "pattern_index": pattern_index,
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


@app.post("/api/analyze")
async def analyze(request: Request):
    form = await request.form()
    uploaded_file = form.get("csv_file")
    case_id_column = (form.get("case_id_column") or DEFAULT_HEADERS["case_id_column"]).strip()
    activity_column = (form.get("activity_column") or DEFAULT_HEADERS["activity_column"]).strip()
    timestamp_column = (form.get("timestamp_column") or DEFAULT_HEADERS["timestamp_column"]).strip()
    selected_analysis_keys = form.getlist("analysis_keys")
    export_excel = form.get("export_excel") == "on"

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
            output_root_dir=OUTPUT_ROOT_DIR,
            export_excel=export_excel,
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
        content={
            "run_id": run_id,
            "source_file_name": source_file_name,
            "selected_analysis_keys": selected_analysis_keys,
            **result,
        }
    )


if __name__ == "__main__":
    uvicorn.run("web_app:app", host="127.0.0.1", port=5000, reload=True)
