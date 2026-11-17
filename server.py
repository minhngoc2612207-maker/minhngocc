"""
Flask Web Server for PartnerIQ & DataIQ Multi-Agent Platform.
Localhost URL: http://localhost:5000.
Orchestrates:
1. PartnerIQ: Channel partner intelligence, compliance checking, scoring & dossier generation.
2. DataIQ: Enterprise data processing agent (B1 Intake -> B2 Reading/Profiling/Planning/Processing/Analysis ->
   B3 Gatekeeper -> B4 Error Diagnostics -> B5 Clean Data/Reports/Feedback & Scoped Reprocessing ->
   B6 User Evaluation -> B7 Multi-format Export & Executive Briefing).
"""

import os
import sys
import json
import uuid
import tempfile
import time
from datetime import datetime
from typing import Dict, Any, List

# Ensure project root is in sys.path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import zipfile

# Auto-unpack bundle.zip if deployed where subfolders weren't uploaded directly
BUNDLE_PATH = os.path.join(BASE_DIR, "bundle.zip")
UNPACKED_DIR = BASE_DIR
if os.path.exists(BUNDLE_PATH):
    UNPACKED_DIR = os.path.join(tempfile.gettempdir(), "partneriq_unpacked")
    try:
        if not os.path.exists(os.path.join(UNPACKED_DIR, "templates", "index.html")):
            with zipfile.ZipFile(BUNDLE_PATH, "r") as zf:
                zf.extractall(UNPACKED_DIR)
    except Exception as _e:
        print(f"Notice unpacking bundle.zip: {_e}")

    if UNPACKED_DIR not in sys.path:
        sys.path.insert(0, UNPACKED_DIR)

from flask import Flask, render_template, request, jsonify, send_file, abort, Response, stream_with_context
from werkzeug.utils import secure_filename
import pandas as pd

# PartnerIQ Legacy modules
from agent.llm_client import load_config, save_config, test_connection
from agent.extractor import extract_text_from_file, parse_unstructured_text, get_empty_schema
from agent.pipeline_manager import (
    get_all_dossiers,
    get_dossier_by_id,
    create_dossier,
    update_verification_and_rescore,
    submit_approval_decision,
    delete_dossier
)
from agent.dossier_generator import export_pipeline_to_excel

# DataIQ & Repository modules
import partneriq.repository as repo
from partneriq.core.data_agent.intake import (
    validate_intake_payload, sanitize_filename, ensure_sample_files, get_sample_manifest
)
from partneriq.core.data_agent.reader import read_all_job_files, read_data_file
from partneriq.core.data_agent.profiler import profile_all_datasets
from partneriq.core.data_agent.gatekeeper import evaluate_gatekeeper_decision
from partneriq.core.data_agent.error_reporter import export_error_report_to_excel
from partneriq.core.data_agent.feedback_engine import execute_scoped_reprocessing
from partneriq.core.data_agent.evaluator import submit_user_evaluation, compute_workspace_quality_stats
from partneriq.core.data_agent.exporter import (
    export_clean_data_to_csv, export_to_multisheet_excel,
    export_executive_report_to_pdf, generate_executive_email_draft
)
from partneriq.core.data_agent.job_manager import (
    start_job_pipeline_async, cancel_job, DATASET_CACHE
)

def _resolve_data_dir() -> str:
    is_cloud_serverless = bool(
        os.environ.get("VERCEL") or
        os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or
        os.environ.get("VERCEL_ENV")
    )
    if is_cloud_serverless:
        target = os.path.join(tempfile.gettempdir(), "partneriq_data")
        os.makedirs(target, exist_ok=True)
        return target
    default_dir = os.path.join(BASE_DIR, "data")
    try:
        os.makedirs(default_dir, exist_ok=True)
        test_file = os.path.join(default_dir, ".perm_test")
        with open(test_file, "w") as f:
            f.write("1")
        os.remove(test_file)
        return default_dir
    except Exception:
        target = os.path.join(tempfile.gettempdir(), "partneriq_data")
        os.makedirs(target, exist_ok=True)
        return target

DATA_DIR = _resolve_data_dir()
SAMPLE_DIR = os.path.join(DATA_DIR, "samples")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")

try:
    os.makedirs(SAMPLE_DIR, exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    ensure_sample_files(SAMPLE_DIR)
except Exception as _e:
    print(f"Directory / Sample initialization notice: {_e}")

template_candidates = [
    os.path.join(BASE_DIR, "templates"),
    os.path.join(UNPACKED_DIR, "templates"),
    os.path.join(tempfile.gettempdir(), "partneriq_unpacked", "templates")
]
active_templates = next((p for p in template_candidates if os.path.exists(p)), os.path.join(BASE_DIR, "templates"))

app = Flask(
    __name__,
    template_folder=active_templates,
    static_folder=os.path.join(BASE_DIR, "static")
)
app.config["MAX_CONTENT_LENGTH"] = 35 * 1024 * 1024  # 35 MB upload limit
app.config["SECRET_KEY"] = "partneriq-dataiq-enterprise-key-2026"


# ========================================================
# CORE UI ROUTE
# ========================================================

@app.route("/")
def index():
    """Renders main unified SaaS portal."""
    try:
        return render_template("index.html")
    except Exception as e:
        candidates = [
            os.path.join(active_templates, "index.html"),
            os.path.join(BASE_DIR, "templates", "index.html"),
            os.path.join(UNPACKED_DIR, "templates", "index.html"),
            os.path.join(tempfile.gettempdir(), "partneriq_unpacked", "templates", "index.html"),
            os.path.join(os.getcwd(), "templates", "index.html")
        ]
        for p in candidates:
            if os.path.exists(p):
                with open(p, "r", encoding="utf-8") as f:
                    return f.read(), 200, {"Content-Type": "text/html; charset=utf-8"}
        return f"<h1>Error loading template: {e}</h1>", 500


# ========================================================
# PARTNERIQ LEGACY ROUTES (REUSED 100%)
# ========================================================

@app.route("/api/dossiers", methods=["GET"])
def api_get_dossiers():
    dossiers = get_all_dossiers()
    total = len(dossiers)
    tier_a = sum(1 for d in dossiers if d.get("score_result", {}).get("tier") == "Tier A")
    tier_b = sum(1 for d in dossiers if d.get("score_result", {}).get("tier") == "Tier B")
    tier_c = sum(1 for d in dossiers if d.get("score_result", {}).get("tier") == "Tier C")
    tier_d = sum(1 for d in dossiers if d.get("score_result", {}).get("tier") == "Tier D")
    phantom_count = sum(1 for d in dossiers if d.get("score_result", {}).get("is_phantom_commitment", False))
    violations_count = sum(1 for d in dossiers if len(d.get("compliance_result", {}).get("violations", [])) > 0)
    approved_count = sum(1 for d in dossiers if d.get("approval_result", {}).get("decision") == "TIẾP TỤC")

    return jsonify({
        "success": True,
        "kpis": {
            "total_dossiers": total,
            "tier_a": tier_a,
            "tier_b": tier_b,
            "tier_c": tier_c,
            "tier_d": tier_d,
            "phantom_commitments": phantom_count,
            "compliance_violations": violations_count,
            "approved_count": approved_count
        },
        "dossiers": dossiers
    })


@app.route("/api/dossiers/<dossier_id>", methods=["GET"])
def api_get_dossier(dossier_id):
    dossier = get_dossier_by_id(dossier_id)
    if not dossier:
        return jsonify({"success": False, "error": "Không tìm thấy hồ sơ"}), 404
    return jsonify({"success": True, "dossier": dossier})


@app.route("/api/intake/upload", methods=["POST"])
def api_intake_upload():
    if "file" not in request.files:
        return jsonify({"success": False, "error": "Chưa chọn tệp tải lên"}), 400
    file = request.files["file"]
    if not file or not file.filename:
        return jsonify({"success": False, "error": "Tên tệp không hợp lệ"}), 400

    filename = secure_filename(file.filename)
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, filename)
    file.save(file_path)

    try:
        raw_text = extract_text_from_file(file_path)
        extracted_data, source = parse_unstructured_text(raw_text)
        return jsonify({
            "success": True,
            "raw_text_preview": raw_text[:800],
            "extracted_data": extracted_data,
            "source": source
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass


@app.route("/api/intake/form", methods=["POST"])
def api_intake_form():
    form_data = request.json
    if not form_data:
        return jsonify({"success": False, "error": "Dữ liệu trống"}), 400
    try:
        dossier = create_dossier(form_data)
        return jsonify({"success": True, "dossier": dossier})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/api/dossiers/<dossier_id>/verification", methods=["POST"])
def api_update_verification(dossier_id):
    payload = request.json or {}
    department = payload.get("department")
    answers = payload.get("answers", {})
    if not department:
        return jsonify({"success": False, "error": "Thiếu thông tin phòng ban"}), 400

    updated = update_verification_and_rescore(dossier_id, department, answers)
    if not updated:
        return jsonify({"success": False, "error": "Lỗi cập nhật thẩm tra"}), 400
    return jsonify({"success": True, "dossier": updated})


@app.route("/api/dossiers/<dossier_id>/approval", methods=["POST"])
def api_submit_approval(dossier_id):
    payload = request.json or {}
    decision = payload.get("decision")
    notes = payload.get("notes", "")
    conflict_res = payload.get("conflict_resolution", "")
    if not decision:
        return jsonify({"success": False, "error": "Chưa chọn quyết định phê duyệt"}), 400

    updated = submit_approval_decision(dossier_id, decision, notes, conflict_res)
    if not updated:
        return jsonify({"success": False, "error": "Lỗi phê duyệt"}), 400
    return jsonify({"success": True, "dossier": updated})


@app.route("/api/dossiers/<dossier_id>/export", methods=["GET"])
def api_export_dossier(dossier_id):
    dossier = get_dossier_by_id(dossier_id)
    if not dossier:
        return jsonify({"success": False, "error": "Không tìm thấy hồ sơ"}), 404
    excel_bytes = export_pipeline_to_excel([dossier])
    fname = f"Ho_So_Tham_Dinh_{dossier.get('legal_entity', {}).get('tax_id', dossier_id)}.xlsx"
    return send_file(
        excel_bytes,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        as_attachment=True,
        download_name=fname
    )


# ========================================================
# DATAIQ REST APIS (NEW AGENT)
# ========================================================

@app.route("/api/data-jobs", methods=["POST"])
def api_create_data_job():
    """
    B1: Receives business context, problem statement, goals, column descriptions,
    masking settings, and files. Initializes job and starts background pipeline.
    """
    try:
        problem_statement = request.form.get("problem_statement", "")
        title = request.form.get("title", "").strip() or f"Phân tích dữ liệu {datetime.now().strftime('%d/%m/%Y %H:%M')}"
        workspace_id = request.form.get("workspace_id", "ws-default")
        user_name = request.form.get("user_name", "staff")

        # Parse JSON fields
        ctx_raw = request.form.get("business_context", "{}")
        goals_raw = request.form.get("goals", "[]")
        cols_raw = request.form.get("column_descriptions", "{}")
        mask_sensitive = request.form.get("mask_sensitive", "true").lower() in ["true", "1", "yes"]

        try:
            business_context = json.loads(ctx_raw) if isinstance(ctx_raw, str) else ctx_raw
        except Exception:
            business_context = {}

        try:
            goals = json.loads(goals_raw) if isinstance(goals_raw, str) else [goals_raw]
        except Exception:
            goals = ["clean_data", "detect_anomalies", "actionable_recommendations"]

        try:
            column_descriptions = json.loads(cols_raw) if isinstance(cols_raw, str) else {}
        except Exception:
            column_descriptions = {}

        uploaded_files = request.files.getlist("files")
        if not uploaded_files or (len(uploaded_files) == 1 and not uploaded_files[0].filename):
            return jsonify({"success": False, "error": "Vui lòng chọn ít nhất 1 tệp dữ liệu thô."}), 400

        # Validate inputs
        is_valid, err_msg, norm_payload = validate_intake_payload(
            problem_statement=problem_statement,
            files=uploaded_files,
            business_context=business_context,
            column_descriptions=column_descriptions
        )
        if not is_valid:
            return jsonify({"success": False, "error": err_msg}), 400

        job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        job_upload_dir = os.path.join(UPLOAD_DIR, job_id)
        os.makedirs(job_upload_dir, exist_ok=True)

        # Create Job in SQLite
        repo.create_job(
            job_id=job_id,
            workspace_id=workspace_id,
            title=title,
            problem_statement=norm_payload["problem_statement"],
            business_context=norm_payload["business_context"],
            goals=goals,
            column_descriptions=column_descriptions,
            mask_sensitive=mask_sensitive,
            created_by=user_name
        )

        # Save files and register
        for f in uploaded_files:
            if not f or not f.filename:
                continue
            orig_name = f.filename
            clean_name = sanitize_filename(orig_name)
            saved_path = os.path.join(job_upload_dir, clean_name)
            f.save(saved_path)

            file_size = os.path.getsize(saved_path)
            ext = os.path.splitext(clean_name)[1].lower().lstrip(".")

            repo.add_data_file(
                file_id=f"f-{uuid.uuid4().hex[:8]}",
                job_id=job_id,
                workspace_id=workspace_id,
                filename=clean_name,
                original_filename=orig_name,
                file_path=saved_path,
                file_type=ext,
                file_size=file_size
            )

        # Start asynchronous pipeline
        start_job_pipeline_async(job_id=job_id, user_name=user_name)

        return jsonify({
            "success": True,
            "job_id": job_id,
            "message": "Đã khởi tạo tác vụ và bắt đầu phân tích dữ liệu!",
            "status": "PROCESSING"
        })

    except Exception as e:
        return jsonify({"success": False, "error": f"Lỗi khởi tạo tác vụ: {str(e)}"}), 500


@app.route("/api/data-jobs", methods=["GET"])
def api_list_data_jobs():
    """Lists all data processing jobs for a workspace."""
    workspace_id = request.args.get("workspace_id", "ws-default")
    status = request.args.get("status")
    jobs = repo.list_jobs(workspace_id=workspace_id, status=status)
    return jsonify({"success": True, "jobs": jobs, "total": len(jobs)})


@app.route("/api/data-jobs/<job_id>", methods=["GET"])
def api_get_data_job_detail(job_id):
    """Retrieves full job details, latest run, findings, change logs, error reports, and diffs."""
    job = repo.get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Không tìm thấy tác vụ."}), 404

    current_ver = job.get("current_run_version", 1)
    target_ver = request.args.get("version", type=int) or current_ver

    # Fetch specific or latest run
    run = repo.get_run(f"{job_id}-v{target_ver}")
    if not run:
        run = repo.get_latest_run(job_id)

    run_id = run.get("id") if run else None

    findings = repo.get_findings(run_id) if run_id else []
    change_logs = repo.get_change_logs(run_id, limit=300) if run_id else []
    feedbacks = repo.get_feedbacks(job_id)
    error_reports = repo.get_error_reports(job_id)
    evaluation = repo.get_evaluation(job_id)
    files = repo.get_job_files(job_id)
    all_runs = repo.get_runs_by_job(job_id)

    return jsonify({
        "success": True,
        "job": job,
        "current_run": run,
        "findings": findings,
        "change_logs": change_logs,
        "feedbacks": feedbacks,
        "error_reports": error_reports,
        "evaluation": evaluation,
        "files": files,
        "versions": [{"version": r["version"], "trigger": r["trigger_type"], "created_at": r["created_at"]} for r in all_runs]
    })


@app.route("/api/data-jobs/<job_id>/progress", methods=["GET"])
def api_get_job_progress(job_id):
    """Real-time progress endpoint via JSON polling or SSE stream."""
    sse_mode = request.args.get("sse", "0") == "1" or "text/event-stream" in request.headers.get("Accept", "")

    job = repo.get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Tác vụ không tồn tại"}), 404

    if not sse_mode:
        return jsonify({
            "success": True,
            "status": job.get("status"),
            "progress_percent": job.get("progress_percent", 0),
            "current_stage": job.get("current_stage", ""),
            "error_summary": job.get("error_summary", {})
        })

    # SSE Generator
    def event_stream():
        last_pct = -1
        last_stage = ""
        while True:
            cur_job = repo.get_job(job_id)
            if not cur_job:
                break
            pct = cur_job.get("progress_percent", 0)
            stage = cur_job.get("current_stage", "")
            st = cur_job.get("status", "")

            if pct != last_pct or stage != last_stage:
                payload = json.dumps({
                    "job_id": job_id,
                    "status": st,
                    "progress_percent": pct,
                    "current_stage": stage,
                    "error_summary": cur_job.get("error_summary", {})
                }, ensure_ascii=False)
                yield f"data: {payload}\n\n"
                last_pct = pct
                last_stage = stage

            if st not in ["PROCESSING", "REPROCESSING"] and pct >= 100:
                break

            time.sleep(0.5)

    return Response(stream_with_context(event_stream()), mimetype="text/event-stream")


@app.route("/api/data-jobs/<job_id>/cancel", methods=["POST"])
def api_cancel_data_job(job_id):
    """Cancels an in-progress data job."""
    user_name = request.json.get("user_name", "staff") if request.is_json else "staff"
    ok = cancel_job(job_id=job_id, user_name=user_name)
    return jsonify({"success": ok, "message": "Đã gửi lệnh hủy tiến trình xử lý."})


@app.route("/api/data-jobs/<job_id>/feedback", methods=["POST"])
def api_submit_feedback(job_id):
    """B5: Submits Human-in-the-Loop Feedback at Report, Finding, or Cell level."""
    payload = request.json or {}
    run_id = payload.get("run_id") or f"{job_id}-v1"
    level = payload.get("level", "REPORT")
    target_ref = payload.get("target_ref", "")
    error_cat = payload.get("error_category", "Khác")
    user_comment = payload.get("user_comment", "").strip()
    expected_val = payload.get("expected_value", "")
    priority = payload.get("priority", "MEDIUM")
    workspace_id = payload.get("workspace_id", "ws-default")
    user_id = payload.get("user_id", "staff")

    if not user_comment:
        return jsonify({"success": False, "error": "Vui lòng nhập nội dung mô tả phản hồi."}), 400

    fb_id = repo.add_feedback({
        "job_id": job_id,
        "run_id": run_id,
        "workspace_id": workspace_id,
        "user_id": user_id,
        "level": level,
        "target_ref": target_ref,
        "error_category": error_cat,
        "user_comment": user_comment,
        "expected_value": expected_val,
        "priority": priority,
        "status": "PENDING"
    })

    repo.log_audit(
        workspace_id=workspace_id,
        action="SUBMIT_FEEDBACK",
        target_type="FEEDBACK",
        target_id=fb_id,
        user_name=user_id,
        details=f"Người dùng gửi phản hồi [{level}] nhóm '{error_cat}': {user_comment[:60]}"
    )

    return jsonify({
        "success": True,
        "feedback_id": fb_id,
        "message": "Đã lưu phản hồi vào danh mục chờ xử lý lại."
    })


@app.route("/api/data-jobs/<job_id>/reprocess", methods=["POST"])
def api_reprocess_job(job_id):
    """
    B5: Triggers scoped reprocessing based on pending or submitted feedbacks.
    Executes fact-checking against ground truth, diff calculation, and version bumping.
    """
    try:
        payload = request.json or {}
        user_name = payload.get("user_name", "staff")
        feedbacks_input = payload.get("feedbacks")

        if not feedbacks_input:
            # Load pending feedbacks from DB
            all_fb = repo.get_feedbacks(job_id)
            feedbacks_input = [f for f in all_fb if f.get("status") in ["PENDING", "REJECTED_CONFLICT", "OVERRIDDEN"]]

        if not feedbacks_input:
            return jsonify({"success": False, "error": "Chưa có phản hồi nào để Agent xử lý lại."}), 400

        job = repo.get_job(job_id)
        if not job:
            return jsonify({"success": False, "error": "Tác vụ không tồn tại."}), 404

        res = execute_scoped_reprocessing(
            job_id=job_id,
            feedbacks_list=feedbacks_input,
            workspace_id=job.get("workspace_id", "ws-default"),
            user_name=user_name
        )

        return jsonify(res)

    except Exception as e:
        return jsonify({"success": False, "error": f"Lỗi tái xử lý: {str(e)}"}), 500


@app.route("/api/data-jobs/<job_id>/override", methods=["POST"])
def api_force_override_feedback(job_id):
    """
    B5: Handles 'Áp dụng theo ý tôi' (Force Override) when feedback had conflict with ground truth.
    Records manual override in audit trail and triggers reprocessing.
    """
    payload = request.json or {}
    fb_id = payload.get("feedback_id")
    override_reason = payload.get("reason", "Người dùng xác nhận ghi đè thủ công theo nghiệp vụ thực tế.")
    user_name = payload.get("user_name", "staff")

    if not fb_id:
        return jsonify({"success": False, "error": "Thiếu feedback_id"}), 400

    job = repo.get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Tác vụ không tồn tại"}), 404

    # Update feedback status to OVERRIDDEN
    repo.update_feedback_status(
        feedback_id=fb_id,
        status="OVERRIDDEN",
        resolution_notes=f"Người dùng xác nhận ghi đè: {override_reason}"
    )

    repo.log_audit(
        workspace_id=job.get("workspace_id", "ws-default"),
        action="FORCE_OVERRIDE",
        target_type="FEEDBACK",
        target_id=fb_id,
        user_name=user_name,
        details=f"Cưỡng chế áp dụng phản hồi mâu thuẫn: {override_reason}"
    )

    # Re-run with trigger_type OVERRIDE
    all_fb = repo.get_feedbacks(job_id)
    target_fb = [f for f in all_fb if f.get("id") == fb_id]

    res = execute_scoped_reprocessing(
        job_id=job_id,
        feedbacks_list=target_fb,
        workspace_id=job.get("workspace_id", "ws-default"),
        trigger_type="OVERRIDE",
        user_name=user_name
    )
    return jsonify(res)


@app.route("/api/data-jobs/<job_id>/evaluate", methods=["POST"])
def api_evaluate_data_job(job_id):
    """B6: Submits end-user evaluation, transitions job to REVIEWED."""
    payload = request.json or {}
    run_id = payload.get("run_id") or f"{job_id}-v1"
    rating = payload.get("rating_stars", 5)
    findings_decisions = payload.get("findings_decisions", {})
    confidence = payload.get("confidence_score", 90)
    notes = payload.get("general_notes", "")
    user_name = payload.get("user_name", "staff")

    job = repo.get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Tác vụ không tồn tại"}), 404

    res = submit_user_evaluation(
        job_id=job_id,
        run_id=run_id,
        rating_stars=rating,
        findings_decisions=findings_decisions,
        confidence_score=confidence,
        general_notes=notes,
        workspace_id=job.get("workspace_id", "ws-default"),
        user_name=user_name
    )
    return jsonify(res)


@app.route("/api/data-jobs/<job_id>/export", methods=["POST"])
def api_export_job_package(job_id):
    """B7: Marks job as EXPORTED and drafts executive email summary."""
    job = repo.get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Tác vụ không tồn tại"}), 404

    current_ver = job.get("current_run_version", 1)
    run_id = f"{job_id}-v{current_ver}"
    user_name = request.json.get("user_name", "staff") if request.is_json else "staff"

    repo.update_job_status(job_id, "EXPORTED", "Hoàn tất bàn giao kết quả (EXPORTED)", user_name=user_name)

    email_draft = generate_executive_email_draft(job_id=job_id, run_id=run_id, user_name=user_name)

    repo.log_audit(
        workspace_id=job.get("workspace_id", "ws-default"),
        action="EXPORT_JOB",
        target_type="JOB",
        target_id=job_id,
        user_name=user_name,
        details="Xuất báo cáo điều hành và bàn giao dữ liệu sạch thành công."
    )

    return jsonify({
        "success": True,
        "status": "EXPORTED",
        "email_draft": email_draft,
        "download_links": {
            "xlsx": f"/api/data-jobs/{job_id}/download/xlsx",
            "pdf": f"/api/data-jobs/{job_id}/download/pdf",
            "csv": f"/api/data-jobs/{job_id}/download/csv",
            "json": f"/api/data-jobs/{job_id}/download/json"
        }
    })


@app.route("/api/data-jobs/<job_id>/download/<export_type>", methods=["GET"])
def api_download_export(job_id, export_type):
    """Downloads export files: Excel multi-sheet, Branded PDF, Clean CSV, Machine JSON, or Error Report."""
    job = repo.get_job(job_id)
    if not job:
        return jsonify({"success": False, "error": "Tác vụ không tồn tại"}), 404

    workspace_id = job.get("workspace_id", "ws-default")
    ws = repo.get_workspace(workspace_id)
    current_ver = job.get("current_run_version", 1)
    run_id = f"{job_id}-v{current_ver}"
    run = repo.get_run(run_id) or repo.get_latest_run(job_id)

    if export_type == "error_xlsx":
        errors = repo.get_error_reports(job_id)
        buf = export_error_report_to_excel(errors, job.get("title", "Error"))
        return send_file(
            buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"Bao_Cao_Loi_{job_id}.xlsx"
        )

    # Reconstruct clean data from preview
    preview = run.get("clean_data_preview", {}) if run else {}
    rows = preview.get("rows", [])
    df = pd.DataFrame(rows)

    if export_type == "csv":
        buf = export_clean_data_to_csv(df)
        return send_file(
            buf,
            mimetype="text/csv",
            as_attachment=True,
            download_name=f"Du_Lieu_Sach_{job_id}_v{current_ver}.csv"
        )

    elif export_type == "xlsx":
        change_logs = repo.get_change_logs(run_id) if run_id else []
        findings = repo.get_findings(run_id) if run_id else []
        brand_color = ws.get("brand_color", "#4f46e5") if ws else "#4f46e5"

        buf = export_to_multisheet_excel(
            clean_datasets={"CleanData": df},
            change_logs=change_logs,
            findings=findings,
            job_title=job.get("title", "Report"),
            brand_color_hex=brand_color
        )
        return send_file(
            buf,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=f"DataIQ_Export_{job_id}_v{current_ver}.xlsx"
        )

    elif export_type == "pdf":
        buf = export_executive_report_to_pdf(job_id=job_id, run_id=run_id, workspace_id=workspace_id)
        return send_file(
            buf,
            mimetype="application/pdf",
            as_attachment=True,
            download_name=f"Bao_Cao_Chien_Luoc_{job_id}_v{current_ver}.pdf"
        )

    elif export_type == "json":
        full_package = {
            "job": job,
            "run": run,
            "findings": repo.get_findings(run_id) if run_id else [],
            "change_logs": repo.get_change_logs(run_id) if run_id else [],
            "feedbacks": repo.get_feedbacks(job_id),
            "evaluation": repo.get_evaluation(job_id),
            "exported_at": datetime.now().isoformat()
        }
        buf = io.BytesIO(json.dumps(full_package, ensure_ascii=False, indent=2).encode("utf-8"))
        buf.seek(0)
        return send_file(
            buf,
            mimetype="application/json",
            as_attachment=True,
            download_name=f"DataIQ_Package_{job_id}_v{current_ver}.json"
        )

    return jsonify({"success": False, "error": "Định dạng xuất không hợp lệ"}), 400


# ========================================================
# SAMPLE DATASET ENDPOINTS
# ========================================================

@app.route("/api/data-jobs/samples", methods=["GET"])
def api_get_sample_manifest():
    """Lists available built-in test sample datasets."""
    samples = get_sample_manifest()
    return jsonify({"success": True, "samples": samples})


@app.route("/api/data-jobs/samples/<sample_id>/download", methods=["GET"])
def api_download_sample_file(sample_id):
    """Downloads one of the intentional-error test sample files."""
    samples = {s["id"]: s for s in get_sample_manifest()}
    if sample_id not in samples:
        return jsonify({"success": False, "error": "Mã mẫu không tồn tại"}), 404

    s_meta = samples[sample_id]
    fpath = os.path.join(SAMPLE_DIR, s_meta["filename"])
    if not os.path.exists(fpath):
        ensure_sample_files(SAMPLE_DIR)

    return send_file(fpath, as_attachment=True, download_name=s_meta["filename"])


@app.route("/api/data-jobs/quick-load-sample", methods=["POST"])
def api_quick_load_sample():
    """1-Click creation and execution of a DataIQ job from sample files."""
    payload = request.json or {}
    sample_id = payload.get("sample_id", "scenario_retail_sales")
    workspace_id = payload.get("workspace_id", "ws-default")
    user_name = payload.get("user_name", "staff")

    samples = {s["id"]: s for s in get_sample_manifest()}
    if sample_id not in samples:
        return jsonify({"success": False, "error": "Mã mẫu không hợp lệ"}), 400

    s_meta = samples[sample_id]
    sample_src_path = os.path.join(SAMPLE_DIR, s_meta["filename"])
    if not os.path.exists(sample_src_path):
        ensure_sample_files(SAMPLE_DIR)

    job_id = f"job-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
    job_upload_dir = os.path.join(UPLOAD_DIR, job_id)
    os.makedirs(job_upload_dir, exist_ok=True)

    dest_path = os.path.join(job_upload_dir, s_meta["filename"])
    with open(sample_src_path, "rb") as sf, open(dest_path, "wb") as df:
        df.write(sf.read())

    # Create job
    repo.create_job(
        job_id=job_id,
        workspace_id=workspace_id,
        title=f"Mẫu Kiểm Thử: {s_meta['name']}",
        problem_statement=s_meta["recommended_problem"],
        business_context={"industry": "Gia dụng & Bán lẻ", "scale": "Doanh nghiệp vừa", "model": "B2B / Đại lý"},
        goals=s_meta["recommended_goals"],
        column_descriptions={"Thanh_Tien": "Doanh thu bán hàng sau chiết khấu"},
        mask_sensitive=True,
        created_by=user_name
    )

    repo.add_data_file(
        file_id=f"f-{uuid.uuid4().hex[:8]}",
        job_id=job_id,
        workspace_id=workspace_id,
        filename=s_meta["filename"],
        original_filename=s_meta["filename"],
        file_path=dest_path,
        file_type=s_meta["file_type"],
        file_size=os.path.getsize(dest_path)
    )

    start_job_pipeline_async(job_id=job_id, user_name=user_name)

    return jsonify({
        "success": True,
        "job_id": job_id,
        "message": f"Đã nạp nhanh mẫu '{s_meta['name']}' và khởi chạy pipeline thành công!",
        "status": "PROCESSING"
    })


# ========================================================
# WORKSPACE, LESSONS & AUDIT LOG APIS
# ========================================================

@app.route("/api/workspaces", methods=["GET"])
def api_get_workspaces():
    ws_list = repo.list_workspaces()
    return jsonify({"success": True, "workspaces": ws_list})


@app.route("/api/workspaces/config", methods=["POST"])
def api_update_workspace_config():
    payload = request.json or {}
    ws_id = payload.get("workspace_id", "ws-default")
    config = payload.get("config", {})
    ok = repo.update_workspace_config(ws_id, config)
    return jsonify({"success": ok, "message": "Đã cập nhật cấu hình Workspace thành công!"})


@app.route("/api/workspaces/<ws_id>/lessons", methods=["GET"])
def api_get_workspace_lessons(ws_id):
    lessons = repo.list_lessons(ws_id)
    return jsonify({"success": True, "lessons": lessons})


@app.route("/api/workspaces/<ws_id>/lessons", methods=["POST"])
def api_add_custom_lesson(ws_id):
    payload = request.json or {}
    category = payload.get("category", "BUSINESS_RULE")
    rule_key = payload.get("rule_key", "").strip()
    rule_content = payload.get("rule_content", "").strip()
    user_name = payload.get("user_name", "staff")

    if not rule_key or not rule_content:
        return jsonify({"success": False, "error": "Khóa quy tắc và nội dung bài học không được trống."}), 400

    les_id = repo.add_lesson(
        workspace_id=ws_id,
        category=category,
        rule_key=rule_key,
        rule_content=rule_content,
        created_by=user_name
    )
    return jsonify({"success": True, "lesson_id": les_id, "message": "Đã lưu bài học mới vào Workspace."})


@app.route("/api/workspaces/<ws_id>/lessons/<lesson_id>", methods=["DELETE"])
def api_delete_workspace_lesson(ws_id, lesson_id):
    ok = repo.delete_lesson(lesson_id)
    repo.log_audit(
        workspace_id=ws_id,
        action="DELETE_LESSON",
        target_type="LESSON",
        target_id=lesson_id,
        details="Người dùng xóa bài học kinh nghiệm khỏi bộ nhớ Agent."
    )
    return jsonify({"success": ok, "message": "Đã xóa bài học thành công."})


@app.route("/api/audit-logs", methods=["GET"])
def api_get_audit_logs():
    workspace_id = request.args.get("workspace_id", "ws-default")
    target_id = request.args.get("target_id")
    limit = request.args.get("limit", 100, type=int)
    logs = repo.get_audit_logs(workspace_id=workspace_id, target_id=target_id, limit=limit)
    return jsonify({"success": True, "audit_logs": logs, "total": len(logs)})


@app.route("/api/data-iq/dashboard", methods=["GET"])
def api_get_data_iq_dashboard():
    """Returns analytics KPIs for the DataIQ agent."""
    workspace_id = request.args.get("workspace_id", "ws-default")
    metrics = repo.get_dashboard_metrics(workspace_id)
    quality = compute_workspace_quality_stats(workspace_id)
    return jsonify({
        "success": True,
        "dashboard": metrics,
        "quality_stats": quality
    })


@app.route("/api/config", methods=["GET"])
def api_get_config():
    conf = load_config()
    safe_conf = {k: v for k, v in conf.items() if k != "gemini_api_key"}
    safe_conf["has_api_key"] = bool(conf.get("gemini_api_key"))
    return jsonify(safe_conf)


@app.route("/api/config", methods=["POST"])
def api_save_config():
    payload = request.json or {}
    new_data = {}
    if "gemini_api_key" in payload and payload["gemini_api_key"]:
        new_data["gemini_api_key"] = payload["gemini_api_key"].strip()
    if "gemini_model" in payload:
        new_data["gemini_model"] = payload["gemini_model"].strip()
    if "temperature" in payload:
        new_data["temperature"] = float(payload["temperature"])
    ok = save_config(new_data)
    return jsonify({"success": ok, "message": "Đã lưu cấu hình thành công!" if ok else "Lỗi lưu cấu hình."})


@app.route("/api/config/test", methods=["POST"])
def api_test_config():
    payload = request.json or {}
    key = payload.get("gemini_api_key")
    model = payload.get("gemini_model")
    result = test_connection(api_key=key, model=model)
    return jsonify(result)


# ========================================================
# INTERACTIVE API DOCUMENTATION (/api/docs)
# ========================================================

@app.route("/api/docs")
def api_documentation():
    """Renders interactive and styled API documentation."""
    docs_html = """<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <title>PartnerIQ & DataIQ - REST API Documentation</title>
  <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-slate-900 text-slate-100 font-sans p-8">
  <div class="max-w-6xl mx-auto">
    <div class="flex items-center justify-between pb-6 border-b border-slate-700">
      <div>
        <h1 class="text-3xl font-extrabold text-indigo-400">DataIQ & PartnerIQ API Reference</h1>
        <p class="text-slate-400 mt-1">Tài liệu đặc tả các giao diện lập trình ứng dụng (REST Endpoints) cho Agent Xử lý Dữ liệu Doanh nghiệp.</p>
      </div>
      <a href="/" class="px-4 py-2 bg-indigo-600 hover:bg-indigo-500 rounded text-sm font-semibold transition">← Quay lại Ứng dụng</a>
    </div>

    <div class="mt-8 space-y-6">
      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-emerald-600 text-xs font-bold rounded">POST</span>
          <span class="font-mono text-emerald-400 font-semibold">/api/data-jobs</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">B1: Khởi tạo phiên xử lý dữ liệu mới. Nhận bối cảnh doanh nghiệp, vấn đề gặp phải, danh sách mục tiêu và các tệp tải lên (multipart/form-data).</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-sky-600 text-xs font-bold rounded">GET</span>
          <span class="font-mono text-sky-400 font-semibold">/api/data-jobs/{job_id}</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">Truy vấn chi tiết phiên làm việc, trạng thái, phiên bản chạy hiện tại, báo cáo Markdown, bảng dữ liệu sạch, Change Log, và danh mục phát hiện.</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-sky-600 text-xs font-bold rounded">GET</span>
          <span class="font-mono text-sky-400 font-semibold">/api/data-jobs/{job_id}/progress</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">Theo dõi tiến trình thời gian thực (Polling hoặc Server-Sent Events SSE). Trả về % tiến trình và giai đoạn hiện tại.</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-emerald-600 text-xs font-bold rounded">POST</span>
          <span class="font-mono text-emerald-400 font-semibold">/api/data-jobs/{job_id}/feedback</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">B5: Tiếp nhận phản hồi từ con người ở 3 cấp độ (Cả báo cáo, Từng phát hiện, hoặc Từng ô/dòng dữ liệu cụ thể).</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-amber-600 text-xs font-bold rounded">POST</span>
          <span class="font-mono text-amber-400 font-semibold">/api/data-jobs/{job_id}/reprocess</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">B5: Kích hoạt tái xử lý có phạm vi (Scoped Reprocessing), nâng cấp phiên bản (v1 -> v2...), đối chiếu mâu thuẫn dữ liệu gốc và trích xuất Bài học (Lessons Learned).</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-rose-600 text-xs font-bold rounded">POST</span>
          <span class="font-mono text-rose-400 font-semibold">/api/data-jobs/{job_id}/override</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">B5: Cưỡng chế áp dụng phản hồi (Force Override) khi có mâu thuẫn với dữ liệu gốc, lưu vết Audit Trail đầy đủ.</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-emerald-600 text-xs font-bold rounded">POST</span>
          <span class="font-mono text-emerald-400 font-semibold">/api/data-jobs/{job_id}/evaluate</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">B6: Người dùng tự đánh giá kết quả: chấm sao 1-5, chấp nhận/chỉnh sửa/từ chối từng phát hiện, chuyển trạng thái sang REVIEWED.</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-purple-600 text-xs font-bold rounded">POST</span>
          <span class="font-mono text-purple-400 font-semibold">/api/data-jobs/{job_id}/export</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">B7: Nghiệm thu và xuất dữ liệu. Gemini tự động soạn Email nháp gửi Lãnh đạo; tạo liên kết tải XLSX đa sheet, PDF thương hiệu, CSV sạch.</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-sky-600 text-xs font-bold rounded">GET</span>
          <span class="font-mono text-sky-400 font-semibold">/api/data-jobs/{job_id}/download/{export_type}</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">Tải tệp xuất khẩu (export_type: xlsx, pdf, csv, json, hoặc error_xlsx).</p>
      </div>

      <div class="bg-slate-800 rounded-lg p-5 border border-slate-700">
        <div class="flex items-center gap-3">
          <span class="px-2.5 py-1 bg-sky-600 text-xs font-bold rounded">GET</span>
          <span class="font-mono text-sky-400 font-semibold">/api/data-iq/dashboard</span>
        </div>
        <p class="text-slate-300 text-sm mt-2">Thống kê đo lường KPI vận hành của Agent: Tỷ lệ lỗi, Điểm đánh giá trung bình, Số vòng phản hồi trung bình, Phân bổ loại sai sót.</p>
      </div>
    </div>
  </div>
</body>
</html>"""
    return docs_html


if __name__ == "__main__":
    conf = load_config()
    port = int(conf.get("server_port", 5000))
    print(f"\n=======================================================")
    print(f"  PARTNERIQ & DATAIQ - NỀN TẢNG AI AGENT ĐA NHIỆM DOANH NGHIỆP")
    print(f"  Khởi động máy chủ Web Server tại: http://localhost:{port}")
    print(f"  Tài liệu REST API: http://localhost:{port}/api/docs")
    print(f"  Mô hình AI: {conf.get('gemini_model', 'gemini-2.5-flash')}")
    print(f"  Sẵn sàng phục vụ Agent Đối Tác & Agent Xử Lý Dữ Liệu!")
    print(f"=======================================================\n")
    app.run(host="0.0.0.0", port=port, debug=False)
