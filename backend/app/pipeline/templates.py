from app.filters.registry import registry
from app.pipeline.models import StepStatus

DEFAULT_TEMPLATE = "phrase_stats_pipeline"

TEMPLATES = {
    DEFAULT_TEMPLATE: [
        {"step_id":"fetch_raw","step_type":"fetch_raw","depends_on":[],"max_attempts":3,"output_artifact_type":"raw_package"},
        {"step_id":"unzip","step_type":"unzip_package","depends_on":["fetch_raw"],"max_attempts":2,"output_artifact_type":"raw"},
        {"step_id":"analyze","step_type":"analyze_phrase_stats","depends_on":["unzip"],"max_attempts":2,"output_artifact_type":"result"},
        {"step_id":"summarize","step_type":"summarize_result","depends_on":["analyze"],"max_attempts":2,"output_artifact_type":"summary"},
    ]
}

def build_steps(template_name: str = DEFAULT_TEMPLATE):
    from app.pipeline.models import step_doc
    steps=[]
    for spec in TEMPLATES[template_name]:
        item=registry.get(spec["step_type"])
        status=StepStatus.ready if not spec["depends_on"] else StepStatus.pending
        steps.append(step_doc(spec["step_id"], spec["step_type"], item.filter_name, item.filter_version, spec["max_attempts"], status, spec["depends_on"], spec["output_artifact_type"]))
    return steps

def template_metadata(template_name: str = DEFAULT_TEMPLATE):
    specs=TEMPLATES[template_name]
    return {"step_order":[s["step_id"] for s in specs], "final_step_id":specs[-1]["step_id"]}
