"""Enterprise wave planner for multi-project migration sizing and sequencing."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from html import escape
from math import ceil
from pathlib import Path
from typing import Any

from src.analyzers.project_analyzer import AssessmentResult
from src.analyzers.strategy_advisor import StrategyRecommendation
from src.core.registry import AssetRegistry


@dataclass
class ProjectWaveEstimate:
    """Deterministic effort estimate for a single project."""

    project_key: str
    asset_count: int
    overall_score: float
    grade: str
    strategy: str
    confidence: float
    estimated_effort_points: int
    estimated_team_size: int
    recommended_batch_size: int
    recommended_parallelism: int
    complexity: str
    unsupported_features: list[str] = field(default_factory=list)
    risks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "project_key": self.project_key,
            "asset_count": self.asset_count,
            "overall_score": round(self.overall_score, 1),
            "grade": self.grade,
            "strategy": self.strategy,
            "confidence": round(self.confidence, 1),
            "estimated_effort_points": self.estimated_effort_points,
            "estimated_team_size": self.estimated_team_size,
            "recommended_batch_size": self.recommended_batch_size,
            "recommended_parallelism": self.recommended_parallelism,
            "complexity": self.complexity,
            "unsupported_features": self.unsupported_features,
            "risks": self.risks,
        }


@dataclass
class WaveAssignment:
    """A wave groups projects that fit within a target effort budget."""

    wave_number: int
    projects: list[ProjectWaveEstimate]
    total_effort_points: int
    recommended_team_size: int
    rationale: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "wave_number": self.wave_number,
            "projects": [project.to_dict() for project in self.projects],
            "total_effort_points": self.total_effort_points,
            "recommended_team_size": self.recommended_team_size,
            "rationale": self.rationale,
        }


@dataclass
class EnterpriseWavePlan:
    """Final wave plan spanning multiple projects."""

    generated_at: str
    target_wave_capacity: int
    total_effort_points: int
    total_projects: int
    staffing_assumptions: dict[str, Any]
    project_estimates: list[ProjectWaveEstimate] = field(default_factory=list)
    waves: list[WaveAssignment] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "total_projects": self.total_projects,
            "total_effort_points": self.total_effort_points,
            "wave_count": len(self.waves),
            "target_wave_capacity": self.target_wave_capacity,
            "staffing_assumptions": self.staffing_assumptions,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary(),
            "project_estimates": [project.to_dict() for project in self.project_estimates],
            "waves": [wave.to_dict() for wave in self.waves],
        }


def estimate_project_wave(
    registry: AssetRegistry,
    assessment: AssessmentResult,
    recommendation: StrategyRecommendation,
) -> ProjectWaveEstimate:
    """Convert assessment output into a deterministic effort estimate."""
    asset_count = len(registry.get_all())
    unsupported_features = [risk.description for risk in assessment.risks if risk.level.value in {"critical", "high"}]
    risks = [f"{risk.level.value}:{risk.description}" for risk in assessment.risks]

    complexity_lookup = {
        "low": 0.8,
        "medium": 1.0,
        "high": 1.25,
        "very_high": 1.5,
    }
    multiplier = complexity_lookup.get(recommendation.estimated_complexity, 1.0)
    base_effort = max(1, ceil(asset_count / 4))
    risk_load = len(unsupported_features) * 2 + len(risks)
    score_load = max(0, ceil((100 - assessment.overall_score) / 15))
    effort_points = max(1, ceil((base_effort + risk_load + score_load) * multiplier))
    team_size = max(1, ceil(effort_points / 18))

    return ProjectWaveEstimate(
        project_key=registry.project_key,
        asset_count=asset_count,
        overall_score=assessment.overall_score,
        grade=assessment.grade.value,
        strategy=recommendation.strategy.value,
        confidence=recommendation.confidence,
        estimated_effort_points=effort_points,
        estimated_team_size=team_size,
        recommended_batch_size=recommendation.recommended_batch_size,
        recommended_parallelism=recommendation.recommended_parallelism,
        complexity=recommendation.estimated_complexity,
        unsupported_features=unsupported_features,
        risks=risks,
    )


def build_enterprise_wave_plan(
    project_entries: list[tuple[AssetRegistry, AssessmentResult, StrategyRecommendation]],
    *,
    target_wave_capacity: int = 40,
) -> EnterpriseWavePlan:
    """Build a deterministic multi-project migration wave plan."""
    project_estimates = [estimate_project_wave(registry, assessment, recommendation) for registry, assessment, recommendation in project_entries]
    project_estimates.sort(key=lambda item: (-item.estimated_effort_points, item.project_key.lower()))

    waves: list[WaveAssignment] = []
    current_wave: list[ProjectWaveEstimate] = []
    current_effort = 0

    def flush_wave() -> None:
        nonlocal current_wave, current_effort
        if not current_wave:
            return
        wave_number = len(waves) + 1
        total_effort = sum(project.estimated_effort_points for project in current_wave)
        recommended_team_size = max(1, ceil(total_effort / 18))
        rationale = (
            f"Wave {wave_number} bundles {len(current_wave)} project(s) for a total of {total_effort} effort points, "
            f"which keeps the batch near the {target_wave_capacity}-point capacity."
        )
        waves.append(WaveAssignment(
            wave_number=wave_number,
            projects=list(current_wave),
            total_effort_points=total_effort,
            recommended_team_size=recommended_team_size,
            rationale=rationale,
        ))
        current_wave = []
        current_effort = 0

    for project in project_estimates:
        if current_wave and current_effort + project.estimated_effort_points > target_wave_capacity:
            flush_wave()
        current_wave.append(project)
        current_effort += project.estimated_effort_points

    flush_wave()

    total_effort = sum(project.estimated_effort_points for project in project_estimates)
    total_projects = len(project_estimates)
    staffing_assumptions = {
        "delivery_team_size": max(1, ceil(total_effort / 18)),
        "review_team_size": max(1, ceil(total_projects / 3)),
        "batch_capacity_points": target_wave_capacity,
        "parallel_waves": min(2, max(1, ceil(total_projects / 3))),
    }

    return EnterpriseWavePlan(
        generated_at=datetime.now(timezone.utc).isoformat(),
        target_wave_capacity=target_wave_capacity,
        total_effort_points=total_effort,
        total_projects=total_projects,
        staffing_assumptions=staffing_assumptions,
        project_estimates=project_estimates,
        waves=waves,
    )


def generate_wave_plan_html(plan: EnterpriseWavePlan) -> str:
    """Render the enterprise wave plan as standalone HTML."""
    h = escape
    project_rows = "".join(
        f"""<tr>
            <td>{h(project.project_key)}</td>
            <td>{project.asset_count}</td>
            <td>{project.grade}</td>
            <td>{h(project.strategy)}</td>
            <td>{project.estimated_effort_points}</td>
            <td>{project.estimated_team_size}</td>
            <td>{h(project.complexity)}</td>
            <td>{h(', '.join(project.unsupported_features[:3]) if project.unsupported_features else 'None')}</td>
        </tr>"""
        for project in plan.project_estimates
    )

    wave_cards = "".join(
        f"""<section class=\"wave\">
            <h3>Wave {wave.wave_number}</h3>
            <p>{h(wave.rationale)}</p>
            <p><strong>Effort:</strong> {wave.total_effort_points} points | <strong>Recommended team:</strong> {wave.recommended_team_size}</p>
            <ul>
                {''.join(f'<li>{h(project.project_key)} ({project.estimated_effort_points} pts)</li>' for project in wave.projects)}
            </ul>
        </section>"""
        for wave in plan.waves
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Enterprise Wave Plan</title>
<style>
body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #f6f8fb; color: #172033; margin: 0; padding: 24px; }}
.container {{ max-width: 1280px; margin: 0 auto; }}
.hero {{ background: linear-gradient(135deg, #0f172a 0%, #1d4ed8 100%); color: white; border-radius: 20px; padding: 28px; box-shadow: 0 16px 48px rgba(15, 23, 42, 0.22); }}
.stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin: 20px 0; }}
.stat {{ background: white; border-radius: 16px; padding: 18px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08); }}
.stat .value {{ font-size: 2rem; font-weight: 700; }}
.section {{ background: white; border-radius: 20px; padding: 24px; margin: 24px 0; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.08); }}
table {{ width: 100%; border-collapse: collapse; }}
th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e5e7eb; vertical-align: top; }}
th {{ color: #475569; font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.04em; }}
.wave {{ border: 1px solid #e5e7eb; border-radius: 16px; padding: 16px; margin: 16px 0; background: #fafcff; }}
</style>
</head>
<body>
<div class="container">
  <div class="hero">
    <h1>Enterprise Migration Wave Plan</h1>
    <p>Deterministic multi-project plan generated at {h(plan.generated_at)}</p>
  </div>
  <div class="stats">
    <div class="stat"><div class="value">{plan.total_projects}</div><div>Projects</div></div>
    <div class="stat"><div class="value">{plan.total_effort_points}</div><div>Total effort points</div></div>
    <div class="stat"><div class="value">{len(plan.waves)}</div><div>Waves</div></div>
    <div class="stat"><div class="value">{plan.target_wave_capacity}</div><div>Wave capacity</div></div>
  </div>
  <div class="section">
    <h2>Staffing Assumptions</h2>
    <pre>{h(json_dumps(plan.staffing_assumptions))}</pre>
  </div>
  <div class="section">
    <h2>Project Estimates</h2>
    <table>
      <tr><th>Project</th><th>Assets</th><th>Grade</th><th>Strategy</th><th>Effort</th><th>Team</th><th>Complexity</th><th>Unsupported features</th></tr>
      {project_rows}
    </table>
  </div>
  <div class="section">
    <h2>Wave Proposals</h2>
    {wave_cards}
  </div>
</div>
</body>
</html>"""
    return html


def json_dumps(data: Any) -> str:
    import json

    return json.dumps(data, indent=2, sort_keys=True)


def save_wave_plan_report(plan: EnterpriseWavePlan, path: str | Path) -> Path:
    """Save the enterprise wave plan as HTML."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(generate_wave_plan_html(plan), encoding="utf-8")
    return target