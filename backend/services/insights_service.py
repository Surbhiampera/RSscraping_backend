from collections import defaultdict
from sqlalchemy.orm import Session

from backend.db.models import FinalFlatOutput


ADDON_KEYS = [
    "Nil Dep Premium", "EP Premium", "RTI Premium", "RSA Premium",
    "Consumables", "Key & Lock Replacement", "Tyre Protector",
    "Loss of Personal Belongings", "Emergency Transport and Hotel Allowance",
    "Daily Allowance", "NCB Protector",
]

_FALSY_ADDON_VALUES = {"", "not included", "null", "none", "0", "0.0"}


# ── helpers ──────────────────────────────────────────────

def _get_all_plans(db: Session) -> list[dict]:
    rows = db.query(FinalFlatOutput).all()
    plans = []
    for r in rows:
        items = r.flat_output if isinstance(r.flat_output, list) else [r.flat_output]
        plans.extend(items)
    return plans


def _num(val) -> float:
    if val is None:
        return 0.0
    try:
        return float(str(val).replace(",", ""))
    except (ValueError, TypeError):
        return 0.0


def _vehicle_age(plan: dict) -> int | None:
    age = plan.get("Vehicle Age")
    if age is not None:
        try:
            return int(age)
        except (ValueError, TypeError):
            pass
    yom = plan.get("YOM")
    if yom:
        try:
            return 2026 - int(yom)
        except (ValueError, TypeError):
            pass
    return None


def _insurer_name(plan: dict) -> str | None:
    return plan.get("Company") or plan.get("Insurer")


def _od(plan: dict) -> float:
    return _num(plan.get("OD Premium") or plan.get("Basic OD Premium"))


def _tp(plan: dict) -> float:
    return _num(plan.get("TP Premium") or plan.get("Basic TP Premium"))


def _net(plan: dict) -> float:
    return _num(plan.get("Net Premium"))


def _final(plan: dict) -> float:
    return _num(plan.get("Final Premium"))


def _age_band(age: int | None) -> str:
    if age is not None and 1 <= age <= 3:
        return "1-3 years"
    if age is not None and 5 <= age <= 9:
        return "5-9 years"
    return "Other"


def _has_addon(plan: dict, key: str) -> bool:
    val = plan.get(key)
    if not val:
        return False
    return str(val).strip().lower() not in _FALSY_ADDON_VALUES


# ── function 1 ───────────────────────────────────────────

def get_insurer_pattern_analysis(db: Session) -> dict:
    plans = _get_all_plans(db)

    grouped: dict[str, list[dict]] = defaultdict(list)
    for p in plans:
        name = _insurer_name(p)
        if name:
            grouped[name].append(p)

    insurers = []
    for name, plist in grouped.items():
        cnt = len(plist)
        avg_od = round(sum(_od(p) for p in plist) / cnt, 2)
        avg_tp = round(sum(_tp(p) for p in plist) / cnt, 2)
        avg_net = round(sum(_net(p) for p in plist) / cnt, 2)
        avg_final = round(sum(_final(p) for p in plist) / cnt, 2)
        insurers.append({
            "name": name,
            "avg_od": avg_od,
            "avg_tp": avg_tp,
            "avg_net": avg_net,
            "avg_final": avg_final,
            "plan_count": cnt,
        })

    insurers.sort(key=lambda x: x["avg_final"])

    # build clusters of consecutive insurers within ±10%
    clusters: list[dict] = []
    if insurers:
        current_cluster = [insurers[0]]
        for ins in insurers[1:]:
            ref = current_cluster[0]["avg_final"]
            if ref == 0 or abs(ins["avg_final"] - ref) / ref <= 0.10:
                current_cluster.append(ins)
            else:
                clusters.append(current_cluster)
                current_cluster = [ins]
        clusters.append(current_cluster)

    cluster_results = []
    for cl in clusters:
        finals = [i["avg_final"] for i in cl]
        min_f, max_f = min(finals), max(finals)
        if max_f > 0:
            similarity = round((1 - (max_f - min_f) / max_f) * 100, 2)
        else:
            similarity = 100.0
        cluster_results.append({
            "band_range": f"{min_f}-{max_f}",
            "insurers": [i["name"] for i in cl],
            "similarity_score": similarity,
        })

    summary = (
        f"Analyzed {len(insurers)} insurers and found {len(cluster_results)} "
        f"premium clusters."
    )

    return {
        "insurers": insurers,
        "clusters": cluster_results,
        "summary": summary,
    }


# ── function 2 ───────────────────────────────────────────

def get_premium_by_vehicle_age(db: Session) -> dict:
    plans = _get_all_plans(db)

    grouped: dict[str, list[dict]] = defaultdict(list)
    for p in plans:
        band = _age_band(_vehicle_age(p))
        grouped[band].append(p)

    age_groups = []
    for band, plist in grouped.items():
        cnt = len(plist)
        avg_od = round(sum(_od(p) for p in plist) / cnt, 2)
        avg_tp = round(sum(_tp(p) for p in plist) / cnt, 2)
        avg_net = round(sum(_net(p) for p in plist) / cnt, 2)
        avg_final = round(sum(_final(p) for p in plist) / cnt, 2)

        active = sorted({_insurer_name(p) for p in plist if _insurer_name(p)})

        insurer_finals: dict[str, list[float]] = defaultdict(list)
        for p in plist:
            n = _insurer_name(p)
            if n:
                insurer_finals[n].append(_final(p))
        insurer_avg = [
            {"name": n, "avg_final": round(sum(v) / len(v), 2)}
            for n, v in insurer_finals.items()
        ]
        insurer_avg.sort(key=lambda x: x["avg_final"])
        top_cheapest = insurer_avg[:3]

        age_groups.append({
            "age_band": band,
            "avg_od": avg_od,
            "avg_tp": avg_tp,
            "avg_net": avg_net,
            "avg_final": avg_final,
            "plan_count": cnt,
            "active_insurers": active,
            "top_cheapest": top_cheapest,
        })

    # comparisons between 1-3 and 5-9
    comparisons: list[str] = []
    band_map = {ag["age_band"]: ag for ag in age_groups}
    young = band_map.get("1-3 years")
    mid = band_map.get("5-9 years")
    if young and mid:
        for metric in ("avg_od", "avg_tp", "avg_net", "avg_final"):
            diff = round(mid[metric] - young[metric], 2)
            direction = "higher" if diff > 0 else "lower"
            comparisons.append(
                f"{metric.replace('avg_', '').replace('_', ' ').title()} for 5-9 years "
                f"is {abs(diff)} {direction} than 1-3 years."
            )

    summary = (
        f"Analyzed {len(plans)} plans across {len(age_groups)} age bands."
    )

    return {
        "age_groups": age_groups,
        "comparisons": comparisons,
        "summary": summary,
    }


# ── function 3 ───────────────────────────────────────────

def get_addon_coverage_patterns(db: Session) -> dict:
    plans = _get_all_plans(db)

    # group by age band
    band_groups: dict[str, list[dict]] = defaultdict(list)
    for p in plans:
        band = _age_band(_vehicle_age(p))
        band_groups[band].append(p)

    age_groups = []
    for band, plist in band_groups.items():
        cnt = len(plist)

        # per-addon stats
        addon_stats = []
        for key in ADDON_KEYS:
            c = sum(1 for p in plist if _has_addon(p, key))
            addon_stats.append({
                "addon_name": key,
                "count": c,
                "percentage": round(c / cnt * 100, 2) if cnt else 0.0,
            })

        # per-insurer addon sets in this band
        insurer_addons: dict[str, set[str]] = defaultdict(set)
        for p in plist:
            name = _insurer_name(p)
            if not name:
                continue
            for key in ADDON_KEYS:
                if _has_addon(p, key):
                    insurer_addons[name].add(key)

        total_insurers_in_band = len(insurer_addons)

        # common addons: offered by >50% of insurers in this band
        common_addons = []
        for key in ADDON_KEYS:
            offering = sum(1 for addons in insurer_addons.values() if key in addons)
            if total_insurers_in_band and offering / total_insurers_in_band > 0.5:
                common_addons.append(key)

        # unique combinations: addons offered by <=1 insurer
        unique_addon_keys: set[str] = set()
        for key in ADDON_KEYS:
            offering = sum(1 for addons in insurer_addons.values() if key in addons)
            if offering <= 1:
                unique_addon_keys.add(key)

        unique_combinations = []
        for name, addons in insurer_addons.items():
            unique = sorted(addons & unique_addon_keys)
            if unique:
                unique_combinations.append({"insurer": name, "addons": unique})

        age_groups.append({
            "age_band": band,
            "plan_count": cnt,
            "addon_stats": addon_stats,
            "common_addons": common_addons,
            "unique_combinations": unique_combinations,
        })

    # insurer strategies across all plans
    global_insurer_addons: dict[str, set[str]] = defaultdict(set)
    for p in plans:
        name = _insurer_name(p)
        if not name:
            continue
        for key in ADDON_KEYS:
            if _has_addon(p, key):
                global_insurer_addons[name].add(key)

    insurer_strategies = [
        {
            "insurer": name,
            "total_addons_offered": len(addons),
            "addon_names": sorted(addons),
        }
        for name, addons in global_insurer_addons.items()
    ]
    insurer_strategies.sort(key=lambda x: x["total_addons_offered"], reverse=True)

    summary = (
        f"Analyzed {len(plans)} plans across {len(age_groups)} age bands "
        f"with {len(ADDON_KEYS)} tracked add-ons."
    )

    return {
        "age_groups": age_groups,
        "insurer_strategies": insurer_strategies,
        "summary": summary,
    }
